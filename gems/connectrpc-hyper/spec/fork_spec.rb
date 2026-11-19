# frozen_string_literal: true

RSpec.describe "Fork", :fork do
  before { skip "fork is unavailable" unless Process.respond_to?(:fork) }

  %w[grpc_h2c grpc_tls].each do |name|
    it "keeps a parent's in-flight #{name} stream and connection while the child calls on its own" do
      from = peer.mark
      tls = name.end_with?("tls") ? tls_options : {}
      tag = name.split("_").first
      output, status = ruby_child(<<~RUBY)
        transport = ConnectRPC::Hyper::Transport.new(**#{tls.inspect})
        peer_client = client(#{peer[name].dump}, transport)
        call = ->(value) { peer_client.unary(MSG.new(value: value), timeout: 5).message.value == value }
        call.call("parent")
        held = Queue.new
        holder = Thread.new do
          peer_client.server_stream(MSG.new(value: "hold"), timeout: 30) do |reader|
            held << reader.receive.value
            reader.each { nil }
          end
        rescue ConnectRPC::Error => e
          e.code
        end
        held.pop
        reader, writer = IO.pipe
        pid = fork do
          reader.close
          writer.puts([call.call("child-1"), call.call("child-2")].inspect)
          writer.close
          GC.start
          exit(0)
        end
        writer.close
        child = reader.read.chomp
        _, child_status = Process.wait2(pid)
        parent = Array.new(3) { |index| call.call("after-\#{index}") }
        puts [child, child_status.success?, parent].inspect
        $stdout.flush
        holder.kill.join
      RUBY
      expect(status).to be_success
      child, child_success, parent = eval(output.lines.last) # rubocop:disable Security/Eval
      expect([child, child_success, parent]).to eq(["[true, true]", true, [true, true, true]])
      events = peer.since(from)
      # The parent dialed once and reused its connection after the fork; the
      # child dialed its own, which closed when the child exited.
      expect(events.grep(/\A#{tag} accept/).size).to eq(2)
      held_done = events.index { |event| event.match?(/#{tag} ss-hold-done/) }
      expect(held_done).not_to be_nil
      # The parent's held stream outlived the child: it ended only when the
      # parent killed its holder thread.
      child_closed = events.index { |event| event.match?(/\A#{tag} conn-closed/) }
      expect(child_closed).to be < held_done
    end
  end

  it "builds fresh roots in a child forked while the parent's trust store is still loading" do
    directory = Dir.mktmpdir("connectrpc-hyper-fork-roots-")
    fifo = File.join(directory, "roots.pem")
    File.mkfifo(fifo)
    ca = File.join(peer.certs, "ca.pem")
    # Ruby's openssl reads SSL_CERT_FILE when it loads, so the child points it
    # at the FIFO only afterwards.
    output, status = ruby_child(<<~RUBY, env: { "SSL_CERT_DIR" => nil })
      ENV["SSL_CERT_FILE"] = #{fifo.dump}
      transport = ConnectRPC::Hyper::Transport.new
      loading = Thread.new { client(#{peer['grpc_tls'].dump}, transport).unary(MSG.new(value: "parent"), timeout: 20).message.value }
      # A writer can open the FIFO only once the trust-store load opened it.
      writer = nil
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
      until writer
        begin
          writer = File.open(#{fifo.dump}, File::WRONLY | File::NONBLOCK)
        rescue Errno::ENXIO
          raise "trust store never loaded" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          Thread.pass
        end
      end
      pid = fork do
        ENV["SSL_CERT_FILE"] = #{ca.dump}
        value = client(#{peer['grpc_tls'].dump}, transport).unary(MSG.new(value: "child"), timeout: 10).message.value
        exit!(value == "child" ? 0 : 1)
      end
      _, child = Process.wait2(pid)
      writer.write(File.read(#{ca.dump}))
      writer.close
      p [child.success?, loading.value]
    RUBY
    expect(status).to be_success
    expect(output.lines.last.chomp).to eq('[true, "parent"]')
  ensure
    FileUtils.rm_rf(directory) if directory
  end

  it "closes the parent's connection for the peer while a long-lived child stays alive" do
    from = peer.mark
    code = <<~RUBY
      $LOAD_PATH.unshift(#{File.join(ROOT, 'interop/grpc/ruby').dump})
      require "connectrpc/hyper"
      require "connectrpc/bindings/google_protobuf"
      require "peer_connect"
      $stdout.sync = true
      transport = ConnectRPC::Hyper::Transport.new
      client = Connectrpc::Grpcinterop::V1::Peer::Client.new(
        client: ConnectRPC::Client.new(base_url: #{peer['grpc_h2c'].dump}, transport: transport, protocol: :grpc,
                                       binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      )
      client.unary(Connectrpc::Grpcinterop::V1::Message.new(value: "parent"), timeout: 5)
      pid = fork { $stdin.gets; exit!(0) }
      puts "closed \#{transport.close(grace: 5)}"
      Process.wait(pid)
      puts "child exited"
    RUBY
    Open3.popen2(RbConfig.ruby, "-e", code) do |stdin, stdout, wait|
      expect(stdout.gets&.chomp).to eq("closed true")
      # The child holds no copy of the socket, so the close reaches the peer now.
      peer.wait_event(/\Agrpc conn-closed /, from: from)
      expect(wait).to be_alive
      stdin.puts("go")
      stdin.close
      expect(stdout.gets&.chomp).to eq("child exited")
      expect(wait.value).to be_success
    end
  end

  # A token's Notify lock may be held by a parent runtime thread at fork
  # time, so firing an inherited token must not touch it.
  it "ignores a cancellation token from before fork that the child fires" do
    output, status = ruby_child(<<~RUBY)
      cancel = ConnectRPC::Hyper.const_get(:Native)::Cancel.new
      reader, writer = IO.pipe
      pid = fork do
        cancel.fire
        writer.puts cancel.fired?.inspect
        exit!(0)
      end
      writer.close
      Process.wait(pid)
      child = reader.read.chomp
      parent = cancel.fired?
      cancel.fire
      p [child, parent, cancel.fired?]
    RUBY
    expect(status).to be_success
    expect(output.lines.last.chomp).to eq('["false", false, true]')
  end

  it "keeps a transport closed before fork closed in the child" do
    output, status = ruby_child(<<~RUBY)
      transport = ConnectRPC::Hyper::Transport.new
      transport.close
      reader, writer = IO.pipe
      pid = fork do
        begin
          client(#{peer['grpc_h2c'].dump}, transport).unary(MSG.new, timeout: 5)
          writer.puts "called"
        rescue ConnectRPC::Error => e
          writer.puts e.message
        end
        exit!(0)
      end
      writer.close
      Process.wait(pid)
      puts reader.read
    RUBY
    expect(status).to be_success
    expect(output.lines.last.chomp).to eq("HTTP transport admission is closed")
  end
end
