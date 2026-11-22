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
      # The peer logs the held stream's end only once it sees the reset,
      # which can come after the child Ruby exited.
      peer.wait_event(/#{tag} ss-hold-done/, from: from)
      events = peer.since(from)
      # The parent dialed once and reused its connection after the fork; the
      # child dialed its own, which closed when the child exited.
      accepted = events.grep(/\A#{tag} accept/)
      expect(accepted.size).to eq(2)
      held_done = events.index { |event| event.match?(/#{tag} ss-hold-done/) }
      expect(held_done).not_to be_nil
      # The parent's held stream outlived the child: it ended only when the
      # parent killed its holder thread.
      child_address = Regexp.escape(accepted.last.split.last)
      child_closed = events.index { |event| event.match?(/\A#{tag} conn-closed #{child_address}\z/) }
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
      peer.wait_closed("grpc", from: from)
      expect(wait).to be_alive
      stdin.puts("go")
      stdin.close
      expect(stdout.gets&.chomp).to eq("child exited")
      expect(wait.value).to be_success
    end
  end

  # A token's Notify lock may be held by a parent runtime thread at fork
  # time, so firing an inherited token must not touch it. A native call in
  # the child that carries the token shows whether the fire landed: a fired
  # token fails the call as canceled before it dials.
  it "ignores a cancellation token from before fork that the child fires" do
    url = "#{peer['connect_h2c']}/connectrpc.grpcinterop.v1.Peer/Unary"
    output, status = ruby_child(<<~RUBY)
      native = ConnectRPC::Hyper.const_get(:Native)
      cancel = native::Cancel.new
      call = lambda do
        driver = native::Driver.new(true, true, nil, 5.0, nil, [], nil, nil, [60.0, 1024])
        status, = driver.unary("POST", #{url.dump}, [["content-type", "application/proto"]], "".b, 1024, nil, 5.0,
                               cancel, nil)
        driver.close(0.0)
        status
      rescue native::Failure => e
        e.kind
      end
      reader, writer = IO.pipe
      pid = fork do
        cancel.fire
        writer.puts call.call.inspect
        exit!(0)
      end
      writer.close
      Process.wait(pid)
      child = reader.read.chomp
      before = call.call
      cancel.fire
      p [child, before, call.call]
    RUBY
    expect(status).to be_success
    expect(output.lines.last.chomp).to eq('["200", 200, :canceled]')
  end

  # The child closes the transport, then calls on it, and reports
  # [close outcome, closed promptly, the call's message]. A child that does
  # not report within 10 seconds is killed and reports :hung.
  def child_close = <<~RUBY
    def child_close(transport)
      reader, writer = IO.pipe
      pid = fork do
        reader.close
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        outcome = transport.close(grace: 5)
        prompt = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started < 2
        message = begin
          client(#{peer['grpc_h2c'].dump}, transport).unary(MSG.new, timeout: 5)
          "called"
        rescue ConnectRPC::Error => e
          e.message
        end
        writer.puts [outcome, prompt, message].inspect
        exit!(0)
      end
      writer.close
      unless reader.wait_readable(10)
        Process.kill(:KILL, pid)
        Process.wait(pid)
        return :hung
      end
      result = eval(reader.read)
      Process.wait(pid)
      result
    end
  RUBY
  def child_closed = [true, true, "HTTP transport admission is closed"]

  # A held stream keeps the parent's close pending across the fork. The
  # child has neither the parent's closer thread nor its teardown, so it must
  # not wait for either.
  it "closes at once in a child that inherits the parent's pending close" do
    from = peer.mark
    output, status = ruby_child(<<~RUBY)
      #{child_close}
      transport = ConnectRPC::Hyper::Transport.new
      held = Queue.new
      holder = Thread.new do
        client(#{peer['grpc_h2c'].dump}, transport).server_stream(MSG.new(value: "hold"), timeout: 30) do |reader|
          held << reader.receive.value
          reader.each { nil }
        end
      rescue ConnectRPC::Error => e
        e.message
      end
      held.pop
      closer = Thread.new { transport.close(grace: 30) }
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
      until transport.inspect.include?("state=closing") && closer.status == "sleep" &&
            closer.backtrace.to_a.any? { |line| line.include?("Driver#close") }
        raise "close never parked" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        Thread.pass
      end
      child = child_close(transport)
      holder.kill.join
      p [child, [true, false].include?(closer.value)]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq([child_closed, true]) # rubocop:disable Security/Eval
    # The held stream ends with the child Ruby; wait for the peer to log it
    # so its unkeyed ss-hold-done never lands in a later example.
    peer.wait_event(/grpc ss-hold-done/, from: from)
  end

  # The child never built a driver of its own, so its close has nothing to
  # drain, and the parent's driver and connection stay in use.
  it "closes at once in a child that never built its own driver, leaving the parent's transport open" do
    output, status = ruby_child(<<~RUBY)
      #{child_close}
      transport = ConnectRPC::Hyper::Transport.new
      peer_client = client(#{peer['grpc_h2c'].dump}, transport)
      call = ->(value) { peer_client.unary(MSG.new(value: value), timeout: 5).message.value }
      call.call("parent")
      child = child_close(transport)
      p [child, call.call("after"), transport.instance_variable_get(:@driver).opened]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq([child_closed, "after", 1]) # rubocop:disable Security/Eval
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
