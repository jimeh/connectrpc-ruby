# frozen_string_literal: true

require_relative "support/http1_client_helpers"

RSpec.describe ConnectRPC::Async::HTTP1Transport, "descriptor lifetime" do
  include HTTP1ClientHelpers

  def descriptor_probe(number)
    IO.for_fd(number, autoclose: false)
  end

  def expect_retained_shutdown(number)
    probe = nil
    expect { probe = descriptor_probe(number) }.not_to raise_error
    expect(probe.stat).to be_a(File::Stat)
    # A TLS peer can reset after shutdown discards pending ciphertext. Both
    # outcomes are terminal; neither permits a native waiter to read new data.
    result = begin
      probe.read_nonblock(1, exception: false)
    rescue Errno::ECONNRESET
      :reset
    end
    expect([nil, :reset]).to include(result)
    expect { probe.write_nonblock("must not escape") }.to raise_error(Errno::EPIPE)
  end

  def descriptor_lifecycle(&)
    observer = Object.new
    observer.define_singleton_method(:attach) { nil }
    observer.define_singleton_method(:detach, &)
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
                               .start(owner: self, role: :client, method: nil, carrier: {})
  end

  # The listener predates final disposition. Keep each new client and accepted
  # peer open while allocating a client with the released descriptor number.
  # A real readability waiter re-arms this number on the original reactor.
  def expect_reused_payload(task, listener, number)
    clients = []
    peers = []
    peer_tasks = []
    victim_port = nil
    go = Async::Promise.new
    received = Async::Promise.new
    acceptor = task.async(finished: false) do
      loop do
        socket = listener.accept
        peers << socket
        port = socket.peeraddr[1]
        peer_tasks << task.async(finished: false) do
          go.wait
          socket.write("VICTIM-BYTES\n")
          socket.shutdown(Socket::SHUT_WR)
          bytes = socket.read
          received.resolve(bytes) if port == victim_port
        rescue IOError, SystemCallError
          nil
        end
      end
    end
    victim = reused_client(listener, number, clients)
    expect(victim).not_to be_nil
    victim_port = victim.addr[1]
    armed = Async::Promise.new
    reader = task.async(finished: false) do
      armed.resolve(nil)
      expect(victim.wait_readable(1)).to be_truthy
      victim.read
    end
    armed.wait
    go.resolve(nil)
    payload = reader.wait
    expect(payload).to eq("VICTIM-BYTES\n")
    victim.shutdown(Socket::SHUT_WR)
    unsolicited = received.wait
    expect(unsolicited).to eq("")
    puts "R5-FD ruby=#{RUBY_VERSION} reused_fd=#{number} armed_readability=true " \
         "payload=#{payload.inspect} server_eof=true unsolicited=#{unsolicited.inspect}"
  ensure
    go.resolve(nil)
    clients.each { |socket| AsyncHelpers::INTERNAL.raw_close(socket) }
    peers.each { |socket| AsyncHelpers::INTERNAL.raw_close(socket) }
    [acceptor, *peer_tasks].each { |worker| AsyncHelpers::INTERNAL.cancel(worker) }
    [acceptor, *peer_tasks].reverse_each { |worker| AsyncHelpers::INTERNAL.join(worker) }
    AsyncHelpers::INTERNAL.join(reader)
  end

  def reused_client(listener, number, clients)
    32.times do
      client = TCPSocket.new("127.0.0.1", listener.addr[1])
      clients << client
      return client if client.fileno == number
    end
    nil
  end

  %i[multiple_operations same_operation].each do |ordering|
    it "retains a suspended native TLS descriptor through #{ordering} cancellation unwind and joins" do
      hang = Async::Promise.new
      hello = Async::Promise.new
      before = lambda do |raw, _index|
        # A real ClientHello has left SSL_connect, without a server handshake.
        header = raw.read(5)
        expect(header.getbyte(0)).to eq(22)
        expect(raw.read(header.byteslice(3, 2).unpack1("n"))).not_to be_empty
        hello.resolve(nil)
        hang.wait
      end
      peer = ->(*) {}
      with_raw_http1(peer: peer, tls: true, before_handshake: before,
                     connection_limit: 2) do |transport, origin, _accepted, task|
        entered = Async::Promise.new
        unwinding = Async::Promise.new
        release = Async::Promise.new
        native_wait = Async::Promise.new
        abort_returned = Async::Promise.new
        native_unwind = Async::Promise.new
        cancels = Hash.new(0)
        setup_cancels_at_unwind = nil
        selected = false
        token = ConnectRPC::CancellationSource.new.token
        source = Object.new
        source.define_singleton_method(:reason) do
          if !selected && caller_locations.any? { |frame| frame.base_label == "acquire" }
            selected = true
            entered.resolve(nil)
            begin
              Async::Promise.new.wait
            rescue Async::Cancel
              unwinding.resolve(Async::Task.current)
              release.wait
            end
          end
          nil
        end
        source.define_singleton_method(:cancelled?) { false }
        source.define_singleton_method(:check!) { nil }
        source.define_singleton_method(:on_cancel) { |&callback| token.on_cancel(&callback) }
        first = nil
        if ordering == :multiple_operations
          first = task.async(finished: false) do
            transport.open_stream(stream_request(origin, cancellation: source)) { nil }
          rescue ConnectRPC::Error, Async::Cancel
            nil
          end
          entered.wait
        end
        outgoing = stream_request(origin)
        operation = nil
        if ordering == :same_operation
          outgoing.lifecycle = descriptor_lifecycle do |_token|
            next unless operation && operation.fetch(:tasks).keys.first.equal?(Async::Task.current)

            setup_cancels_at_unwind = cancels[operation.fetch(:resource).setup]
            unwinding.resolve(Async::Task.current)
            release.wait
          end
        end
        allow(Fiber.scheduler).to receive(:io_wait).and_wrap_original do |original, io, events, *args|
          if io.is_a?(BasicSocket) && events.anybits?(IO::READABLE) && caller_locations.any? do |frame|
            frame.base_label == "connect"
          end && !native_wait.resolved?
            native_wait.resolve(Async::Task.current)
          end
          begin
            original.call(io, events, *args)
          ensure
            # Keep the real SSL_connect frame until its readiness/cancellation
            # unwind is released. Ruby 4's close interrupts the native wait
            # before abort returns; that path stays natural.
            if native_wait.resolved? && native_wait.wait.equal?(Async::Task.current) && abort_returned.resolved?
              native_unwind.resolve(nil)
              begin
                release.wait
              rescue Async::Cancel
                release.wait
              end
            end
          end
        end
        second = task.async(finished: false) do
          transport.open_stream(outgoing, &:response_head)
        rescue ConnectRPC::Error, Async::Cancel
          nil
        end
        hello.wait
        setup = native_wait.wait
        operation = transport.instance_variable_get(:@calls).keys.last
        resource = operation.fetch(:resource)
        allow(resource).to receive(:abort).and_wrap_original do |original|
          result = original.call
          abort_returned.resolve(nil)
          result
        end
        expect(setup).to equal(resource.setup)
        expect(setup).not_to be_finished
        expect(setup.backtrace.any? { |frame| frame.include?("connect") }).to be(true)
        number = resource.raw.fileno
        listener = TCPServer.new("127.0.0.1", 0)
        tracked_operations = transport.instance_variable_get(:@calls).keys
        tracked_operations.flat_map { |active| active.fetch(:tasks).keys }.each do |worker|
          allow(worker).to receive(:cancel).and_wrap_original do |original, *args, **options|
            cancels[worker] += 1
            original.call(*args, **options)
          end
        end
        closer = task.async(finished: false) { transport.close(grace: 0) }
        held = unwinding.wait
        expect(resource.raw).to be_closed
        expect_retained_shutdown(number)
        if RUBY_VERSION.start_with?("3.")
          native_unwind.wait
          expect(setup).not_to be_finished
          # Async schedules the cancelling owner when a task yields. By the
          # time this observer runs, it may have delivered the next cancel.
          expect(setup_cancels_at_unwind || cancels[setup]).to eq(0)
        end
        expect(resource).not_to be_finished
        expect(operation[:cleanup_complete]).not_to be(true)
        expect(transport.instance_variable_get(:@leases)).to include(resource)
        expect(transport.instance_variable_get(:@state)).to eq(:closing)
        puts "R5-RETAIN ruby=#{RUBY_VERSION} ordering=#{ordering} fd=#{number} native_ssl_wait=true " \
             "raw_closed=true retained_shutdown=true setup_alive=#{setup.alive? == true}"
        # Allocation while held cannot take this descriptor, even on Ruby 4
        # where raw close may already have interrupted the native waiter.
        other = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
        expect(other.fileno).not_to eq(number)
        AsyncHelpers::INTERNAL.raw_close(other)
        release.resolve(nil)
        expect(closer.wait).to be(false)
        AsyncHelpers::INTERNAL.join(first)
        AsyncHelpers::INTERNAL.join(second)
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        expect(cancels.values).to all(eq(1))
        expect(cancels[held]).to eq(1)
        expect(resource).to be_finished
        expect(operation.fetch(:cleanup_done)).to be_resolved
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect { descriptor_probe(number) }.to raise_error(Errno::EBADF)
        expect_reused_payload(task, listener, number)
      ensure
        release&.resolve(nil)
        hang.resolve(nil)
        [first, second, closer].each { |worker| AsyncHelpers::INTERNAL.cancel(worker) }
        [first, second, closer].reverse_each { |worker| AsyncHelpers::INTERNAL.join(worker) }
        AsyncHelpers::INTERNAL.raw_close(listener)
      end
    end
  end

  it "retains the shut down descriptor until cleanup-child lifecycle detachment joins" do
    with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, _accepted, _requests, task|
      detached = Async::Promise.new
      release = Async::Promise.new
      operation = nil
      number = nil
      outgoing = stream_request(origin)
      outgoing.lifecycle = descriptor_lifecycle do |_token|
        next unless operation && operation[:cleanup_task].equal?(Async::Task.current)

        transport.send(:abort, operation)
        detached.resolve(nil)
        begin
          release.wait
        ensure
          release.wait
        end
      end
      caller = task.async(finished: false) do
        transport.open_stream(outgoing) do |exchange|
          exchange.close_send
          expect(exchange.read(2)).to eq("ok")
          expect(exchange.read(1)).to be_nil
          operation = exchange.instance_variable_get(:@operation)
          number = operation.fetch(:resource).raw.fileno
          exchange.abort
        end
      end
      detached.wait
      resource = operation.fetch(:resource)
      expect_retained_shutdown(number)
      expect(operation.fetch(:cleanup_task)).not_to be_finished
      expect(operation.fetch(:cleanup_done)).not_to be_resolved
      expect(transport.instance_variable_get(:@leases)).to eq([resource])
      release.resolve(nil)
      caller.wait
      expect(operation.fetch(:tasks).keys).to all(be_finished)
      expect { descriptor_probe(number) }.to raise_error(Errno::EBADF)
    ensure
      release&.resolve(nil)
      AsyncHelpers::INTERNAL.join(caller)
    end
  end
  it "invalidates and disposes TCP returned after an earlier nil-raw abort" do
    with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, _accepted, _requests, task|
      connected = Async::Promise.new
      unwinding = Async::Promise.new
      release = Async::Promise.new
      socket = nil
      number = nil
      allow(IO::Endpoint).to receive(:tcp).and_wrap_original do |original, *args|
        endpoint = original.call(*args)
        allow(endpoint).to receive(:connect).and_wrap_original do |connect|
          socket = connect.call
          number = socket.fileno
          connected.resolve(nil)
          begin
            Async::Promise.new.wait
          rescue Async::Cancel
            unwinding.resolve(nil)
            release.wait
          end
          socket
        end
        endpoint
      end
      caller = task.async(finished: false) do
        transport.open_stream(stream_request(origin)) { raise "Retired TCP was handed off" }
      end
      connected.wait
      operation = transport.instance_variable_get(:@calls).keys.first
      resource = operation.fetch(:resource)
      expect(resource.raw).to be_nil
      caller.cancel
      unwinding.wait
      expect(resource.raw).to be_nil
      expect(socket).not_to be_closed
      expect(resource).not_to be_finished
      expect(operation.fetch(:cleanup_done)).not_to be_resolved
      release.resolve(nil)
      AsyncHelpers::INTERNAL.join(caller)
      expect(resource.raw).to equal(socket)
      expect(socket).to be_closed
      expect(operation[:writer]).to be_nil
      expect(operation.fetch(:tasks).keys).to all(be_finished)
      expect(resource).to be_finished
      expect { descriptor_probe(number) }.to raise_error(Errno::EBADF)
      expect(transport.instance_variable_get(:@leases)).to be_empty
    ensure
      release&.resolve(nil)
      AsyncHelpers::INTERNAL.join(caller)
    end
  end

  %i[active idle].each do |state|
    %i[connection socket private_owner].each do |boundary|
      it "finishes #{state} disposition despite a final #{boundary} close error" do
        with_http1_peer(response: http1_wire, tls: true,
                        connection_limit: 1) do |transport, origin, _accepted, _requests, task|
          prepared = Async::Promise.new
          command = Async::Promise.new
          closing = Async::Promise.new
          release = Async::Promise.new
          operation = nil
          observed = nil
          error = IOError.new("final #{boundary} close failed")
          caller = task.async(finished: false) do
            transport.open_stream(stream_request(origin)) do |exchange|
              exchange.close_send
              expect(exchange.read(2)).to eq("ok")
              expect(exchange.read(1)).to be_nil
              operation = exchange.instance_variable_get(:@operation)
              prepared.resolve(nil)
              command.wait
              state == :active ? exchange.abort : exchange.finish
            end
          rescue IOError => e
            observed = e
          end
          prepared.wait
          resource = operation.fetch(:resource)
          number = resource.raw.fileno
          owner = nil
          exited = Async::Promise.new
          allow(resource).to receive(:close).and_wrap_original do |original|
            original.call
          ensure
            exited.resolve(nil)
          end
          calls = 0
          releases = 0
          install = lambda do |target|
            allow(target).to receive(:close).and_wrap_original do |original|
              calls += 1
              if calls == 1
                closing.resolve(Async::Task.current)
                release.wait
                raise error
              end
              releases += 1
              original.call
            end
          end
          if boundary == :private_owner
            allow(IO).to receive(:for_fd).and_wrap_original do |original, *args, **options|
              handle = original.call(*args, **options)
              if args.first == number && options.empty?
                owner = handle
                install.call(handle)
              end
              handle
            end
          else
            install.call(resource.public_send(boundary))
          end
          command.resolve(nil)
          if state == :idle
            caller.wait
            expect(transport.instance_variable_get(:@idle)).to eq([resource])
            closer = task.async(finished: false) do
              transport.close
            rescue IOError => e
              observed = e
            end
          end
          wait_until(task, "final close attempted or disposition returned") { closing.resolved? || exited.resolved? }
          expect(closing).to be_resolved
          held = closing.wait
          expect(held).to equal(state == :idle ? transport.instance_variable_get(:@closer) : caller)
          expect(operation.fetch(:tasks).keys).to all(be_finished)
          expect(operation.fetch(:cleanup_task)).to be_finished
          expect_retained_shutdown(number)
          expect(resource).not_to be_finished
          expect(transport.instance_variable_get(:@leases)).to eq([resource])
          release.resolve(nil)
          AsyncHelpers::INTERNAL.join(caller)
          AsyncHelpers::INTERNAL.join(closer)
          expect(observed).to equal(error)
          expect { descriptor_probe(number) }.to raise_error(Errno::EBADF)
          expect(owner).to be_closed if owner
          expect(releases).to eq(1) if boundary == :private_owner
          expect(resource).to be_finished
          expect(transport.instance_variable_get(:@leases)).to be_empty
        ensure
          command&.resolve(nil)
          release&.resolve(nil)
          AsyncHelpers::INTERNAL.join(caller)
          AsyncHelpers::INTERNAL.join(closer)
        end
      end
    end
  end

  %i[connection socket private_owner].each do |boundary|
    it "retains the original interruption across repeated final #{boundary} close interruption" do
      with_http1_peer(response: http1_wire, tls: true,
                      connection_limit: 1) do |transport, origin, _accepted, _requests, task|
        prepared = Async::Promise.new
        command = Async::Promise.new
        closing = Async::Queue.new
        release = Async::Promise.new
        operation = nil
        observed = nil
        original_interrupt = nil
        cause = IOError.new("first final-close interruption")
        caller = task.async(finished: false) do
          transport.open_stream(stream_request(origin)) do |exchange|
            exchange.close_send
            exchange.read(2)
            exchange.read(1)
            operation = exchange.instance_variable_get(:@operation)
            prepared.resolve(nil)
            command.wait
            exchange.abort
          end
        rescue Async::Cancel => e
          observed = e
          raise
        end
        prepared.wait
        resource = operation.fetch(:resource)
        number = resource.raw.fileno
        exited = Async::Promise.new
        allow(resource).to receive(:close).and_wrap_original do |original|
          original.call
        ensure
          exited.resolve(nil)
        end
        calls = 0
        install = lambda do |target|
          allow(target).to receive(:close).and_wrap_original do |original|
            calls += 1
            closing.enqueue(Async::Task.current)
            begin
              release.wait
            rescue Async::Cancel => e
              original_interrupt ||= e
              raise
            end
            original.call
          end
        end
        if boundary == :private_owner
          allow(IO).to receive(:for_fd).and_wrap_original do |original, *args, **options|
            handle = original.call(*args, **options)
            install.call(handle) if args.first == number && options.empty?
            handle
          end
        else
          install.call(resource.public_send(boundary))
        end
        command.resolve(nil)
        wait_until(task, "final close attempted or disposition returned") { !closing.empty? || exited.resolved? }
        expect(closing).not_to be_empty
        expect(closing.dequeue).to equal(caller)
        expect(operation.fetch(:cleanup_task)).to be_finished
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        caller.cancel(cause: cause)
        expect(closing.dequeue).to equal(caller)
        caller.cancel
        expect(closing.dequeue).to equal(caller)
        expect_retained_shutdown(number)
        expect(resource).not_to be_finished
        expect(operation.fetch(:cleanup_done)).not_to be_resolved
        expect(transport.instance_variable_get(:@leases)).to eq([resource])
        release.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        expect(observed).to equal(original_interrupt)
        expect(observed.cause).to equal(cause)
        expect(calls).to eq(3)
        expect { descriptor_probe(number) }.to raise_error(Errno::EBADF)
        expect(operation.fetch(:cleanup_done)).to be_resolved
        expect(transport.instance_variable_get(:@leases)).to be_empty
      ensure
        release&.resolve(nil)
        command&.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
      end
    end
  end
end
