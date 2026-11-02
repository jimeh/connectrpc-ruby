# frozen_string_literal: true

require "protocol/http/body/writable"

RSpec.describe "HTTP/2 stream and pool cleanup" do
  %i[h2c tls].each do |mode|
    it "skips response body output after a peer reset during #{mode} response headers" do
      reactor do |task|
        with_ca do |ca_file|
          entered = Async::Queue.new
          release_handler = Async::Queue.new
          host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
            if message.name == "sibling"
              entered.enqueue(true)
              release_handler.dequeue
            end
            message
          }), tls: mode == :tls ? host_tls : nil).start
          origin = "#{mode == :tls ? 'https' : 'http'}://localhost:#{host.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: mode == :tls ? ca_file : nil)
          typed = client(transport, origin)
          sibling = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "sibling"), timeout: nil) }
          entered.dequeue
          resource = host.instance_variable_get(:@resources).first
          gate = FrameGate.new(resource.socket, frame_type: 1)
          resource.connection.stream.instance_variable_set(:@io, gate)
          gate.armed = true
          call = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "cancel"), timeout: nil) }
          gate.started.dequeue
          writer = resource.connection.writers.keys.first
          stream = resource.connection.streams.values.max_by(&:id)
          call.cancel
          call.wait
          wait_until(task, "peer reset during header flush") { stream.closed? }
          gate.release.enqueue(true)
          wait_until(task, "response header writer completed") { writer.finished? }
          release_handler.enqueue(true)
          expect(sibling.wait.message.name).to eq("sibling")
          expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
          expect(host.instance_variable_get(:@resources)).to eq([resource])
          expect(resource.raw).not_to be_closed
          # reactor also asserts that no background task emitted an unhandled failure.
        ensure
          gate&.release&.enqueue(true)
          release_handler&.enqueue(true)
          transport&.close(grace: 0)
          host&.close(grace: 0)
        end
      end
    end
  end

  it "cleans every wire stream state and releases its pool slot exactly once, including peer reset races" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      requests = Async::Queue.new
      bodies = []
      peer = task.async do
        raw, = listener.accept
        connection = Async::HTTP::Protocol::HTTP2.server(raw)
        while (incoming = connection.requests.dequeue)
          body = if %w[/open /half-local /race].include?(incoming.path)
                   Protocol::HTTP::Body::Writable.new.tap { |value| bodies << value }
                 else
                   ["ok"]
                 end
          incoming.send_response(Protocol::HTTP::Response[200, {}, body])
          requests.enqueue(incoming)
        end
      ensure
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
      requests.dequeue
      pool = transport.instance_variable_get(:@pool)
      connection = transport.instance_variable_get(:@resources).first.connection
      %i[idle open half_closed_local half_closed_remote closed].each do |state|
        acquired = pool.acquire
        response = acquired.create_response
        unless state == :idle
          target = { open: "/open", half_closed_local: "/half-local", half_closed_remote: "/half-remote",
                     closed: "/closed" }.fetch(state)
          outgoing_body = Protocol::HTTP::Body::Writable.new if %i[open half_closed_remote].include?(state)
          outgoing = Protocol::HTTP::Request["GET", target, {}, outgoing_body, scheme: "http",
                                                                               authority: URI(origin).authority]
          acquired.synchronize { task.defer_cancel { acquired.write_request(response, outgoing) } }
          requests.dequeue
          acquired.read_response(response)
          response.read if %i[half_closed_remote closed].include?(state)
        end
        expect(response.stream.state).to eq(state)
        expect(response.stream.pool).to be_nil
        transport.send(:cleanup, { connection: acquired, resource: acquired.resource, response: response })
        expect(response.stream.state).to eq(:closed)
        expect(pool.resources.values.sum).to eq(0)
        nil
      end
      acquired = pool.acquire
      response = acquired.create_response
      response.send_request(Protocol::HTTP::Request["GET", "/race", {}, Protocol::HTTP::Body::Writable.new,
                                                    scheme: "http", authority: URI(origin).authority])
      incoming = requests.dequeue
      acquired.read_response(response)
      locked = Async::Queue.new
      release = Async::Queue.new
      holder = task.async do
        connection.synchronize do
          locked.enqueue(true)
          release.dequeue
        end
      end
      locked.dequeue
      cleaning = task.async do
        transport.send(:cleanup, { connection: acquired, resource: acquired.resource, response: response })
      end
      incoming.stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL)
      wait_until(task, "peer reset observed") { response.stream.closed? }
      expect(pool.resources.values.sum).to eq(1)
      release.enqueue(true)
      holder.wait
      cleaning.wait
      expect(pool.resources.values.sum).to eq(0)
      expect(transport.call(request("#{origin}/after")).body).to eq("ok")
      expect(transport.instance_variable_get(:@resources).size).to eq(1)
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      bodies&.each(&:close)
      peer&.cancel
      peer&.wait
      bound&.close
    end
  end

  %i[refused goaway].each do |failure|
    it "never replays a #{failure} stream even when the backend labels it safe to retry" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        listener = bound.sockets.first
        observed = []
        peer = task.async do
          raw, = listener.accept
          connection = Async::HTTP::Protocol::HTTP2.server(raw)
          incoming = connection.requests.dequeue
          observed << incoming.path
          if failure == :refused
            incoming.stream.send_reset_stream(Protocol::HTTP2::Error::REFUSED_STREAM)
          else
            frame = Protocol::HTTP2::GoawayFrame.new
            frame.pack(0, 0, "")
            connection.write_frame(frame)
          end
          Async::Condition.new.wait
        ensure
          connection&.close
          AsyncHelpers::INTERNAL.raw_close(raw)
        end
        origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect { transport.call(request("#{origin}/once")) }.to raise_error(ConnectRPC::Error) { |e|
          expect(e.code).to eq("unavailable")
        }
        expect(observed).to eq(["/once"])
        expect(listener.accept_nonblock(exception: false)).to eq(:wait_readable)
        expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
      ensure
        transport&.close(grace: 0)
        peer&.cancel
        peer&.wait
        bound&.close
      end
    end
  end
end

RSpec.describe "HTTP/2 pre-dispatch closure" do
  it "does not reconnect a connection closed after pool acquisition; only a separate future call reconnects" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      transport_class = Class.new(ConnectRPC::Async::Transport) do
        attr_reader :attempts

        private

        def connect(operation)
          @attempts = (@attempts || 0) + 1
          super
        end
      end
      transport = transport_class.new(origin: origin)
      typed = client(transport, origin)
      typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
      record = transport.instance_variable_get(:@resources).first
      connection = record.connection
      # A source-coupled race fixture closes after acquisition, at the dispatch
      # guard, while leaving the real pool, dispatch and socket logic intact.
      connection.define_singleton_method(:synchronize) do |&block|
        close! unless @closed_at_dispatch
        @closed_at_dispatch = true
        super(&block)
      end
      expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "once")) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("unavailable")
      }
      expect(transport.attempts).to eq(1)
      expect(record.raw).to be_closed
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "future")).message.name).to eq("future")
      expect(transport.attempts).to eq(2)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
