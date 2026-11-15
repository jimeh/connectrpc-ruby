# frozen_string_literal: true

require "protocol/http/body/writable"
require_relative "support/reset_examples"

RSpec.describe "HTTP/2 stream and pool cleanup" do
  include_examples "HTTP/2 peer reset during headers"

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
      manager = connection_manager(transport)
      connection = transport.instance_variable_get(:@resources).first.connection
      %i[idle open half_closed_local half_closed_remote closed].each do |state|
        acquired = manager.acquire
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
        expect(manager.usage).to eq(0)
        nil
      end
      acquired = manager.acquire
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
      expect(manager.usage).to eq(1)
      release.enqueue(true)
      holder.wait
      cleaning.wait
      expect(manager.usage).to eq(0)
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
        expect(lease_usage(transport)).to eq(0)
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
  def counting_host(observed)
    ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
      observed << message.name
      message
    })).start
  end

  it "reroutes a call whose connection closed at the dispatch guard to one new connection" do
    reactor do
      observed = []
      attempts = []
      host = counting_host(observed)
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(
        origin: origin, connection_observer: ->(event) { attempts << event.outcome if event.type == :attempt_finished }
      )
      typed = client(transport, origin)
      typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
      record = transport.instance_variable_get(:@resources).first
      connection = record.connection
      # A source-coupled race fixture closes after acquisition, at the dispatch
      # guard, while leaving the real manager, dispatch and socket logic intact.
      connection.define_singleton_method(:synchronize) do |**options, &block|
        close! unless @closed_at_dispatch
        @closed_at_dispatch = true
        super(**options, &block)
      end
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "once")).message.name).to eq("once")
      expect(observed).to eq(%w[warm once])
      expect(attempts).to eq(%i[ready ready])
      expect(record.raw).to be_closed
      expect(connection_manager(transport).connections.keys).not_to include(connection)
      expect(lease_usage(transport)).to eq(0)
      expect(transport.instance_variable_get(:@calls)).to be_empty
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "publishes idle when the peer closes an idle connection and dials once for the next call" do
    reactor do |task|
      observed = []
      states = []
      host = counting_host(observed)
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(
        origin: origin, connection_observer: ->(event) { states << event.state if event.type == :state_changed }
      )
      typed = client(transport, origin)
      typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
      record = transport.instance_variable_get(:@resources).first
      expect(transport.connection_state).to eq(:ready)
      host_session(host).instance_variable_get(:@resources).first.abort
      # No call is running; the death hook alone republishes the state.
      wait_until(task, "idle after peer close") { transport.connection_state == :idle }
      expect(connection_manager(transport).connections).to be_empty
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "next")).message.name).to eq("next")
      expect(observed).to eq(%w[warm next])
      expect(states).to eq(%i[connecting ready idle connecting ready])
      expect(transport.instance_variable_get(:@resources).last).not_to equal(record)
      expect(record.raw).to be_closed
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
