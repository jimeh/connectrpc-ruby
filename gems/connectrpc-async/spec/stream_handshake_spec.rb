# frozen_string_literal: true

RSpec.describe "Native incremental exchange handshakes" do
  it "publishes headers while the original processing task owns handler cleanup" do
    reactor do |task|
      release = Async::Queue.new
      entered = Async::Queue.new
      processor = nil
      cleaned = false
      application = Object.new
      application.define_singleton_method(:call) do |_request, **|
        Protocol::HTTP::Response[200, { "x-publication" => "missing" }, nil]
      end
      application.define_singleton_method(:call_with_publication) do |_request, publication:, **|
        processor = Async::Task.current
        body = AsyncHelpers::INTERNAL::OutputBody.new
        publication.publish(Protocol::HTTP::Response[200, { "x-publication" => "early" }, body])
        entered.enqueue(processor)
        release.dequeue
        raise "processor moved" unless Async::Task.current.equal?(processor)

        cleaned = true
        body.close_write
      end
      target = server
      host_type = Class.new(ConnectRPC::Async::Host) do
        define_method(:build_session) do |**options|
          options[:application] = application
          ConnectRPC::Async::ServerSession.new(**options)
        end
      end
      host = host_type.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
      connection = AsyncHelpers::INTERNAL::ClientConnection.new(IO::Stream(raw))
      connection.send_connection_preface(AsyncHelpers::INTERNAL.settings(128))
      connection.read_in_background(parent: task)
      response = connection.create_response
      connection.write_request(response, Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path,
                                                                 {}, nil, scheme: "http",
                                                                          authority: URI(origin).authority])
      connection.read_response(response)
      expect(response.headers["x-publication"]).to eq(["early"])
      expect(entered.dequeue).to equal(processor)
      expect(cleaned).to be(false)
      release.enqueue(true)
      expect(response.read).to be_nil
      expect(cleaned).to be(true)
    ensure
      release&.enqueue(true)
      response&.close
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw)
      host&.close(grace: 0)
    end
  end

  it "yields a writable exchange before a peer can produce response headers" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      received = Async::Queue.new
      peer = task.async(finished: false) do
        raw, = listener.accept
        connection = Async::HTTP::Protocol::HTTP2.server(raw)
        incoming = connection.requests.dequeue
        bytes = incoming.body.read
        received.enqueue(bytes)
        incoming.send_response(Protocol::HTTP::Response[200, { "x-after-input" => "yes" }, ["reply"]])
        Async::Condition.new.wait
      ensure
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport).to respond_to(:open_stream)
      transport.open_stream(request("#{origin}/test.API/Bidi")) do |exchange|
        exchange.write("first")
        expect(received.dequeue).to eq("first")
        exchange.close_send
        expect(exchange.response_head.headers["x-after-input"]).to eq(["yes"])
        expect(exchange.read(5)).to eq("reply")
        expect(exchange.read(1)).to be_nil
        exchange.finish
      end
    ensure
      transport&.close(grace: 0)
      peer&.cancel
      peer&.wait
      bound&.close
    end
  end
end
