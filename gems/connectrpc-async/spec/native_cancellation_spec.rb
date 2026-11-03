# frozen_string_literal: true

require "protocol/http/body/writable"

RSpec.describe "Native HTTP/2 cancellation boundaries" do
  [Protocol::HTTP2::Error::CANCEL, Protocol::HTTP2::Error::NO_ERROR,
   Protocol::HTTP2::Error::REFUSED_STREAM].each do |code|
    it "signals Context only for peer CANCEL, with reset code #{code}" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        context = nil
        target = server(echo: lambda { |message, current|
          if message.name == "hold"
            context = current
            entered.enqueue(true)
            release.dequeue
          end
          message
        })
        host = ConnectRPC::Async::Host.new(server: target).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        typed = client(transport, origin)
        call = task.async(finished: false) { typed.echo(AsyncHelpers::MESSAGE.new(name: "hold"), timeout: nil) }
        entered.dequeue
        operation = transport.instance_variable_get(:@calls).keys.first
        operation.fetch(:response).stream.send_reset_stream(code)
        resource = host_session(host).instance_variable_get(:@resources).first
        wait_until(task, "server reset stream closure") { resource.connection.streams.empty? }
        expect(context.cancellation_reason).to eq(code == Protocol::HTTP2::Error::CANCEL ? :canceled : nil)
        expect { call.wait }.to raise_error(ConnectRPC::Error)
        release.enqueue(true)
        expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      ensure
        release&.enqueue(true)
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end

  it "publishes peer CANCEL before teardown resumes an exceptional blocked request read" do
    reactor do |task|
      reading = Async::Queue.new
      outcomes = []
      observer = Object.new
      observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
      subscriber = Object.new
      subscriber.define_singleton_method(:start) { |_| observer }
      bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
      target = server(instrumentation: bus)
      context = nil
      allow(target).to receive(:request_context).and_wrap_original do |original, *args|
        context = original.call(*args)
      end
      allow(ConnectRPC::Async.const_get(:Internal)::Input).to receive(:new).and_wrap_original do |original, *args|
        input = original.call(*args)
        allow(input).to receive(:read).and_wrap_original do |read, length|
          reading.enqueue(true)
          read.call(length)
        end
        input
      end
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
      connection = AsyncHelpers::INTERNAL::ClientConnection.new(IO::Stream(raw))
      connection.send_connection_preface(AsyncHelpers::INTERNAL.settings(128))
      connection.read_in_background(parent: task)
      body = Protocol::HTTP::Body::Writable.new
      response = connection.create_response
      outgoing = Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path,
                                         { "content-type" => "application/proto" }, body,
                                         scheme: "http", authority: URI(origin).authority]
      connection.write_request(response, outgoing)
      body.write("\x0a")
      reading.dequeue
      expect(outcomes).to be_empty
      response.stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL)
      wait_until(task, "cancelled processing finished") { outcomes.size == 1 }
      expect(context.cancellation_reason).to eq(:canceled)
      expect(outcomes.first.code).to eq("canceled")
      expect(outcomes.first.error).not_to be_nil
      expect(outcomes.first.normalized_error.code).to eq("canceled")
      expect(outcomes.first.error).not_to equal(outcomes.first.normalized_error)
    ensure
      body&.close
      AsyncHelpers::INTERNAL.raw_close(raw)
      connection&.close
      host&.close(grace: 0)
    end
  end
end
