# frozen_string_literal: true

RSpec.describe "gRPC original processor completion" do
  [false, true].each do |late_failure|
    it "holds staged terminal HEADERS behind original-task cleanup, late failure #{late_failure}" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        processors = []
        failure = RuntimeError.new("late private cleanup")
        target = server(protocol: :grpc, echo: lambda { |message, context|
          expect(context).to be_instance_of(ConnectRPC::Context)
          processors << Async::Task.current
          message
        })
        application_type = Class.new(ConnectRPC::Async::Application) do
          define_method(:call_with_publication) do |*args, **options|
            super(*args, **options)
          ensure
            processors << Async::Task.current
            entered.enqueue(true)
            release.dequeue
            processors << Async::Task.current
            raise failure if late_failure
          end
        end
        application = application_type.new(server: target)
        host_type = Class.new(ConnectRPC::Async::Host) do
          define_method(:build_session) do |**options|
            options[:application] = application
            ConnectRPC::Async::ServerSession.new(**options)
          end
        end
        host = host_type.new(server: target).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        core = ConnectRPC::Client.new(base_url: origin, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                      transport: transport, protocol: :grpc)
        call = task.async do
          core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new(name: "queued"), timeout: nil)
        rescue ConnectRPC::Error => e
          e
        end
        entered.dequeue
        expect(call).not_to be_finished
        response = transport.instance_variable_get(:@calls).keys.first.fetch(:response)
        expect(response.stream.grpc_trailers).to be_nil
        expect(processors.map(&:object_id).uniq.size).to eq(1)
        release.enqueue(true)
        result = call.wait
        if late_failure
          expect(result).to be_a(ConnectRPC::Error)
          expect(result.code).to eq("internal")
          expect(result.cause).to be_nil
          expect(result.message).not_to include(failure.message)
        else
          expect(result.message.name).to eq("queued")
          expect(response.stream.grpc_trailers["grpc-status"]).to eq(["0"])
        end
        expect(processors.map(&:object_id).uniq.size).to eq(1)
        expect(transport.instance_variable_get(:@calls)).to be_empty
      ensure
        release&.enqueue(true)
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end
end

RSpec.describe "gRPC peer cancellation source" do
  { 0 => nil, 1 => nil, 8 => :canceled }.each do |reset, reason|
    it "signals Context only for actual CANCEL, receiving reset #{reset}" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        target = server(protocol: :grpc, echo: lambda { |message, context|
          entered.enqueue(context)
          release.dequeue
          message
        })
        host = ConnectRPC::Async::Host.new(server: target).start
        raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
        connection = Async::HTTP::Protocol::HTTP2.client(raw)
        response = connection.create_response
        bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "accepted"))
        response.send_request(Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path,
                                                      { "content-type" => "application/grpc" },
                                                      [[0, bytes.bytesize].pack("CN") + bytes],
                                                      scheme: "http",
                                                      authority: "127.0.0.1:#{host.port}"])
        context = entered.dequeue
        expect(context.cancellation_reason).to be_nil
        resource = host_session(host).instance_variable_get(:@resources).first
        stream = resource.connection.streams.values.first
        response.stream.send_reset_stream(reset)
        wait_until(task, "actual peer reset retired native server stream") { stream.closed? }
        expect(context.cancellation_reason).to eq(reason)
      ensure
        release&.enqueue(true)
        response&.close
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
        host&.close(grace: 0)
      end
    end
  end
end
