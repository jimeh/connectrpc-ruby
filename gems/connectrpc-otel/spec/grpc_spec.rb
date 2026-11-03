# frozen_string_literal: true

RSpec.describe "gRPC SDK conventions and processor ownership" do
  %i[async falcon].each do |backend|
    it "retains #{backend} mTLS identity and SDK context through all four shapes and cleanup" do
      provider, exporter, bus = tracing(request_metadata: ["x-request"], response_metadata: ["x-response"])
      reactor do
        records = []
        host = nil
        observe = lambda do |phase|
          records << [phase, Async::Task.current, current_span.context.span_id, host.peer_certificate&.serial&.to_i]
        end
        original = ConnectRPC::Bindings::GoogleProtobuf.new
        binding = Object.new
        %i[encode decode].each do |operation|
          binding.define_singleton_method(operation) do |value, **options|
            observe.call(operation)
            original.public_send(operation, value, **options)
          ensure
            observe.call(:codec_cleanup)
          end
        end
        kinds = [ConnectRPC::Method, ConnectRPC::ServerStreamMethod,
                 ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod]
        methods = kinds.each_with_index.map do |kind, index|
          kind.new(service: "trace.GRPC", name: "Call#{index}", request_type: AsyncHelpers::MESSAGE,
                   response_type: AsyncHelpers::MESSAGE)
        end
        handler = lambda do |context|
          observe.call(:handler)
          expect(context.request_metadata["x-request"]).to eq(["value"])
          context.response_metadata["x-response"] = ["value"]
          Async::Task.current.async do
            expect(host.peer_certificate).to be_nil
            expect(current_span.context).not_to be_valid
          end.wait
        end
        handlers = {
          methods[0].path => lambda { |message, context|
            expect(context).to be_instance_of(ConnectRPC::Context)
            handler.call(context)
            message
          },
          methods[1].path => lambda { |message, writer, context|
            handler.call(context)
            writer.send_message(message)
          },
          methods[2].path => lambda { |reader, context|
            handler.call(context)
            message = reader.receive
            expect(reader.receive).to be_nil
            message
          },
          methods[3].path => lambda { |duplex, context|
            handler.call(context)
            duplex.each { |message| duplex.send_message(message) }
          }
        }
        target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: "trace.GRPC", methods: methods)],
                                        binding: binding, status_codec: original, protocol: :grpc,
                                        handlers: handlers, instrumentation: bus)
        tls = host_tls(anchors: [tls_material.fetch(:client_ca)])
        if backend == :async
          host = ConnectRPC::Async::Host.new(server: target, tls: tls).start
          port = host.port
        else
          bound = ConnectRPC::Falcon::Endpoint.parse("https://127.0.0.1:0").bound
          host = ConnectRPC::Falcon::Server.new(server: target, endpoint: bound, tls: tls)
          host.run
          port = bound.sockets.first.local_address.ip_port
        end
        application = host_session(host).instance_variable_get(:@application)
        allow(application).to receive(:call_with_publication).and_wrap_original do |work, *args, **options|
          work.call(*args, **options)
        ensure
          observe.call(:application_cleanup)
        end
        with_ca do |ca_file|
          certificate, key = tls_material.fetch(:identity)
          origin = "https://127.0.0.1:#{port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file,
                                                       certificate_chain: [certificate.to_pem],
                                                       private_key: key.private_to_pem)
          client = ConnectRPC::Client.new(base_url: origin, transport: transport, binding: original, protocol: :grpc,
                                          instrumentation: bus)
          options = { metadata: { "x-request" => ["value"] }, timeout: nil }
          message = AsyncHelpers::MESSAGE.new(name: "typed")
          expect(client.call(methods[0], message, **options).message).to eq(message)
          client.server_stream(methods[1], message, **options) do |reader|
            reader.each do |reply|
              expect(reply).to eq(message)
            end
          end
          expect(client.client_stream(methods[2], **options) do |writer|
            writer.send_message(message)
          end.message).to eq(message)
          client.bidi_stream(methods[3], **options) do |duplex|
            duplex.send_message(message)
            expect(duplex.receive).to eq(message)
            duplex.close_send
            expect(duplex.receive).to be_nil
          end
          spans = rpc_spans(exporter)
          expect(spans.size).to eq(8)
          expect(spans.map { |span| span.attributes["rpc.system.name"] }).to all(eq("grpc"))
          expect(spans.map { |span| span.attributes["rpc.response.status_code"] }).to all(eq("OK"))
          spans.select { |span| span.kind == :server }.each do |span|
            observed = records.select { |record| record[2] == span.span_id }
            expect(observed.map(&:first)).to include(:handler, :encode, :decode, :codec_cleanup, :application_cleanup)
            expect(observed.map { |record| record[1].object_id }.uniq.size).to eq(1)
            expect(observed.map(&:last)).to all(eq(certificate.serial.to_i))
            expect(span.attributes["rpc.request.metadata.x-request"]).to eq(["value"])
            expect(span.attributes["rpc.response.metadata.x-response"]).to eq(["value"])
            expect(span.attributes["connectrpc.delivery"]).to eq("completed")
          end
        ensure
          transport&.close(grace: 0)
        end
      ensure
        host&.close(grace: 0)
        bound&.close
        provider.shutdown
      end
    end
  end
end

RSpec.describe "gRPC error observation" do
  %w[canceled unavailable].each do |code|
    it "emits standard #{code == 'canceled' ? 'CANCELLED' : 'UNAVAILABLE'} codes and preserves server error policy" do
      provider, exporter, bus = tracing
      reactor do
        failure = ConnectRPC::Error.new(code, "safe remote failure")
        target = server(protocol: :grpc, instrumentation: bus, echo: ->(*) { raise failure })
        host = ConnectRPC::Async::Host.new(server: target).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        core = ConnectRPC::Client.new(base_url: origin, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                      transport: transport, protocol: :grpc, instrumentation: bus)
        expect do
          core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new)
        end.to raise_error(ConnectRPC::Error) { |e|
                 expect(e.code).to eq(code)
                 expect(e.cause).to be_nil
               }
        spans = rpc_spans(exporter)
        standard = code == "canceled" ? "CANCELLED" : "UNAVAILABLE"
        expect(spans.map { |span| span.attributes["rpc.response.status_code"] }).to eq([standard, standard])
        client_span = spans.find { |span| span.kind == :client }
        server_span = spans.find { |span| span.kind == :server }
        expect(client_span.attributes["error.type"]).to eq(standard)
        expected = code == "canceled" ? OpenTelemetry::Trace::Status::UNSET : OpenTelemetry::Trace::Status::ERROR
        expect(server_span.status.code).to eq(expected)
        expect(spans.flat_map { |span| span.events || [] }).to be_empty
      ensure
        transport&.close(grace: 0)
        host&.close(grace: 0)
        provider.shutdown
      end
    end
  end
end

RSpec.describe "gRPC local transport error conventions" do
  it "keeps code-based error.type separate from delivery abort" do
    provider, exporter, bus = tracing
    transport = Object.new
    transport.define_singleton_method(:grpc_capabilities) { [:unary] }
    transport.define_singleton_method(:open_grpc_stream) { |_, &| raise ConnectRPC::Error.new(:unavailable, "setup failed") }
    core = ConnectRPC::Client.new(base_url: "https://example.invalid", transport: transport, protocol: :grpc,
                                  binding: ConnectRPC::Bindings::GoogleProtobuf.new, instrumentation: bus)
    expect { core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new) }.to raise_error(ConnectRPC::Error)
    span = rpc_spans(exporter).fetch(0)
    expect(span.attributes["rpc.system.name"]).to eq("grpc")
    expect(span.attributes["rpc.response.status_code"]).to eq("UNAVAILABLE")
    expect(span.attributes["error.type"]).to eq("UNAVAILABLE")
    expect(span.attributes["connectrpc.delivery"]).to eq("aborted")
  ensure
    provider.shutdown
  end
end

RSpec.describe "gRPC SDK owned cleanup observation" do
  [false, true].each do |failure|
    it "finishes the client span after owned cleanup with failure #{failure}" do
      provider, exporter, bus = tracing
      reactor do
        host = ConnectRPC::Async::Host.new(server: server(protocol: :grpc)).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        allow(transport).to receive(:dispatch_stream).and_wrap_original do |dispatch, operation, body|
          dispatch.call(operation, body)
          allow(operation.fetch(:response)).to receive(:close).and_wrap_original do |close|
            expect(rpc_spans(exporter)).to be_empty
            close.call
            raise IOError, "private cleanup secret" if failure
          end
        end
        core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: :grpc,
                                      instrumentation: bus, binding: ConnectRPC::Bindings::GoogleProtobuf.new)
        if failure
          expect { core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new) }.to raise_error(ConnectRPC::Error)
        else
          core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new)
        end
        span = rpc_spans(exporter).fetch(0)
        expect(span.attributes["rpc.response.status_code"]).to eq(failure ? "UNAVAILABLE" : "OK")
        expect(span.attributes["connectrpc.delivery"]).to eq(failure ? "aborted" : "completed")
        expect(span.status.code).to eq(failure ? OpenTelemetry::Trace::Status::ERROR : OpenTelemetry::Trace::Status::UNSET)
        expect(span.events || []).to be_empty
        expect(span.attributes.to_s).not_to include("private cleanup secret")
      ensure
        transport&.close(grace: 0)
        host&.close(grace: 0)
        provider.shutdown
      end
    end
  end
end
