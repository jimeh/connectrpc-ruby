# frozen_string_literal: true

RSpec.describe "Web authenticated processor and SDK ownership" do
  [%i[async h2], %i[falcon h2], %i[falcon http1]].each do |backend, protocol|
    it "retains #{backend}/#{protocol} mTLS identity through handler, codecs, status and original ensure" do
      provider, exporter, bus = tracing
      reactor do |task|
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
            observe.call(:codec_ensure)
          end
        end
        codec = Object.new
        codec.define_singleton_method(:encode_status) do |error|
          observe.call(:status)
          original.encode_status(error)
        ensure
          observe.call(:status_ensure)
        end
        codec.define_singleton_method(:decode_status) { |bytes, **options| original.decode_status(bytes, **options) }
        handler = lambda do |message, _context|
          observe.call(:handler)
          if message.name == "error"
            raise ConnectRPC::Error.new(:not_found, "safe", details: [
                                          ConnectRPC::ErrorDetail.new(type: "opaque.Type", value: "opaque")
                                        ])
          end
          message
        ensure
          observe.call(:handler_ensure)
        end
        target = ConnectRPC::Server.new(services: [AsyncHelpers::ECHO::SERVICE], binding: binding,
                                        protocol: :grpc_web, status_codec: codec, instrumentation: bus,
                                        handlers: AsyncHelpers::ECHO.handlers(echo: handler, fail: handler))
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
          observe.call(:application_ensure)
        end
        context = OpenSSL::SSL::SSLContext.new
        context.verify_mode = OpenSSL::SSL::VERIFY_PEER
        context.cert_store = OpenSSL::X509::Store.new.tap { |store| store.add_cert(tls_material.fetch(:root).first) }
        context.cert, context.key = tls_material.fetch(:identity)
        context.alpn_protocols = [protocol == :h2 ? "h2" : "http/1.1"]
        raw = IO::Endpoint.tcp("127.0.0.1", port).connect
        socket = OpenSSL::SSL::SSLSocket.new(raw, context)
        socket.hostname = "127.0.0.1"
        socket.connect
        socket.post_connection_check("127.0.0.1")
        expect(socket.alpn_protocol).to eq(protocol == :h2 ? "h2" : "http/1.1")
        implementation = protocol == :h2 ? Async::HTTP::Protocol::HTTP2 : Async::HTTP::Protocol::HTTP1
        connection = implementation.client(socket)
        %w[success error].each do |name|
          message = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: name))
          wire = [0, message.bytesize].pack("CN") + message
          response = connection.call(Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path,
                                                             { "content-type" => "application/grpc-web" }, [wire],
                                                             authority: "127.0.0.1:#{port}", scheme: "https"])
          expect(response.status).to eq(200)
          body = response.read
          expect(body).to include("grpc-status: #{name == 'success' ? 0 : 5}\r\n")
        end
        wait_until(task, "SDK Web spans finish after delivery") { rpc_spans(exporter).size == 2 }
        spans = rpc_spans(exporter)
        expect(spans.map { |span| span.attributes["connectrpc.protocol"] }).to eq(%w[grpc_web grpc_web])
        expect(spans.map { |span| span.attributes["rpc.system.name"] }).to eq(%w[grpc grpc])
        expect(spans.map { |span| span.attributes["connectrpc.delivery"] }).to eq(%w[completed completed])
        expect(spans.map { |span| span.attributes["rpc.response.status_code"] }).to eq(%w[OK NOT_FOUND])
        spans.each do |span|
          observed = records.select { |record| record[2] == span.span_id }
          expect(observed.map(&:first)).to include(:handler, :decode, :handler_ensure, :codec_ensure,
                                                   :application_ensure)
          expect(observed.map { |record| record[1].object_id }.uniq.size).to eq(1)
          expect(observed.map(&:last).uniq).to eq([tls_material.fetch(:identity).first.serial.to_i])
        end
        expect(records.map(&:first)).to include(:encode, :status, :status_ensure)
        expect(host.peer_certificate).to be_nil
      ensure
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
        host&.close(grace: 0)
        bound&.close
      end
    ensure
      provider&.shutdown
    end
  end
end
