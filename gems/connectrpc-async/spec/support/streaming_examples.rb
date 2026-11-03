# frozen_string_literal: true

RSpec.shared_examples "native streaming matrix" do
  def stream_methods
    @stream_methods ||= [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod]
                        .map do |type|
      type.new(service: "test.Streams", name: type.name.split("::").last,
               request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
    end
  end

  def streaming_server(handlers, **)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: "test.Streams", methods: stream_methods)],
                           binding: ConnectRPC::Bindings::GoogleProtobuf.new, handlers: handlers, **)
  end

  it "publishes real CS response HEADERS before any request DATA is produced" do
    reactor do
      _, cs, = stream_methods
      handler = lambda do |reader, context|
        context.send_headers(metadata: { "x-before-input" => "yes" })
        message = reader.receive
        expect(reader.receive).to be_nil
        message
      end
      host, origin, listener = build_stream_host(streaming_server({ cs.path => handler }), nil, "http")
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      request = ConnectRPC::StreamRequest.new(url: URI(origin + cs.path), http_method: "POST",
                                              headers: { "content-type" => ["application/connect+proto"] },
                                              deadline: nil, lifecycle: nil, cancellation: nil)
      transport.open_stream(request) do |exchange|
        expect(exchange.response_head.headers["x-before-input"]).to eq(["yes"])
        message = AsyncHelpers::MESSAGE.new(name: "after-headers")
        bytes = AsyncHelpers::MESSAGE.encode(message)
        exchange.write([0, bytes.bytesize].pack("CN") + bytes)
        exchange.close_send
        engine = ConnectRPC.const_get(:Streaming)
        frames = engine::Framing.new(input: exchange, request: false, wire_limit: 1024, limit: 1024)
        expect(AsyncHelpers::MESSAGE.decode(frames.read_frame.last)).to eq(message)
        expect(frames.read_frame.first).to eq(2)
        expect(exchange.read(1)).to be_nil
        exchange.finish
      end
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
      listener&.close
    end
  end

  %i[proto json].product(%i[identity gzip], %i[h2c tls mtls]).each do |codec, encoding, security|
    it "runs incremental SS/CS/bidi #{codec}/#{encoding}/#{security} with early headers" do
      reactor do
        server_stream, client_stream, bidi = stream_methods
        release = Async::Queue.new
        cs_progress = Async::Queue.new
        handlers = {
          server_stream.path => lambda { |message, writer, context|
            context.send_headers(metadata: { "x-phase" => ["before-gate"] })
            release.dequeue
            writer.send_message(message)
            release.dequeue
            context.response_trailers["x-tail"] = ["done"]
            42 # SS handler return values never emit payloads.
          },
          client_stream.path => lambda { |reader, context|
            context.send_headers(metadata: { "x-phase" => ["before-input"] })
            count = 0
            reader.each do |_|
              count += 1
              cs_progress.enqueue(count)
            end
            AsyncHelpers::MESSAGE.new(name: count.to_s)
          },
          bidi.path => lambda { |duplex, _context|
            duplex.each { |message| duplex.send_message(message) }
          }
        }
        target = streaming_server(handlers, encoding: encoding)
        identity = security == :mtls ? tls_material.fetch(:client_ca) : nil
        tls = host_tls(anchors: identity ? [identity] : nil) unless security == :h2c
        scheme = security == :h2c ? "http" : "https"
        host, origin, listener = build_stream_host(target, tls, scheme)
        with_ca do |ca_file|
          tls_options = security == :h2c ? {} : { ca_file: ca_file }
          if security == :mtls
            certificate, key = tls_material.fetch(:identity)
            tls_options.merge!(certificate_chain: [certificate.to_pem], private_key: key.private_to_pem)
          end
          transport = ConnectRPC::Async::Transport.new(origin: origin, **tls_options)
          core = ConnectRPC::Client.new(base_url: origin, transport: transport, codec: codec, encoding: encoding,
                                        binding: ConnectRPC::Bindings::GoogleProtobuf.new, use_get: true)
          expect(core.server_stream(server_stream, AsyncHelpers::MESSAGE.new(name: "first")) do |reader|
            expect(reader.headers["x-phase"]).to eq(["before-gate"])
            release.enqueue(true)
            expect(reader.receive.name).to eq("first")
            release.enqueue(true)
            expect(reader.receive).to be_nil
            expect(reader.trailers["x-tail"]).to eq(["done"])
          end).to be_nil
          response = core.client_stream(client_stream) do |writer|
            writer.send_message(AsyncHelpers::MESSAGE.new)
            expect(cs_progress.dequeue).to eq(1)
            writer.send_message(AsyncHelpers::MESSAGE.new)
            expect(cs_progress.dequeue).to eq(2)
          end
          expect(response.message.name).to eq("2")
          expect(response.metadata["x-phase"]).to eq(["before-input"])
          core.bidi_stream(bidi) do |duplex|
            duplex.send_message(AsyncHelpers::MESSAGE.new(name: "before-half-close"))
            expect(duplex.receive.name).to eq("before-half-close")
            duplex.close_send
            expect(duplex.receive).to be_nil
          end
        ensure
          transport&.close(grace: 0)
        end
      ensure
        release&.enqueue(true)
        host&.close(grace: 0)
        listener&.close
      end
    end
  end
end

RSpec.shared_examples "generated mixed streaming client" do
  it "uses one generated Client for safe GET and all streaming POST operations with nil deadlines" do
    generated = File.expand_path("../../../../spec/fixtures/generation/ruby", __dir__)
    $LOAD_PATH.unshift(generated)
    begin
      require "generation/streaming_connect"
    ensure
      $LOAD_PATH.delete(generated)
    end
    reactor do
      service = GenerationFixture::Streaming::Mixed
      message_type = GenerationFixture::Streaming::Item
      target = ConnectRPC::Server.new(
        services: [service::SERVICE], binding: ConnectRPC::Bindings::GoogleProtobuf.new,
        handlers: service.handlers(
          lookup: ->(message, _) { message },
          watch: ->(message, writer, _) { 2.times { writer.send_message(message) } },
          collect: lambda { |reader, _|
            names = +""
            reader.each do |message|
              names << "," unless names.empty?
              names << message.name
            end
            message_type.new(name: names)
          },
          chat: ->(duplex, _) { duplex.each { |message| duplex.send_message(message) } }
        )
      )
      host, origin, listener = build_stream_host(target, nil, "http")
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      dispatched = []
      allow(transport).to(receive(:call).and_wrap_original do |original, request|
        dispatched << request
        original.call(request)
      end)
      allow(transport).to receive(:open_stream).and_wrap_original do |original, request, &block|
        dispatched << request
        original.call(request, &block)
      end
      client = service::Client.new(client: ConnectRPC::Client.new(base_url: origin, transport: transport,
                                                                  binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                                                  use_get: true))
      source = ConnectRPC::CancellationSource.new
      options = { timeout: nil, cancellation: source.token, metadata: { "x-client" => ["mixed"] } }
      expect(client.lookup(message_type.new(name: "get"), **options).message.name).to eq("get")
      replies = []
      client.watch(message_type.new(name: "post"), **options) { |reader| reader.each { |reply| replies << reply.name } }
      expect(replies).to eq(%w[post post])
      expect(client.collect(**options) { |writer| writer.send_message(message_type.new(name: "collect")) }.message.name)
        .to eq("collect")
      client.chat(**options) do |duplex|
        duplex.send_message(message_type.new(name: "chat"))
        expect(duplex.receive.name).to eq("chat")
        duplex.close_send
        expect(duplex.receive).to be_nil
      end
      expect(dispatched.map(&:http_method)).to eq(%w[GET POST POST POST])
      expect(dispatched.map(&:deadline)).to eq([nil] * 4)
      expect(dispatched.map { |request| request.headers["connect-timeout-ms"] }).to eq([nil] * 4)
      expect(dispatched.map { |request| request.headers["x-client"] }).to eq([["mixed"]] * 4)
      expect(source.token.cancelled?).to be(false)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
      listener&.close
    end
  end
end
