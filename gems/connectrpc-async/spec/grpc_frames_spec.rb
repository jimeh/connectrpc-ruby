# frozen_string_literal: true

RSpec.describe "gRPC HTTP/2 frame evidence" do
  def with_grpc_peer(responder)
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      peer = task.async(finished: false) do
        raw, = listener.accept
        connection = Async::HTTP::Protocol::HTTP2.server(raw)
        while (incoming = connection.requests.dequeue)
          responder.call(incoming)
        end
      ensure
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      declaration = ConnectRPC::BidiStreamMethod.new(service: "wire.GRPC", name: "Chat",
                                                     request_type: AsyncHelpers::MESSAGE,
                                                     response_type: AsyncHelpers::MESSAGE)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: :grpc,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      yield core, declaration, transport, task
    ensure
      transport&.close(grace: 0)
      peer&.cancel
      peer&.wait
      bound&.close
    end
  end

  def head = [[":status", "200"], ["content-type", "application/grpc+proto"]]

  %w[0 00 0000].each do |status|
    it "accepts actual trailers-only END_STREAM #{status} without treating custom trailers as leading metadata" do
      responder = lambda do |incoming|
        incoming.stream.send_headers(head + [["grpc-status", status], %w[x-tail done]], Protocol::HTTP2::END_STREAM)
      end
      with_grpc_peer(responder) do |core, declaration, transport|
        core.bidi_stream(declaration) do |duplex|
          duplex.close_send
          expect(duplex.headers.to_h).to be_empty
          expect(duplex.receive).to be_nil
          expect(duplex.trailers["x-tail"]).to eq(["done"])
        end
        expect(transport.instance_variable_get(:@calls)).to be_empty
      end
    end
  end

  it "ignores nonterminal status after informational HEADERS and uses actual trailers" do
    responder = lambda do |incoming|
      incoming.stream.send_headers([[":status", "103"], ["x-info", "hint"]])
      incoming.stream.send_headers(head + [%w[grpc-status 9], %w[x-head ready]])
      incoming.stream.send_headers([%w[grpc-status 00], %w[x-tail done]], Protocol::HTTP2::END_STREAM)
    end
    with_grpc_peer(responder) do |core, declaration|
      core.bidi_stream(declaration) do |duplex|
        duplex.close_send
        expect(duplex.headers["x-head"]).to eq(["ready"])
        expect(duplex.receive).to be_nil
        expect(duplex.trailers["x-tail"]).to eq(["done"])
      end
    end
  end

  it "drains queued response DATA before committing valid final status followed by NO_ERROR" do
    message = AsyncHelpers::MESSAGE.new(name: "queued before reset")
    bytes = AsyncHelpers::MESSAGE.encode(message)
    responder = lambda do |incoming|
      incoming.stream.send_headers(head)
      incoming.stream.send_data([0, bytes.bytesize].pack("CN") + bytes)
      incoming.stream.send_headers([%w[grpc-status 0]], Protocol::HTTP2::END_STREAM)
      incoming.stream.send_reset_stream(Protocol::HTTP2::Error::NO_ERROR) unless incoming.stream.closed?
    end
    with_grpc_peer(responder) do |core, declaration, transport, task|
      core.bidi_stream(declaration, timeout: nil) do |duplex|
        duplex.headers
        stream = transport.instance_variable_get(:@calls).keys.first.fetch(:response).stream
        wait_until(task, "actual NO_ERROR after terminal status before message consumption") do
          stream.grpc_reset&.zero?
        end
        expect(duplex.receive).to eq(message)
        expect(duplex.receive).to be_nil
      end
    end
  end

  %i[data_end empty_trailers second_nonfinal].each do |scenario|
    it "rejects #{scenario} as successful terminal evidence" do
      responder = lambda do |incoming|
        incoming.stream.send_headers(head)
        case scenario
        when :data_end then incoming.stream.send_data(nil, Protocol::HTTP2::END_STREAM)
        when :empty_trailers then incoming.stream.send_headers([], Protocol::HTTP2::END_STREAM)
        else incoming.stream.send_headers([%w[x-nonfinal invalid]])
        end
      end
      with_grpc_peer(responder) do |core, declaration|
        expect do
          core.bidi_stream(declaration) do |duplex|
            duplex.close_send
            duplex.receive
          end
        end.to raise_error(ConnectRPC::Error) { |error|
          expect(error.code).to eq(scenario == :empty_trailers ? "unknown" : "internal")
          expect(error.cause).to be_nil
        }
      end
    end
  end

  { 0 => "internal", 1 => "internal", 2 => "internal", 3 => "resource_exhausted", 4 => "internal", 5 => "internal",
    6 => "internal", 7 => "unavailable", 8 => "canceled", 9 => "internal", 10 => "internal",
    11 => "resource_exhausted", 12 => "permission_denied", 13 => "internal", 99 => "internal" }.each do |reset, code|
    it "maps peer reset #{reset} to #{code} without replay and keeps a same-connection sibling healthy" do
      calls = 0
      responder = lambda do |incoming|
        calls += 1
        if calls == 1
          incoming.stream.send_reset_stream(reset)
        else
          incoming.stream.send_headers(head + [%w[grpc-status 0]], Protocol::HTTP2::END_STREAM)
        end
      end
      with_grpc_peer(responder) do |core, declaration, transport|
        expect do
          core.bidi_stream(declaration) do |duplex|
            duplex.close_send
            duplex.receive
          end
        end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(code) }
        expect(calls).to eq(1)
        core.bidi_stream(declaration) do |duplex|
          duplex.close_send
          expect(duplex.receive).to be_nil
        end
        expect(calls).to eq(2)
        expect(transport.instance_variable_get(:@resources).size).to eq(1)
      end
    end
  end

  %i[informational initial terminal trailers_only missing_media].each do |block|
    it "bounds decoded #{block} fields including CONTINUATION before application copies, preserving a sibling" do
      calls = 0
      overflow = [["x-large", "x" * 65_500]]
      responder = lambda do |incoming|
        calls += 1
        if calls > 1
          incoming.stream.send_headers(head + [%w[grpc-status 0]], Protocol::HTTP2::END_STREAM)
        else
          case block
          when :informational then incoming.stream.send_headers([[":status", "103"], *overflow])
          when :terminal
            incoming.stream.send_headers(head)
            incoming.stream.send_headers([%w[grpc-status 0], *overflow], Protocol::HTTP2::END_STREAM)
          when :trailers_only
            incoming.stream.send_headers(head + [%w[grpc-status 0], *overflow], Protocol::HTTP2::END_STREAM)
          when :missing_media
            incoming.stream.send_headers([[":status", "200"], *overflow])
          else incoming.stream.send_headers(head + overflow)
          end
        end
      end
      with_grpc_peer(responder) do |core, declaration, transport|
        expect do
          core.bidi_stream(declaration, timeout: nil) do |duplex|
            duplex.close_send
            duplex.receive
          end
        end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
        core.bidi_stream(declaration) do |duplex|
          duplex.close_send
          expect(duplex.receive).to be_nil
        end
        expect(transport.instance_variable_get(:@resources).size).to eq(1)
      end
    end
  end
end

RSpec.describe "gRPC request field retention" do
  ["application/grpc", "application/grpc+json", "duplicate"].each do |media|
    it "caps #{media} intent before backend normalization, observers and handlers while preserving a sibling" do
      reactor do
        seen = []
        observer_calls = []
        subscriber = Object.new
        subscriber.define_singleton_method(:start) do |call|
          observer_calls << call
          nil
        end
        bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
        target = server(protocol: :grpc, instrumentation: bus, echo: lambda { |message, _|
          seen << message
          message
        })
        host = ConnectRPC::Async::Host.new(server: target).start
        raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
        connection = Async::HTTP::Protocol::HTTP2.client(raw)
        response = connection.create_response
        fields = [[":method", "POST"], [":scheme", "http"], [":authority", "127.0.0.1:#{host.port}"],
                  [":path", AsyncHelpers::ECHO::ECHO.path],
                  ["content-type", media == "duplicate" ? "application/grpc" : media],
                  ["x-large", "x" * 65_500]]
        fields << ["content-type", "application/grpc+json"] if media == "duplicate"
        response.stream.send_headers(fields, Protocol::HTTP2::END_STREAM)
        expect { connection.read_response(response) }.to raise_error(Protocol::HTTP2::StreamError) { |error|
          expect(error.code).to eq(Protocol::HTTP2::Error::ENHANCE_YOUR_CALM)
        }
        expect(seen).to be_empty
        expect(observer_calls).to be_empty
        response.close
        message = AsyncHelpers::MESSAGE.new(name: "healthy sibling")
        bytes = AsyncHelpers::MESSAGE.encode(message)
        healthy = connection.create_response
        healthy.send_request(Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path,
                                                     { "content-type" => "application/grpc" },
                                                     [[0, bytes.bytesize].pack("CN") + bytes],
                                                     scheme: "http",
                                                     authority: "127.0.0.1:#{host.port}"])
        connection.read_response(healthy)
        body = healthy.read
        expect(body.byteslice(5..)).to eq(bytes)
        expect(seen).to eq([message])
        expect(host_session(host).instance_variable_get(:@resources).size).to eq(1)
      ensure
        healthy&.close
        response&.close
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
        host&.close(grace: 0)
      end
    end
  end
end

RSpec.describe "gRPC outbound field overflow" do
  %i[leading terminal].each do |phase|
    it "resets #{phase} overflow with resource_exhausted and preserves Connect on the same connection" do
      reactor do
        target = server(protocol: :auto, grpc_status_wire_limit: phase == :terminal ? 43 : 65_536,
                        echo: lambda { |message, context|
                          if phase == :leading && message.name == "overflow"
                            context.response_metadata["x-large"] = ["x" * 65_500]
                          end
                          message
                        })
        host = ConnectRPC::Async::Host.new(server: target).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: :grpc,
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new)
        expect do
          core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new(name: "overflow"))
        end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
        connect = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                         binding: ConnectRPC::Bindings::GoogleProtobuf.new)
        expect(connect.call(AsyncHelpers::ECHO::ECHO,
                            AsyncHelpers::MESSAGE.new(name: "healthy")).message.name).to eq("healthy")
        expect(transport.instance_variable_get(:@resources).size).to eq(1)
      ensure
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end
end
