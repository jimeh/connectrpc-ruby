# frozen_string_literal: true

RSpec.describe "Async native gRPC" do
  def declarations
    @declarations ||= [ConnectRPC::Method, ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod,
                       ConnectRPC::BidiStreamMethod].each_with_index.map do |kind, index|
      kind.new(service: "test.GRPC", name: "Run#{index}", request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
    end
  end

  def grpc_server(**)
    unary, ss, cs, bidi = declarations
    handlers = {
      unary.path => lambda { |request, context|
        expect(context).to be_instance_of(ConnectRPC::Context)
        context.response_trailers["x-tail"] = ["done"]
        request
      },
      ss.path => lambda { |request, writer, context|
        expect(context).to be_instance_of(ConnectRPC::StreamingContext)
        writer.send_headers(metadata: { "x-head" => ["ready"] })
        writer.send_message(request)
        writer.send_message(AsyncHelpers::MESSAGE.new(name: "second"))
      },
      cs.path => lambda { |reader, _context|
        values = []
        reader.each { |message| values << message.name } # rubocop:disable Style/MapIntoArray -- RequestReader has block-only each.
        AsyncHelpers::MESSAGE.new(name: values.join(","))
      },
      bidi.path => lambda { |duplex, _context|
        duplex.each { |message| duplex.send_message(message) }
      }
    }
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: "test.GRPC", methods: declarations)],
                           binding: ConnectRPC::Bindings::GoogleProtobuf.new, handlers: handlers, protocol: :auto, **)
  end

  %i[identity gzip].each do |encoding|
    it "executes all four shapes over real h2c with #{encoding} and final HEADERS" do
      reactor do
        host = ConnectRPC::Async::Host.new(server: grpc_server(encoding: encoding)).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        client = ConnectRPC::Client.new(base_url: origin, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                        transport: transport, protocol: :grpc, encoding: encoding)
        begin
          unary, ss, cs, bidi = declarations
          result = client.call(unary, AsyncHelpers::MESSAGE.new(name: "unary"))
          expect(result.message.name).to eq("unary")
          expect(result.trailers["x-tail"]).to eq(["done"])
          values = []
          client.server_stream(ss, AsyncHelpers::MESSAGE.new(name: "first")) do |reader|
            expect(reader.headers["x-head"]).to eq(["ready"])
            reader.each { |message| values << message.name }
          end
          expect(values).to eq(%w[first second])
          result = client.client_stream(cs) do |writer|
            writer.send_message(AsyncHelpers::MESSAGE.new(name: "a"))
            writer.send_message(AsyncHelpers::MESSAGE.new(name: "b"))
          end
          expect(result.message.name).to eq("a,b")
          client.bidi_stream(bidi) do |duplex|
            duplex.send_message(AsyncHelpers::MESSAGE.new(name: "chat"))
            expect(duplex.receive.name).to eq("chat")
            duplex.close_send
            expect(duplex.receive).to be_nil
          end
        ensure
          transport.close
          host.close
        end
      end
    end
  end
end
