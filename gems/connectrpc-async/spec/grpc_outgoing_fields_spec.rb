# frozen_string_literal: true

require "stringio"

RSpec.describe "Complete outgoing gRPC field blocks" do
  %i[request response informational terminal trailers_only].each do |direction|
    [-1, 0, 1].each do |delta|
      it "checks #{direction} pseudoheaders before serialization at limit #{'+' if delta >= 0}#{delta}" do
        reactor do
          bytes = StringIO.new("".b)
          type = direction == :request ? AsyncHelpers::INTERNAL::ClientConnection : AsyncHelpers::INTERNAL::ServerConnection
          connection = type.new(IO::Stream(bytes))
          stream = if direction == :request
                     connection.create_response.stream
                   else
                     AsyncHelpers::INTERNAL::RequestStream.create(connection, 1)
                   end
          stream.grpc_mark!
          fields = if direction == :request
                     [[":scheme", "https"], [":method", "POST"], [":path", "/#{'long' * 1000}"],
                      [":authority", "#{'a' * 200}.example:443"], ["content-type", "application/grpc"]]
                   elsif direction == :terminal
                     [%w[grpc-status 0]]
                   else
                     status = direction == :informational ? "103" : "200"
                     values = [[":status", status], ["content-type", "application/grpc"]]
                     values << %w[grpc-status 0] if direction == :trailers_only
                     values
                   end
          size = fields.sum { |key, value| key.bytesize + value.bytesize + 32 }
          fields << ["x-pad", "x" * (65_536 + delta - size - 37)]
          serialized = []
          allow(stream).to receive(:write_headers) do |*arguments|
            serialized << arguments.first
            Protocol::HTTP2::HeadersFrame.new(1)
          end
          if direction == :terminal
            stream.send_headers([[":status", "200"], ["content-type", "application/grpc"]])
            serialized.clear
          end
          flags = %i[terminal trailers_only].include?(direction) ? Protocol::HTTP2::END_STREAM : 0
          if delta.positive?
            expect { stream.send_headers(fields, flags) }.to raise_error(ConnectRPC::Error) { |error|
              expect(error.code).to eq("resource_exhausted")
              expect(error.cause).to be_nil
            }
            expect(serialized).to be_empty
          else
            stream.send_headers(fields, flags)
            expect(serialized).to eq([fields])
          end
        end
      end
    end
  end

  it "rejects an oversized actual request before RPC dispatch and reuses that connection for a healthy sibling" do
    reactor do
      seen = []
      sent = []
      allow_any_instance_of(AsyncHelpers::INTERNAL::ResponseStream).to receive(:write_headers)
        .and_wrap_original do |write, *arguments|
          sent << arguments.first.to_a
          write.call(*arguments)
        end
      host = ConnectRPC::Async::Host.new(server: server(protocol: :grpc, echo: lambda { |message, _|
        seen << message.name
        message
      })).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: :grpc,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      fields = [[":scheme", "http"], [":method", "POST"], [":path", AsyncHelpers::ECHO::ECHO.path],
                [":authority", "127.0.0.1:#{host.port}"], ["content-type", "application/grpc+proto"],
                ["te", "trailers"], ["grpc-accept-encoding", "identity"]]
      size = fields.sum { |key, value| key.bytesize + value.bytesize + 32 }
      resource = nil
      [0, 1].each do |delta|
        metadata = { "x-pad" => ["x" * (65_536 + delta - size - 37)] }
        if delta.zero?
          expect(core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new(name: "at limit"),
                           timeout: nil, metadata: metadata).message.name).to eq("at limit")
          resource = transport.instance_variable_get(:@resources).first
        else
          expect do
            core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new(name: "oversize"),
                      timeout: nil, metadata: metadata)
          end.to raise_error(ConnectRPC::Error) { |error|
            expect(error.code).to eq("resource_exhausted")
            expect(error.cause).to be_nil
          }
        end
      end
      expect(seen).to eq(["at limit"])
      expect(sent.size).to eq(1)
      expect(core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new(name: "healthy"), timeout: nil).message.name)
        .to eq("healthy")
      expect(seen).to eq(["at limit", "healthy"])
      expect(transport.instance_variable_get(:@resources)).to eq([resource])
      expect(resource.finished?).to be(false)
      expect(transport.instance_variable_get(:@calls)).to be_empty
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
