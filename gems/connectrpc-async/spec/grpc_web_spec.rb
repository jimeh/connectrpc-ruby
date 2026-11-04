# frozen_string_literal: true

RSpec.describe "Web request field retention" do
  it "checks the complete H2 block including pseudoheaders at 64KiB before copies, lifecycle and invocation" do
    reactor do |task|
      seen = []
      calls = []
      completions = []
      subscriber = Object.new
      subscriber.define_singleton_method(:start) do |call|
        calls << call
        Object.new.tap do |observer|
          observer.define_singleton_method(:finished) do |completion|
            completions << completion
          end
        end
      end
      copies = []
      allow(AsyncHelpers::INTERNAL).to receive(:headers).and_wrap_original do |original, incoming|
        copies << incoming
        original.call(incoming)
      end
      target = server(protocol: :grpc_web, instrumentation: ConnectRPC::Instrumentation.new(subscribers: [subscriber]),
                      echo: lambda { |message, _|
                        seen << message
                        message
                      })
      host = ConnectRPC::Async::Host.new(server: target).start
      raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
      connection = Async::HTTP::Protocol::HTTP2.client(raw)
      fields = [[":method", "POST"], [":scheme", "http"], [":authority", "127.0.0.1:#{host.port}"],
                [":path", AsyncHelpers::ECHO::ECHO.path], ["content-type", "application/grpc-web"]]
      size = fields.sum { |key, value| key.bytesize + value.bytesize + 32 }
      response = nil
      [0, 1].each do |delta|
        block = fields + [["x-pad", "x" * (65_536 + delta - size - 37)]]
        expect(block.sum { |key, value| key.bytesize + value.bytesize + 32 }).to eq(65_536 + delta)
        # Without the real pseudoheaders, the excess case would incorrectly fit.
        expect(block.reject { |key, _| key.start_with?(":") }.sum { |key, value| key.bytesize + value.bytesize + 32 })
          .to be < 65_536
        response = connection.create_response
        response.build_request(block)
        response.stream.send_headers(block, Protocol::HTTP2::END_STREAM)
        if delta.zero?
          connection.read_response(response)
          expect(response.status).to eq(200)
          expect(response.read).to include("grpc-status: 12\r\n") # Header-only request, not an empty protobuf frame.
          wait_until(task, "at-limit Web completion") { completions.size == 1 }
          expect(calls.size).to eq(1)
          expect(copies).not_to be_empty
          copies.clear
        else
          expect { connection.read_response(response) }.to raise_error(Protocol::HTTP2::StreamError) { |error|
            expect(error.code).to eq(Protocol::HTTP2::Error::ENHANCE_YOUR_CALM)
          }
          expect(copies).to be_empty
          expect(calls.size).to eq(1)
          expect(completions.size).to eq(1)
        end
        response.close
      end
      expect(seen).to be_empty
      healthy = connection.call(Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path,
                                                        { "content-type" => "application/grpc-web" },
                                                        [[0, 0].pack("CN")], scheme: "http",
                                                                             authority: "127.0.0.1:#{host.port}"])
      expect(healthy.status).to eq(200)
      expect(healthy.read).to include("grpc-status: 0\r\n")
      wait_until(task, "healthy sibling completes and admission retires") do
        completions.size == 2 && host_session(host).instance_variable_get(:@admitted).all? do |stream, done|
          done && stream.closed?
        end
      end
      expect(seen.size).to eq(1)
      expect(calls.size).to eq(2)
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
