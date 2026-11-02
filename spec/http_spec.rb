# frozen_string_literal: true

require_relative "../examples/echo_schema"
require_relative "../examples/localhost"

RSpec.describe "real Rack/NetHTTP unary calls" do
  let(:handler) do
    lambda do |request, context|
      context.response_metadata["x-echo"] = context.request_metadata["x-request"]
      context.response_trailers["x-tail"] = %w[one two]
      request
    end
  end
  let(:failure) do
    lambda do |request, _context|
      detail = ConnectRPC::ErrorDetail.new(type: "example.v1.EchoMessage", value: request.to_proto)
      raise ConnectRPC::Error.new(:invalid_argument, "deliberate", details: [detail],
                                                                   metadata: { "x-error" => %w[a b] },
                                                                   trailers: { "x-end" => %w[c d] })
    end
  end

  around do |example|
    server = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
                                    handlers: { EchoExample::ECHO.path => handler, EchoExample::FAIL.path => failure })
    @host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: server))
    example.run
  ensure
    @host&.close
  end

  %i[proto json].each do |codec|
    it "exchanges typed #{codec}, host-visible repeated metadata, trailers and errors" do
      client = ConnectRPC::Client.new(base_url: @host.url, binding: EchoExample::BINDING, codec: codec)
      message = EchoExample::Message.new(name: "", count: 9_007_199_254_740_993, mood: :HAPPY)
      response = client.call(EchoExample::ECHO, message, metadata: { "x-request" => %w[a b] })
      expect(response.message).to eq(message)
      # rackup/WEBrick coalesces duplicates. These assertions describe what the
      # hosting boundary actually exposes, including the comma and space.
      expect(response.metadata["x-echo"]).to eq(["a, b"])
      expect(response.trailers["x-tail"]).to eq(["one, two"])
      expect { client.call(EchoExample::FAIL, message) }.to raise_error(ConnectRPC::Error) do |error|
        expect(error.code).to eq("invalid_argument")
        expect(error.message).to eq("deliberate")
        expect(error.metadata["x-error"]).to eq(["a, b"])
        expect(error.trailers["x-end"]).to eq(["c, d"])
        expect(error.details.first.decode(binding: EchoExample::BINDING, type: EchoExample::Message)).to eq(message)
      end
    end
  end

  it "enforces response limits over actual HTTP" do
    client = ConnectRPC::Client.new(base_url: @host.url, binding: EchoExample::BINDING, response_limit: 2)
    expect do
      client.call(EchoExample::ECHO, EchoExample::Message.new(name: "long"), metadata: { "x-request" => ["ok"] })
    end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
  end
  it "observes cooperative server expiry over a real localhost request" do
    observed = Queue.new
    allow(handler).to receive(:call) do |_request, context|
      mutex = Mutex.new
      condition = ConditionVariable.new
      # This bounded timed wait measures the inbound protocol deadline itself.
      # The handler cooperates; the host is not interrupting arbitrary work.
      mutex.synchronize do
        condition.wait(mutex, context.deadline.remaining) until context.cancelled?
      end
      observed << context.cancelled?
      context.check!
    end
    uri = URI(@host.url + EchoExample::ECHO.path)
    http = Net::HTTP.new(uri.hostname, uri.port, nil)
    http.open_timeout = http.read_timeout = http.write_timeout = 2
    request = Net::HTTP::Post.new(uri.request_uri, "content-type" => "application/proto",
                                                   "accept-encoding" => "identity", "connect-timeout-ms" => "10")
    request.body = EchoExample::Message.new(name: "deadline").to_proto
    response = http.start { |connection| connection.request(request) }
    expect(observed.pop(timeout: 1)).to be(true)
    expect(response.code).to eq("504")
    expect(JSON.parse(response.body)["code"]).to eq("deadline_exceeded")
  end
end
