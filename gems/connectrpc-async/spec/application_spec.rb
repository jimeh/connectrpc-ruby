# frozen_string_literal: true

RSpec.describe "Native Protocol::HTTP application" do
  it "owns unknown methods inside registered services and strips queries without invoking fallback" do
    fallback = []
    application = ConnectRPC::Async::Application.new(server: server, fallback: lambda { |request|
      fallback << request.path
      Protocol::HTTP::Response[204, {}, nil]
    })
    request = Protocol::HTTP::Request["POST", "/example.v1.EchoService/Missing?x=1",
                                      { "content-type" => "application/proto" }]
    expect(application.call(request).status).to eq(501)
    expect(fallback).to be_empty
    expect(application.call(Protocol::HTTP::Request["GET", "/ordinary?x=1"]).status).to eq(204)
    expect(fallback).to eq(["/ordinary?x=1"])
  end

  it "translates HTTP/1 requests without a stream, including absent and empty bodies" do
    application = ConnectRPC::Async::Application.new(server: server)
    payload = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "buffered"))
    [nil, [], [payload]].each do |body|
      incoming = Protocol::HTTP::Request["POST", "#{AsyncHelpers::ECHO::ECHO.path}?q=1",
                                         [["content-type", "application/proto"], ["x-request", "a"],
                                          ["x-request", "b"]], body]
      expect(incoming).not_to respond_to(:stream)
      response = application.call(incoming)
      expect(response.status).to eq(200)
      expect(response.headers["x-echo"]).to eq(%w[a b])
      expect(response.headers["trailer-x-tail"]).to eq(%w[three four])
    ensure
      response&.close
    end
    expect(application.call(Protocol::HTTP::Request["GET", "/other"]).status).to eq(404)
  end

  it "preserves native request compatibility when another host exposes its own stream object" do
    fallback = ->(_) { Protocol::HTTP::Response[204, {}, nil] }
    application = ConnectRPC::Async::Application.new(server: server, fallback: fallback)
    [["POST", AsyncHelpers::ECHO::ECHO.path, 200], ["GET", "/ordinary", 204]].each do |verb, target, status|
      incoming = Protocol::HTTP::Request[verb, target, { "content-type" => "application/proto" }]
      foreign_stream = Struct.new(:declared_length).new(nil)
      incoming.define_singleton_method(:stream) { foreign_stream }
      result = begin
        application.call(incoming)
      rescue StandardError => e
        e
      end
      expect(result).to be_a(Protocol::HTTP::Response)
      expect(result.status).to eq(status)
    ensure
      result.close if result.is_a?(Protocol::HTTP::Response)
    end
  end

  it "validates its server and optional fallback" do
    expect { ConnectRPC::Async::Application.new(server: Object.new) }.to raise_error(ArgumentError)
    expect { ConnectRPC::Async::Application.new(server: server, fallback: Object.new) }.to raise_error(ArgumentError)
  end
end
