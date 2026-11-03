# frozen_string_literal: true

require "open3"
require "rbconfig"
require "connectrpc/bindings/google_protobuf"
require_relative "../examples/localhost"

$LOAD_PATH.unshift(File.expand_path("fixtures/generation/ruby", __dir__))
require "generation/service_connect"
require "generation/dotted_connect"
require "generation/google_connect"
require "generation/google_ruby_connect"
require "generation_empty_connect"

RSpec.describe "Generated service integration" do
  let(:service) { GenerationFixture::V1::EchoService }
  let(:type) { GenerationFixture::V1::Message }
  let(:binding) { ConnectRPC::Bindings::GoogleProtobuf.new }

  it "retains schema safety without inferring GET safety from names" do
    service = GenerationFixture::V1::EchoService
    expect(service::ECHO.idempotency).to eq(:no_side_effects)
    expect(service::FAIL.idempotency).to eq(:idempotent)
    expect(service::GET_HTTP.idempotency).to eq(:unknown)
  end

  it "uses concrete official message classes and frozen reusable declarations" do
    expect(service::ECHO.request_type).to equal(type)
    expect(service::ECHO.response_type).to equal(type)
    expect(service::GET_HTTP.request_type).to equal(GenerationFixture::Types::Outer_name::PB__value)
    expect(service::SERVICE.methods).to eq([service::ECHO, service::FAIL, service::GET_HTTP])
    expect(service::SERVICE).to be_frozen
    expect(service::ECHO).to be_frozen
    expect(service::ECHO.path).to eq("/generation_fixture.v1.EchoService/Echo")
    expect(GenerationFixture::V1::ConnectRPC.new.value).to eq("")
  end

  it "loads official Google package and ruby_package messages with identical WKT types" do
    google_service = Google::GenerationFixture::V1::EchoService
    google_type = Google::GenerationFixture::V1::Message
    expect(google_service::ECHO.request_type).to equal(google_type)
    expect(google_service::ECHO.response_type).to equal(google_type)
    expect(google_service::TIME.request_type).to equal(Google::Protobuf::Timestamp)
    expect(google_service::TIME.response_type).to equal(Google::Protobuf::Timestamp)
    mapped_service = Google::GenerationFixture::MappedV1::EchoService
    mapped_type = Google::GenerationFixture::MappedV1::Message
    expect(mapped_service::ECHO.request_type).to equal(mapped_type)
    expect(mapped_service::ECHO.response_type).to equal(mapped_type)
    %i[proto json].each do |codec|
      message = google_type.new(time: Google::Protobuf::Timestamp.new(seconds: 123, nanos: 456))
      decoded = binding.decode(binding.encode(message, type: google_type, codec: codec), type: google_type,
                                                                                         codec: codec)
      expect(decoded.class).to equal(google_type)
      expect(decoded).to eq(message)
    end
  end

  it "forwards omitted, nil, finite, metadata and arbitrary options with return identity" do
    calls = []
    result = Object.new
    recorder = Object.new
    recorder.define_singleton_method(:call) do |method, request, **options|
      calls << [method, request, options]
      result
    end
    client = service::Client.new(client: recorder)
    request = type.new(count: 0)
    [{}, { timeout: nil }, { timeout: 2, metadata: { "x-test" => ["yes"] } }, { custom: true }].each do |options|
      expect(client.echo(request, **options)).to equal(result)
      expect(calls.last).to eq([service::ECHO, request, options])
    end
  end

  it "requires every handler and rejects noncallable values with the keyword name" do
    handler = ->(request, _context) { request }
    expect(service.handlers(echo: handler, fail: handler, get_http: handler)).to eq(
      service::ECHO.path => handler, service::FAIL.path => handler, service::GET_HTTP.path => handler
    )
    expect { service.handlers(echo: handler) }.to raise_error(ArgumentError, /missing keywords/)
    expect do
      service.handlers(echo: handler, fail: false,
                       get_http: handler)
    end.to raise_error(ArgumentError, /fail must respond to call/)
    expect(GenerationFixture::V1::EmptyService::SERVICE.methods).to be_empty
    expect(GenerationFixture::V1::EmptyService.handlers).to eq({})
  end

  it "retains Client option validation, timeout defaults and custom transport/limits" do
    requests = []
    transport = Object.new
    transport.define_singleton_method(:call) do |request|
      requests << request
      ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                        body: request.body)
    end
    core = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport,
                                  request_limit: 10, response_limit: 10)
    client = service::Client.new(client: core)
    client.echo(type.new(count: 0))
    expect(requests.last.deadline).to be_a(ConnectRPC::Deadline)
    expect(requests.last.headers.fetch("connect-timeout-ms").first.to_i).to be_between(29_000, 30_000)
    client.echo(type.new, timeout: nil)
    expect(requests.last.deadline).to be_nil
    expect(requests.last.headers).not_to have_key("connect-timeout-ms")
    expect { client.echo(type.new, unknown: true) }.to raise_error(ArgumentError, /unknown keyword/)
    expect { client.echo(GenerationFixture::V1::ConnectRPC.new) }.to raise_error(ConnectRPC::Error, /Cannot encode/)
    expect { client.echo(type.new(count: 0), timeout: 0) }.to raise_error(ArgumentError)
  end

  %i[proto json].each do |codec|
    it "runs typed #{codec} Rack/NetHTTP success, presence and deliberate error details" do
      handler = lambda do |request, context|
        context.response_metadata.add("x-request", context.request_metadata["x-request"].first)
        context.response_trailers.add("x-tail", "done")
        request
      end
      failure = lambda do |request, _context|
        detail = ConnectRPC::ErrorDetail.new(type: type.descriptor.name, value: request.to_proto)
        raise ConnectRPC::Error.new(:invalid_argument, "safe", details: [detail], metadata: { "x-error" => ["yes"] },
                                                               trailers: { "x-end" => ["yes"] })
      end
      server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding,
                                      handlers: service.handlers(echo: handler, fail: failure, get_http: handler))
      host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: server))
      begin
        client = service::Client.new(client: ConnectRPC::Client.new(base_url: host.url, binding: binding, codec: codec))
        [type.new, type.new(count: 0)].each do |message|
          response = client.echo(message, metadata: { "x-request" => ["generated"] }, timeout: 2)
          expect(response.message).to eq(message)
          expect(response.message.has_count?).to eq(message.has_count?)
          expect(response.metadata["x-request"]).to eq(["generated"])
          expect(response.trailers["x-tail"]).to eq(["done"])
        end
        expect { client.fail(type.new(count: 42), timeout: 2) }.to raise_error(ConnectRPC::Error) { |error|
          expect([error.code, error.message]).to eq(%w[invalid_argument safe])
          expect(error.details.first.decode(binding: binding, type: type)).to eq(type.new(count: 42))
          expect(error.metadata["x-error"]).to eq(["yes"])
          expect(error.trailers["x-end"]).to eq(["yes"])
        }
      ensure
        host.close
      end
    end
  end

  it "preserves official naming for empty, dotted and explicit Ruby packages" do
    expect(PB__generation_service::CALL.request_type).to equal(Generation_message)
    expect(GenerationFixture::DottedName::Service_name::GET_FOO.request_type).to equal(GenerationFixture::DottedName::Message_name)
  end

  it "allows repeated require but fails clearly on deliberate redefinition" do
    expect(require("generation/service_connect")).to be(false)
    expect { load File.expand_path("fixtures/generation/ruby/generation/service_connect.rb", __dir__) }
      .to raise_error(ArgumentError, /Generated service constant already defined/)
  end

  it "guards out-of-request Client constants before reopening their classes" do
    code = <<~CODE
      require "generation/service_pb"
      module GenerationFixture::V1::EchoService
        class Client; end
      end
      require "generation/service_connect"
    CODE
    _output, error, status = Open3.capture3(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__),
                                            "-I", File.expand_path("fixtures/generation/ruby", __dir__), "-e", code)
    expect(status.success?).to be(false)
    expect(error).to include("Generated service constant already defined", "Client")
  end
end
