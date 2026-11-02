# frozen_string_literal: true

# Used against independent protoc/Buf trees and copied into the isolated package
# cwd. Load paths are supplied by the caller, never by this script.
require "generation/service_connect"
require "generation/types_connect"
require "generation/dotted_connect"
require "generation_empty_connect"
require "connectrpc/bindings/google_protobuf"
require_relative "localhost"

service = GenerationFixture::V1::EchoService
type = GenerationFixture::V1::Message
binding = ConnectRPC::Bindings::GoogleProtobuf.new
handler = lambda do |request, context|
  context.response_metadata.add("x-generated", "yes")
  context.response_trailers.add("x-tail", "yes")
  request
end
failure = lambda do |request, _context|
  detail = ConnectRPC::ErrorDetail.new(type: type.descriptor.name, value: request.to_proto)
  raise ConnectRPC::Error.new(:invalid_argument, "generated error", details: [detail],
                                                                    metadata: { "x-error" => ["yes"] },
                                                                    trailers: { "x-end" => ["yes"] })
end
server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding,
                                handlers: service.handlers(echo: handler, fail: failure, get_http: handler))
host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: server))
begin
  %i[proto json].each do |codec|
    client = service::Client.new(client: ConnectRPC::Client.new(base_url: host.url, binding: binding, codec: codec))
    [type.new, type.new(count: 0)].each do |message|
      response = client.echo(message, timeout: 2)
      abort "Optional presence lost" unless response.message.has_count? == message.has_count?
      abort "Generated response mismatch" unless response.message == message
      abort "Generated metadata/trailers lost" unless response.metadata["x-generated"] == ["yes"] &&
                                                      response.trailers["x-tail"] == ["yes"]
    end
    begin
      client.fail(type.new(count: 42), timeout: 2)
      abort "Expected generated error"
    rescue ConnectRPC::Error => e
      abort "Generated code/message mismatch" unless e.code == "invalid_argument" && e.message == "generated error"
      abort "Generated error detail mismatch" unless e.details.one? &&
                                                     e.details.first.decode(binding: binding,
                                                                            type: type) == type.new(count: 42)
      unless e.metadata["x-error"] == ["yes"] && e.trailers["x-end"] == ["yes"]
        abort "Generated error metadata/trailers lost"
      end
    end
    nested = GenerationFixture::Types::Outer_name::PB__value.new(count: 0)
    abort "Public import/nested class mismatch" unless client.get_http(nested, timeout: 2).message == nested
    puts "Generated #{codec}: success/error/details/metadata/trailers/presence/nesting passed (Ruby #{RUBY_VERSION})"
  end
  abort "Proto2 service type mismatch" unless GenerationFixture::Types::LegacyService::LEGACY_GET.request_type ==
                                              GenerationFixture::Types::Outer_name::PB__value
  abort "Empty service mismatch" unless GenerationFixture::V1::EmptyService.handlers.empty?
  abort "Empty package naming mismatch" unless PB__generation_service::CALL.request_type == Generation_message
  abort "Dot package naming mismatch" unless GenerationFixture::DottedName::Service_name::GET_FOO.name == "Get_Foo"
ensure
  host.close
end
