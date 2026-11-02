# frozen_string_literal: true

# Used against independent protoc/Buf trees and copied into the isolated package
# cwd. Load paths are supplied by the caller, never by this script.
require "generation/service_connect"
require "generation/types_connect"
require "generation/dotted_connect"
require "generation/google_connect"
require "generation/google_ruby_connect"
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
google_service = Google::GenerationFixture::V1::EchoService
google_type = Google::GenerationFixture::V1::Message
mapped_service = Google::GenerationFixture::MappedV1::EchoService
mapped_type = Google::GenerationFixture::MappedV1::Message
abort "Google request class mismatch" unless google_service::ECHO.request_type.equal?(google_type)
abort "Google response class mismatch" unless google_service::ECHO.response_type.equal?(google_type)
abort "WKT class mismatch" unless google_service::TIME.request_type.equal?(Google::Protobuf::Timestamp) &&
                                  google_service::TIME.response_type.equal?(Google::Protobuf::Timestamp)
abort "Mapped Google class mismatch" unless mapped_service::ECHO.request_type.equal?(mapped_type) &&
                                            mapped_service::ECHO.response_type.equal?(mapped_type)
handlers = service.handlers(echo: handler, fail: failure, get_http: handler)
handlers.merge!(google_service.handlers(echo: handler, time: handler))
handlers.merge!(mapped_service.handlers(echo: handler))
server = ConnectRPC::Server.new(services: [service::SERVICE, google_service::SERVICE, mapped_service::SERVICE],
                                binding: binding, handlers: handlers)
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
    google_client = google_service::Client.new(client: ConnectRPC::Client.new(base_url: host.url, binding: binding,
                                                                              codec: codec))
    timestamp = Google::Protobuf::Timestamp.new(seconds: 123, nanos: 456)
    google_message = google_type.new(time: timestamp)
    google_response = google_client.echo(google_message, timeout: 2).message
    abort "Google typed round trip mismatch" unless google_response.instance_of?(google_type) &&
                                                    google_response == google_message
    time_response = google_client.time(timestamp, timeout: 2).message
    abort "WKT typed round trip mismatch" unless time_response.instance_of?(Google::Protobuf::Timestamp) &&
                                                 time_response == timestamp
    mapped_client = mapped_service::Client.new(client: ConnectRPC::Client.new(base_url: host.url, binding: binding,
                                                                              codec: codec))
    mapped_message = mapped_type.new(value: Google::Protobuf::Empty.new)
    mapped_response = mapped_client.echo(mapped_message, timeout: 2).message
    abort "Mapped Google round trip mismatch" unless mapped_response.instance_of?(mapped_type) &&
                                                     mapped_response == mapped_message
    puts "Google #{codec}: package/ruby_package/WKT exact class identity and typed calls passed (Ruby #{RUBY_VERSION})"
  end
  abort "Proto2 service type mismatch" unless GenerationFixture::Types::LegacyService::LEGACY_GET.request_type ==
                                              GenerationFixture::Types::Outer_name::PB__value
  abort "Empty service mismatch" unless GenerationFixture::V1::EmptyService.handlers.empty?
  abort "Empty package naming mismatch" unless PB__generation_service::CALL.request_type == Generation_message
  abort "Dot package naming mismatch" unless GenerationFixture::DottedName::Service_name::GET_FOO.name == "Get_Foo"
ensure
  host.close
end
