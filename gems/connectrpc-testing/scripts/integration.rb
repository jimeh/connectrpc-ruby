# frozen_string_literal: true

# This file also runs from an external cwd with installed gems and copied fixtures.
require "connectrpc/testing"
require "connectrpc/bindings/google_protobuf"
require "echo_connect"

binding = ConnectRPC::Bindings::GoogleProtobuf.new
service = Example::V1::EchoService
type = Example::V1::EchoMessage
echo = lambda do |message, context|
  raise "Repeated request metadata changed" unless context.request_metadata["x-repeat"] == ["one", "two,three"]

  context.response_metadata["x-leading"] = %w[a b]
  context.response_trailers.add_binary("x-binary-bin", "\x00\xff".b)
  message
end
fail_handler = lambda do |message, context|
  context.response_metadata["x-leading"] = ["error"]
  detail = ConnectRPC::ErrorDetail.new(type: type.descriptor.name, value: type.encode(message))
  raise ConnectRPC::Error.new(:invalid_argument, "safe", details: [detail],
                                                         trailers: { "x-error" => %w[yes again] })
end
server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, encoding: :gzip,
                                handlers: service.handlers(echo: echo, fail: fail_handler))
in_process = ConnectRPC::Testing::Transport.new(server: server)
observed = []
transport = lambda do |request|
  response = in_process.call(request)
  observed << [request, response]
  response
end
%i[proto json].product(%i[identity gzip]).each do |codec, encoding|
  core = ConnectRPC::Client.new(base_url: "https://in-process.invalid", binding: binding, transport: transport,
                                codec: codec, encoding: encoding)
  client = service::Client.new(client: core)
  message = type.new(name: "hello 世界", count: 42, data: "\x00\xff".b)
  result = client.echo(message, metadata: { "x-repeat" => ["one", "two,three"] })
  raise "Typed result changed" unless result.message == message && result.message.is_a?(type)
  raise "Leading metadata changed" unless result.metadata["x-leading"] == %w[a b]
  raise "Binary trailer changed" unless result.trailers.binary("x-binary-bin") == ["\x00\xff".b]

  request, response = observed.last
  expected = binding.encode(message, type: type, codec: codec).b
  if encoding == :gzip
    raise "Missing gzip headers" unless request.headers["content-encoding"] == ["gzip"] &&
                                        response.headers["content-encoding"] == ["gzip"]
    raise "Gzip bytes changed" unless Zlib.gunzip(request.body) == expected && Zlib.gunzip(response.body) == expected
  else
    raise "Identity bytes changed" unless request.body.b == expected && response.body.b == expected
  end
  begin
    client.fail(message)
    raise "Expected deliberate error"
  rescue ConnectRPC::Error => e
    raise "Error changed" unless e.code == "invalid_argument" && e.message == "safe"
    raise "Detail changed" unless e.details.first.decode(binding: binding, type: type) == message
    raise "Error metadata changed" unless e.metadata["x-leading"] == ["error"] &&
                                          e.trailers["x-error"] == %w[yes again]
  end
  get_core = ConnectRPC::Client.new(base_url: "https://in-process.invalid", binding: binding, transport: transport,
                                    codec: codec, encoding: encoding, use_get: true)
  get_result = service::Client.new(client: get_core).echo(message, metadata: { "x-repeat" => ["one", "two,three"] })
  raise "Installed generated GET" unless get_result.message == message && observed.last.first.http_method == "GET" &&
                                         observed.last.first.body.empty? &&
                                         get_result.metadata["vary"] == ["Accept-Encoding"]

  puts "Generated #{codec}/#{encoding}: typed success/error/details/metadata and opaque bytes passed " \
       "(Ruby #{RUBY_VERSION})"
end
puts "4 generated codec/encoding scenarios passed"
