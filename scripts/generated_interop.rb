# frozen_string_literal: true

# Run in a separate process: official echo_pb and EchoExample's descriptor cannot
# be registered together in the global Google descriptor pool.
require "connectrpc/bindings/google_protobuf"
require_relative "interop"

$LOAD_PATH.unshift(File.expand_path("../spec/fixtures/generation/echo", __dir__))
require "echo_connect"

module EchoExample
  Message = Example::V1::EchoMessage
  SERVICE = Example::V1::EchoService::SERVICE
  ECHO = Example::V1::EchoService::ECHO
  FAIL = Example::V1::EchoService::FAIL
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new
end

binary = ARGV.fetch(0)
service = Example::V1::EchoService
Open3.popen2(binary, "serve") do |input, output, process|
  raise "Generated Go peer did not announce readiness" unless output.wait_readable(10)

  url = output.gets&.strip
  Interop.check(url&.start_with?("http://127.0.0.1:"), "Invalid generated peer readiness: #{url.inspect}")
  %i[proto json].each do |codec|
    client = service::Client.new(client: ConnectRPC::Client.new(base_url: url, binding: EchoExample::BINDING,
                                                                codec: codec))
    message = EchoExample::Message.new(name: "", count: 9_007_199_254_740_993, mood: :HAPPY,
                                       at: Google::Protobuf::Timestamp.new(seconds: 1_704_067_200), data: "\x00\xff".b)
    result = client.echo(message, metadata: { "x-request" => ["generated"] }, timeout: 2)
    Interop.check(result.message == message && result.message.has_name?, "Generated Ruby->Go #{codec} message/presence")
    Interop.check(result.metadata["x-echo"] == ["generated"] && result.trailers["x-tail"] == %w[three four],
                  "Generated Ruby->Go #{codec} metadata/trailers")
    begin
      client.fail(message, timeout: 2)
      raise "Expected generated Go error"
    rescue ConnectRPC::Error => e
      Interop.check(e.code == "invalid_argument" && e.message == "deliberate", "Generated Ruby->Go #{codec} error")
      Interop.check(e.details.one?, "Generated Go error detail count")
      decoded = e.details.first.decode(binding: EchoExample::BINDING, type: EchoExample::Message)
      Interop.check(decoded == message, "Generated Ruby->Go #{codec} typed detail")
    end
    puts "PASS generated Ruby->Go #{codec} success/error/detail/presence/metadata/trailers"
  end
ensure
  input.close
  unless process.join(10)
    Process.kill("TERM", process.pid)
    process.join
    raise "Generated Go peer shutdown timed out"
  end
  Interop.check(process.value.success?, "Generated Go server failed")
end
handlers = Interop.ruby_handlers
server = ConnectRPC::Server.new(services: [service::SERVICE], binding: EchoExample::BINDING, request_limit: 1024,
                                handlers: service.handlers(echo: handlers.fetch(service::ECHO.path),
                                                           fail: handlers.fetch(service::FAIL.path)))
host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: server))
begin
  system(binary, "client", host.url, "Ruby", exception: true)
ensure
  host.close
end
puts "Generated independent path: Ruby->Go success/error and Go->Ruby success/error/limit in both codecs passed"
