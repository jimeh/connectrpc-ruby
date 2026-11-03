# frozen_string_literal: true

require_relative "interop"
require_relative "interop_gzip"
require_relative "../examples/echo_schema"

# Scenario setup, real wire assertions and cleanup stay together.
# rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Metrics/BlockLength, Metrics/CyclomaticComplexity
module GetInterop
  def self.ruby_clients(url)
    %i[proto json].product(%i[identity gzip]).each do |codec, encoding|
      observed = GzipInterop::ObservedTransport.new
      client = ConnectRPC::Client.new(base_url: url, binding: EchoExample::BINDING, codec: codec,
                                      encoding: encoding, transport: observed, use_get: true)
      message = EchoExample::Message.new(name: "GET 雪 +", count: 42, data: "\x00\xff".b)
      result = client.call(EchoExample::ECHO, message, metadata: { "x-request" => ["get"], "x-get-probe" => ["yes"] })
      Interop.check(result.message == message, "GET Ruby->Go typed message")
      Interop.check(result.metadata["x-observed-http-method"] == ["GET"], "Go did not observe GET")
      Interop.check(result.metadata["x-echo"] == ["get"] && result.trailers["x-tail"] == %w[three four],
                    "GET Ruby->Go metadata/trailers")
      outgoing = observed.request_wire
      Interop.check(outgoing.http_method == "GET" && outgoing.body.empty?, "GET not selected")
      params = URI.decode_www_form(outgoing.url.query).to_h
      payload = params.fetch("message")
      payload = ConnectRPC::Metadata.decode_binary(payload.tr("-_", "+/")) if params["base64"] == "1"
      wire_size = payload.bytesize
      payload = Zlib.gunzip(payload) if encoding == :gzip
      Interop.check(EchoExample::BINDING.decode(payload, type: EchoExample::Message, codec: codec) == message,
                    "GET query payload mismatch")
      puts "GET_WIRE #{JSON.generate(http_method: outgoing.http_method, codec: codec, compression: encoding,
                                     query_payload_bytes: wire_size, http_body_bytes: outgoing.body.bytesize)}"
      puts "PASS GET Ruby->Go #{codec}/#{encoding} success/actual-verb/payload/metadata/trailers"
      error = GzipInterop.expect_error("invalid_argument") do
        client.call(EchoExample::ECHO, message, metadata: { "x-get-fail" => "yes" })
      end
      Interop.check(error.message == "deliberate" && error.metadata["x-error"] == %w[a b] &&
                    error.trailers["x-end"] == %w[c d], "GET error metadata/trailers")
      Interop.check(error.details.first.decode(binding: EchoExample::BINDING, type: EchoExample::Message) == message,
                    "GET error detail")
      Interop.check(observed.request_wire.http_method == "GET", "GET error method")
      puts "PASS GET Ruby->Go #{codec}/#{encoding} error/details/metadata/trailers"
    end
    observed = GzipInterop::ObservedTransport.new
    client = ConnectRPC::Client.new(base_url: url, binding: EchoExample::BINDING, transport: observed, use_get: true)
    GzipInterop.expect_error("invalid_argument") { client.call(EchoExample::FAIL, EchoExample::Message.new) }
    Interop.check(observed.request_wire.http_method == "POST", "Unsafe GET selected")
    puts "PASS GET Ruby->Go unsafe remains POST"
  end

  def self.run
    root = File.expand_path("..", __dir__)
    peer_dir = File.join(root, "interop")
    Dir.mktmpdir("connectrpc-get-peer-") do |temporary|
      binary = File.join(temporary, "peer")
      system("go", "mod", "verify", chdir: peer_dir, exception: true)
      system("go", "build", "-mod=readonly", "-o", binary, ".", chdir: peer_dir, exception: true)
      Open3.popen2(binary, "serve-gzip", chdir: peer_dir) do |input, output, process|
        raise "GET Go peer readiness timeout" unless output.wait_readable(10)

        url = output.gets&.strip
        Interop.check(url&.start_with?("http://127.0.0.1:"), "Invalid GET peer readiness")
        system(binary, "client-get", url, "Go", chdir: peer_dir, exception: true)
        ruby_clients(url)
      ensure
        input.close
        unless process.join(10)
          Process.kill("TERM", process.pid)
          process.join
          raise "GET Go peer shutdown timed out"
        end
        Interop.check(process.value.success?, "GET Go peer failed")
      end
      handlers = Interop.ruby_handlers
      echo = handlers.fetch(EchoExample::ECHO.path)
      failure = handlers.fetch(EchoExample::FAIL.path)
      handlers[EchoExample::ECHO.path] = lambda do |message, context|
        failure.call(message, context) if context.request_metadata["x-get-fail"] == ["yes"]
        echo.call(message, context)
      end
      server = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
                                      encoding: :gzip, handlers: handlers)
      rack = ConnectRPC::Rack.new(server: server)
      app = lambda do |env|
        status, headers, body = rack.call(env)
        headers["x-observed-http-method"] = [env.fetch("REQUEST_METHOD")]
        [status, headers, body]
      end
      host = EchoExample::Localhost.new(app)
      begin
        system(binary, "client-get", host.url, "Ruby", chdir: peer_dir, exception: true)
      ensure
        host.close
      end
    end
    puts "GET interop subset: 27 cases passed (9 Go->Go, 9 Ruby->Go, 9 Go->Ruby); " \
         "original 18 identity/generated and 40 gzip remain separate"
  end
end

GetInterop.run if $PROGRAM_NAME == __FILE__

# rubocop:enable Metrics/AbcSize, Metrics/MethodLength, Metrics/BlockLength, Metrics/CyclomaticComplexity
