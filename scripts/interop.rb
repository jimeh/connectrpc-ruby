# frozen_string_literal: true

require "open3"
require "rbconfig"
require "tmpdir"
require_relative "../examples/localhost"

module Interop # rubocop:disable Metrics/ModuleLength
  def self.check(condition, message)
    raise message unless condition
  end

  def self.ruby_clients(url)
    %i[proto json].each do |codec|
      client = ConnectRPC::Client.new(base_url: url, binding: EchoExample::BINDING, codec: codec)
      message = EchoExample::Message.new(name: "", count: 9_007_199_254_740_993, mood: :HAPPY,
                                         at: Google::Protobuf::Timestamp.new(seconds: 1_704_067_200),
                                         data: "\x00\xff".b)
      response = client.call(EchoExample::ECHO, message, metadata: { "x-request" => %w[a b] }, timeout: 2)
      check(response.message == message, "Ruby->Go #{codec} message mismatch")
      check(response.message.has_name?, "Ruby->Go #{codec} optional presence lost")
      check(response.metadata["x-repeat"] == %w[one two], "Go raw repeated headers lost")
      check(response.metadata["x-echo"] == ["a, b"], "NetHTTP request coalescing changed")
      check(response.trailers["x-tail"] == %w[three four], "Go raw repeated trailers lost")
      puts "PASS Ruby->Go #{codec} success/presence/int64/enum/WKT/bytes/metadata/trailers"
      begin
        client.call(EchoExample::FAIL, message, timeout: 2)
        raise "Expected Go error"
      rescue ConnectRPC::Error => e
        check(e.code == "invalid_argument" && e.message == "deliberate", "Go code/message mismatch")
        check(e.details.size == 1, "Go detail count mismatch")
        check(e.details.first.decode(binding: EchoExample::BINDING, type: EchoExample::Message) == message,
              "Go detail mismatch")
        check(e.metadata["x-error"] == %w[a b], "Go error leading metadata mismatch")
        check(e.trailers["x-end"] == %w[c d], "Go error trailing metadata mismatch")
      end
      puts "PASS Ruby->Go #{codec} deliberate error/detail/metadata/trailers"
      begin
        client.call(EchoExample::ECHO, EchoExample::Message.new(name: "x" * 2048), timeout: 2)
        raise "Expected Go request limit"
      rescue ConnectRPC::Error => e
        check(e.code == "resource_exhausted", "Go request limit mismatch")
      end
      puts "PASS Ruby->Go #{codec} request limit"
    end
  end

  def self.ruby_handlers
    handler = lambda do |request, context|
      context.response_metadata["x-echo"] = context.request_metadata["x-request"] || []
      context.response_metadata["x-repeat"] = %w[one two]
      context.response_trailers["x-tail"] = %w[three four]
      request
    end
    failure = lambda do |request, _context|
      detail = ConnectRPC::ErrorDetail.new(type: "example.v1.EchoMessage", value: request.to_proto)
      raise ConnectRPC::Error.new(:invalid_argument, "deliberate", details: [detail],
                                                                   metadata: { "x-error" => %w[a b] },
                                                                   trailers: { "x-end" => %w[c d] })
    end
    { EchoExample::ECHO.path => handler, EchoExample::FAIL.path => failure }
  end

  def self.ruby_server
    ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING, request_limit: 1024,
                           handlers: ruby_handlers)
  end

  # Keep peer startup, observable readiness and cleanup in the same scope.
  # rubocop:disable-next Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def self.run(profile: "identity")
    require_relative "../examples/echo_schema"

    serve_mode, client_mode, ruby_peer = if profile == "gzip"
                                           ["serve-gzip", "client-gzip", GzipInterop]
                                         else
                                           ["serve", "client", Interop]
                                         end

    version, status = Open3.capture2("go", "version")
    check(status.success? && version.start_with?("go version go1.27.3 "),
          "Interop requires pinned Go 1.27.3: #{version}")
    puts version
    root = File.expand_path("..", __dir__)
    peer_dir = File.join(root, "interop")
    Dir.mktmpdir("connectrpc-interop-") do |temporary|
      binary = File.join(temporary, "peer")
      system("go", "mod", "verify", chdir: peer_dir, exception: true)
      system("go", "build", "-mod=readonly", "-o", binary, ".", chdir: peer_dir, exception: true)
      Open3.popen2(binary, serve_mode, chdir: peer_dir) do |input, output, process|
        raise "Go peer did not announce readiness" unless output.wait_readable(10)

        url = output.gets&.strip
        check(url&.start_with?("http://127.0.0.1:"), "Invalid Go peer readiness: #{url.inspect}")
        system(binary, client_mode, url, "Go", chdir: peer_dir, exception: true)
        ruby_peer.ruby_clients(url)
      ensure
        input.close
        unless process.join(10)
          Process.kill("TERM", process.pid)
          process.join
          raise "Go peer shutdown timed out"
        end
        check(process.value.success?, "Go server failed")
      end
      if profile == "identity"
        system(RbConfig.ruby, File.join(root, "scripts/generated_interop.rb"), binary, chdir: peer_dir, exception: true)
      end
      host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: ruby_peer.ruby_server))
      begin
        system(binary, client_mode, host.url, "Ruby", chdir: peer_dir, exception: true)
      ensure
        host.close
      end
    end
    if profile == "gzip"
      return puts "Gzip interop subset: 40 cases passed " \
                  "(14 Go->Go, 12 Ruby->Go, 14 Go->Ruby, including 4 raw asymmetric probes)"
    end

    puts "Unary interop subset: 18 cases passed (6 Go->Go baseline, 12 cross-language); no full conformance claim"
  end
end

if $PROGRAM_NAME == __FILE__
  require_relative "interop_gzip"
  profiles = ARGV.empty? ? %w[identity gzip get] : ARGV
  raise "Usage: interop.rb [identity|gzip|get]" unless profiles.all? { |entry| %w[identity gzip get].include?(entry) }

  profiles.each do |profile|
    if profile == "get"
      require_relative "interop_get"
      GetInterop.run
    else
      Interop.run(profile: profile)
    end
  end
end
