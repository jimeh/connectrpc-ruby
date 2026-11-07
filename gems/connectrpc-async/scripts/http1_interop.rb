# frozen_string_literal: true

require "open3"
require "fileutils"
require "tmpdir"
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
require "google/protobuf/wrappers_pb"

root = File.expand_path("../../..", __dir__)
$LOAD_PATH.unshift(File.join(root, "spec/fixtures/generation/echo"))
require "echo_connect"
require_relative "../spec/support/helpers"
require_relative "streaming_fixture"
require_relative "http1_fixture"

helpers = Object.new.extend(AsyncHelpers)
binary = File.join(root, "vendor/async/connect-go-http1-client-peer")
system("go", "mod", "verify", chdir: File.join(root, "interop"), exception: true)
system("go", "build", "-mod=readonly", "-o", binary, File.join(__dir__, "../interop/streaming.go"),
       chdir: File.join(root, "interop"), exception: true)
Dir.mktmpdir("http1-client-interop-", File.join(root, "vendor/async")) do |fixtures|
  ca = helpers.credentials(1, "interop CA", authority: true)
  identities = { "server" => helpers.credentials(101, "localhost", issuer: ca, usage: "serverAuth"),
                 "client" => helpers.credentials(201, "client", issuer: ca, usage: "clientAuth") }
  File.write(File.join(fixtures, "ca.pem"), ca.first.to_pem)
  identities.each do |name, (certificate, key)|
    File.write(File.join(fixtures, "#{name}.pem"), certificate.to_pem)
    File.write(File.join(fixtures, "#{name}.key"), key.private_to_pem)
  end
  %w[h1 h1tls h1mtls].each do |mode|
    Open3.popen2(binary, "serve", mode, fixtures) do |input, output, process|
      raise "Go HTTP1 readiness timeout" unless output.wait_readable(10)

      origin = output.gets.strip
      helpers.reactor do |task|
        options = mode == "h1" ? {} : { ca_file: File.join(fixtures, "ca.pem") }
        options.merge!(helpers.host_tls(identities.fetch("client")).except(:client_ca)) if mode == "h1mtls"
        transport = ConnectRPC::Async::HTTP1Transport.new(origin: origin, **options)
        %i[proto json].product(%i[identity gzip]).each do |codec, coding|
          core = ConnectRPC::Client.new(base_url: origin, binding: StreamingInterop::BINDING,
                                        transport: transport, codec: codec, encoding: coding)
          StreamingInterop::HTTP1ClientProbe.new(core, task: task, tls: mode != "h1").run
          puts "PASS Ruby->Go H1 client #{mode}/#{codec}/#{coding}: unary/SS/CS incremental/error/cancel " \
               "observed HTTP/1.1 ALPN=#{mode == 'h1' ? 'nil' : 'http/1.1'}"
        end
        raise "H1 transport did not drain" unless transport.close
      ensure
        transport&.close(grace: 0)
      end
    ensure
      input.close
      unless process.join(5)
        Process.kill("TERM", process.pid)
        process.join
        raise "Go HTTP1 shutdown timeout"
      end
      raise "Go HTTP1 peer failed" unless process.value.success?
    end
  end
end
