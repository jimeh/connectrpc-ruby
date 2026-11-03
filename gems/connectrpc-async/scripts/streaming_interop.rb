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

helpers = Object.new.extend(AsyncHelpers)
binary = File.join(root, "vendor/async/connect-go-streaming-peer")
system("go", "mod", "verify", chdir: File.join(root, "interop"), exception: true)
system("go", "build", "-mod=readonly", "-o", binary, File.join(__dir__, "../interop/streaming.go"),
       chdir: File.join(root, "interop"), exception: true)
Dir.mktmpdir("streaming-interop-", File.join(root, "vendor/async")) do |fixtures|
  ca = helpers.credentials(1, "interop CA", authority: true)
  identities = { "server" => helpers.credentials(101, "localhost", issuer: ca, usage: "serverAuth"),
                 "client" => helpers.credentials(201, "client", issuer: ca, usage: "clientAuth") }
  File.write(File.join(fixtures, "ca.pem"), ca.first.to_pem)
  identities.each do |name, (certificate, key)|
    File.write(File.join(fixtures, "#{name}.pem"), certificate.to_pem)
    File.write(File.join(fixtures, "#{name}.key"), key.private_to_pem)
  end
  %w[h2c tls mtls].each do |mode|
    Open3.popen2(binary, "serve", mode, fixtures) do |input, output, process|
      raise "Go streaming readiness timeout" unless output.wait_readable(10)

      origin = output.gets.strip
      helpers.reactor do
        options = mode == "h2c" ? {} : { ca_file: File.join(fixtures, "ca.pem") }
        options.merge!(helpers.host_tls(identities.fetch("client")).except(:client_ca)) if mode == "mtls"
        transport = ConnectRPC::Async::Transport.new(origin: origin, **options)
        %i[proto json].product(%i[identity gzip]).each do |codec, coding|
          core = ConnectRPC::Client.new(base_url: origin, binding: StreamingInterop::BINDING,
                                        transport: transport, codec: codec, encoding: coding)
          StreamingInterop::ClientProbe.new(core).run
          puts "PASS Ruby->Go streaming #{mode}/#{codec}/#{coding} " \
               "SS/CS/bidi incremental/default/zero/error/bounds/cancel"
        end
      ensure
        transport&.close(grace: 0)
      end
    ensure
      input.close
      unless process.join(5)
        Process.kill("TERM", process.pid)
        process.join
        raise "Go streaming shutdown timeout"
      end
      raise "Go streaming peer failed" unless process.value.success?
    end
    helpers.reactor do |task|
      tls = helpers.host_tls(identities.fetch("server"), anchors: mode == "mtls" ? [ca] : nil) unless mode == "h2c"
      host = ConnectRPC::Async::Host.new(server: StreamingInterop.server, tls: tls).start
      origin = "#{mode == 'h2c' ? 'http' : 'https'}://127.0.0.1:#{host.port}"
      task.async do
        system(binary, "client", origin, mode, fixtures, exception: true)
      end.wait
    ensure
      host&.close(grace: 0)
    end
  end
end
