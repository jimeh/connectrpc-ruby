# frozen_string_literal: true

require "open3"
require "fileutils"
require "tmpdir"
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"

root = File.expand_path("../../..", __dir__)
binary = File.join(root, "vendor/async/connect-go-peer")
FileUtils.mkdir_p(File.dirname(binary))
system("go", "mod", "verify", chdir: File.join(root, "interop"), exception: true)
system("go", "build", "-mod=readonly", "-o", binary, File.join(__dir__, "../interop/peer.go"),
       chdir: File.join(root, "interop"), exception: true)
$LOAD_PATH.unshift(File.join(root, "spec/fixtures/generation/echo"))
require "echo_connect"
require_relative "../spec/support/helpers"
include AsyncHelpers

Dir.mktmpdir("interop-", File.join(root, "vendor/async")) do |fixtures|
  ca = credentials(1, "interop CA", authority: true)
  server_identity = credentials(101, "localhost", issuer: ca, usage: "serverAuth")
  client_identity = credentials(201, "client", issuer: ca, usage: "clientAuth")
  File.write(File.join(fixtures, "ca.pem"), ca.first.to_pem)
  { "server" => server_identity, "client" => client_identity }.each do |name, identity|
    File.write(File.join(fixtures, "#{name}.pem"), identity.first.to_pem)
    File.write(File.join(fixtures, "#{name}.key"), identity.last.private_to_pem)
  end
  %w[h2c tls mtls].each do |mode|
    Open3.popen2(binary, "serve", mode, fixtures, chdir: File.join(root, "interop")) do |input, output, process|
      raise "Go peer readiness timeout" unless output.wait_readable(10)

      origin = output.gets&.strip
      raise "Invalid Go peer origin" unless origin&.match?(%r{\Ahttps?://127\.0\.0\.1:\d+\z})

      reactor do
        options = { origin: origin }
        options[:ca_file] = File.join(fixtures, "ca.pem") unless mode == "h2c"
        options.merge!(host_tls(client_identity).except(:client_ca)) if mode == "mtls"
        transport = ConnectRPC::Async::Transport.new(**options)
        %i[proto json].each do |codec|
          typed = client(transport, origin, codec: codec)
          message = AsyncHelpers::MESSAGE.new(name: "", count: 9_007_199_254_740_993, mood: :HAPPY,
                                              at: Google::Protobuf::Timestamp.new(seconds: 1_704_067_200),
                                              data: "\x00\xff".b)
          response = typed.echo(message, metadata: { "x-request" => %w[a b] })
          raise "Typed #{mode}/#{codec} response" unless response.message == message && response.message.has_name?
          raise "Repeated request metadata" unless response.metadata["x-echo"] == %w[a b]
          raise "Repeated response metadata" unless response.metadata["x-repeat"] == %w[one two]
          raise "Leading trailer metadata" unless response.trailers["x-tail"] == %w[three four]

          begin
            typed.fail(message)
            raise "Expected typed error"
          rescue ConnectRPC::Error => e
            raise "Typed error" unless e.code == "invalid_argument" && e.message == "deliberate"
            raise "Typed detail" unless AsyncHelpers::MESSAGE.decode(e.details.first.value) == message
            raise "Error metadata" unless e.metadata["x-error"] == %w[a b] && e.trailers["x-end"] == %w[c d]
          end
          begin
            typed.echo(AsyncHelpers::MESSAGE.new(name: "x" * 2048))
            raise "Expected request limit"
          rescue ConnectRPC::Error => e
            raise "Request limit" unless e.code == "resource_exhausted"
          end
          puts "PASS generated Ruby->Go #{mode} #{codec} success/presence/types/errors/details/metadata/trailers/limit"
        end
      ensure
        transport&.close
      end
    ensure
      input.close
      unless process.join(10)
        Process.kill("TERM", process.pid)
        process.join
        raise "Go server shutdown timeout"
      end
      raise "Go server failed" unless process.value.success?
    end

    reactor do |task|
      tls = host_tls(server_identity, anchors: mode == "mtls" ? [ca] : nil) unless mode == "h2c"
      host = ConnectRPC::Async::Host.new(server: server(request_limit: 1024), tls: tls).start
      origin = "#{mode == 'h2c' ? 'http' : 'https'}://127.0.0.1:#{host.port}"
      peer = task.async do
        system(binary, "client", origin, "Async", fixtures, mode, chdir: File.join(root, "interop"), exception: true)
      end
      peer.wait
    ensure
      host&.close(grace: 0)
    end
  end
  %w[h2c tls].each do |mode|
    %w[proto json].each do |codec|
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        handler = lambda do |message, _|
          entered.enqueue(true)
          release.dequeue
          message
        end
        host = ConnectRPC::Async::Host.new(server: server(echo: handler),
                                           tls: mode == "tls" ? host_tls(server_identity) : nil).start
        origin = "#{mode == 'tls' ? 'https' : 'http'}://localhost:#{host.port}"
        peer = task.async do
          system(binary, "tail", origin, codec, fixtures, chdir: File.join(root, "interop"), exception: true)
        end
        entered.dequeue
        closing = task.async { host.close(grace: 3) }
        release.enqueue(true)
        peer.wait
        raise "Graceful wire tail failed #{mode}/#{codec}" unless closing.wait

        puts "PASS host #{mode} #{codec} wire drain, clean GOAWAY and bounded half-close/EOF"
      ensure
        release&.enqueue(true)
        host&.close(grace: 0)
      end
    end
  end
end
puts "Independent connect-go HTTP/2: both directions/codecs, h2c/TLS/mTLS and four typed wire-tail drains passed"
