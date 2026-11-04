# frozen_string_literal: true

require "open3"
require "fileutils"
require "tmpdir"
require "connectrpc/falcon"
require "async/service/configuration"
require "async/container/forked"
require "rbconfig"
require "connectrpc/bindings/google_protobuf"

unless ARGV.delete("--profile")
  %w[identity gzip].each do |profile|
    system({ "CONNECTRPC_FALCON_GZIP" => profile == "gzip" ? "1" : nil }, RbConfig.ruby, __FILE__, "--profile", *ARGV,
           exception: true)
  end
  exit
end

root = File.expand_path("../../..", __dir__)
binary = File.join(root, "vendor/falcon/connect-go-peer")
FileUtils.mkdir_p(File.dirname(binary))
system("go", "mod", "verify", chdir: File.join(root, "interop"), exception: true)
peer_sources = %w[peer.go web_rejection.go].map { |name| File.join(__dir__, "../interop", name) }
system("go", "test", "-mod=readonly", "-v", *peer_sources, File.join(__dir__, "../interop/web_rejection_test.go"),
       chdir: File.join(root, "interop"), exception: true)
system("go", "build", "-mod=readonly", "-o", binary, *peer_sources,
       chdir: File.join(root, "interop"), exception: true)
$LOAD_PATH.unshift(File.join(root, "spec/fixtures/generation/echo"))
require "echo_connect"
require_relative "../../connectrpc-async/spec/support/helpers"
include AsyncHelpers

module FalconInterop
  class FalconInteropHost
    def initialize(server:, tls: nil)
      scheme = tls ? "https" : "http"
      @bound = ConnectRPC::Falcon::Endpoint.parse("#{scheme}://127.0.0.1:0").bound
      @server = ConnectRPC::Falcon::Server.new(server: server, endpoint: @bound, tls: tls)
    end

    def port = @bound.sockets.first.local_address.ip_port

    def start
      @server.run
      self
    end

    def close(**)
      @server.close(**)
    ensure
      @bound.close
    end
  end

  class FalconInteropService < ConnectRPC::Falcon::Service
    attr_reader :port

    def initialize(environment, events:)
      @events = events
      super(environment)
    end

    def make_listener(evaluator, endpoint, bound)
      @port = bound.sockets.first.local_address.ip_port
      super
    end

    def emit_running(instance, clock)
      super
      @events.write("#{Process.pid}\n")
    end
  end
end

# Keep worker setup, actual readiness and bounded process cleanup in one fixture.
# rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
def falcon_workers(tls:)
  core_factory = -> { server(request_limit: 1024, encoding: :gzip) }
  scheme = tls ? "https" : "http"
  configuration = Async::Service::Configuration.build do
    service "connectrpc-go-interop" do
      include ConnectRPC::Falcon::Environment

      url "#{scheme}://127.0.0.1:0"
      count 2
      health_check_timeout nil
      connectrpc_server { core_factory.call }
      tls tls
    end
  end
  reader, writer = IO.pipe
  writer.sync = true
  service = FalconInterop::FalconInteropService.new(configuration.environments.first, events: writer)
  service.start
  container = Async::Container::Forked.new
  service.setup(container)
  monitor = Async::Task.current.async { container.wait }
  pids = Async::Task.current.with_timeout(5) { 2.times.map { Integer(reader.gets) } }
  raise "Real Falcon workers missing" unless pids.uniq.size == 2 && !pids.include?(Process.pid)

  puts "Falcon #{scheme} parent-bound listener workers #{pids.join(', ')}"
  yield "#{scheme}://127.0.0.1:#{service.port}"
ensure
  container&.interrupt
  monitor&.wait
  service&.stop
  reader&.close
  writer&.close
end

# rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

Dir.mktmpdir("interop-", File.join(root, "vendor/falcon")) do |fixtures|
  ca = credentials(1, "interop CA", authority: true)
  server_identity = credentials(101, "localhost", issuer: ca, usage: "serverAuth")
  client_identity = credentials(201, "client", issuer: ca, usage: "clientAuth")
  File.write(File.join(fixtures, "ca.pem"), ca.first.to_pem)
  { "server" => server_identity, "client" => client_identity }.each do |name, identity|
    File.write(File.join(fixtures, "#{name}.pem"), identity.first.to_pem)
    File.write(File.join(fixtures, "#{name}.key"), identity.last.private_to_pem)
  end
  require_relative "web_rejection"
  FalconWebRejection.run(binary, fixtures, server_identity, root)
  next if ARGV.include?("--web-head-only")

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
          typed = client(transport, origin, codec: codec, encoding: ENV["CONNECTRPC_FALCON_GZIP"] ? :gzip : :identity)
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
      falcon_workers(tls: tls) do |origin|
        task.async do
          system(binary, "client", origin, "Falcon", fixtures, mode, chdir: File.join(root, "interop"), exception: true)
        end.wait
      end
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
        host = FalconInterop::FalconInteropHost.new(server: server(echo: handler),
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
unless ARGV.include?("--web-head-only")
  puts "Independent connect-go Falcon HTTP/2: both directions/codecs, h2c/TLS/mTLS " \
       "and four typed wire-tail drains passed"
end
