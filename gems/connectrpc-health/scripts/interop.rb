# frozen_string_literal: true

# grpc-go's standard grpc_health_v1 client against the Async Host and real
# Falcon workers over h2c, TLS and mTLS, and against grpc-go's reference
# health server with the same script.
require "open3"
require "tmpdir"
require "connectrpc/health"
require "connectrpc/async"
require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
root = File.expand_path("../../..", __dir__)
$LOAD_PATH.unshift(File.join(root, "interop/grpc/ruby"), File.join(root, "spec/fixtures/generation/echo"))
require "peer_connect"
require "echo_connect"
require_relative "../../connectrpc-async/spec/support/helpers"
require_relative "../../connectrpc-falcon/spec/support/process_cluster"

module HealthInterop
  PEER = Connectrpc::Grpcinterop::V1::Peer
  MESSAGE = Connectrpc::Grpcinterop::V1::Message
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new
  STATUSES = { "UNKNOWN" => :unknown, "SERVING" => :serving, "NOT_SERVING" => :not_serving }.freeze

  # Each Falcon worker builds its own server and registry. The Peer Unary
  # control therefore changes the registry owning the client's connection.
  def self.server
    health = ConnectRPC::Health::Registry.new
    health.set(PEER::SERVICE.name, :serving)
    unused = ->(*) { raise ConnectRPC::Error.new(:unimplemented, "not used by health interop") }
    handlers = PEER.handlers(unary: ->(request, _) { control(health, request) }, server_stream: unused,
                             client_stream: unused, bidi: unused)
    ConnectRPC::Server.new(services: [PEER::SERVICE, ConnectRPC::Health::SERVICE], binding: BINDING,
                           protocol: :grpc, handlers: handlers.merge(health.handlers))
  end

  def self.control(health, request)
    case request.value
    when /\Ahealth:set:([^:]*):([A-Z_]+)\z/
      health.set(Regexp.last_match(1), STATUSES.fetch(Regexp.last_match(2)))
    when "health:watchers" then return MESSAGE.new(value: health.watcher_count.to_s)
    when "health:shutdown" then health.shutdown
    else raise ConnectRPC::Error.new(:invalid_argument, "unknown health control")
    end
    request
  end

  def self.falcon(scheme, tls)
    reader, writer = IO.pipe
    writer = FalconProcessFixtures::FalconEventWriter.new(writer)
    writer.sync = true
    configuration = Async::Service::Configuration.build do
      service "health-interop" do
        include ConnectRPC::Falcon::Environment
        include FalconProcessFixtures::FalconWorkerObservation

        url "#{scheme}://127.0.0.1:0"
        count 2
        health_check_timeout nil
        events writer
        connectrpc_server { HealthInterop.server }
        tls tls
      end
    end
    host = FalconProcessFixtures::FalconProcessCluster.new(configuration, reader, writer)
    begin
      host.event(:running, count: 2)
      pids = host.events.select { |event| event.fetch("kind") == "running" }.map { |event| event.fetch("pid") }
      raise "Real Falcon workers missing" unless pids.uniq.size == 2 && !pids.include?(Process.pid)
    rescue StandardError => e
      # The caller never receives this host, so close it here; keep the startup error.
      begin
        host.close
      rescue StandardError
        nil
      end
      raise e
    end
    [host, pids]
  end
end

helpers = Object.new.extend(AsyncHelpers)
binary = File.join(root, "vendor/grpc/health-peer")
interop = File.join(root, "interop/grpc")
system("go", "mod", "verify", chdir: interop, exception: true)
system("go", "build", "-mod=readonly", "-o", binary, ".", chdir: interop, exception: true)
Dir.mktmpdir("health-interop-", File.join(root, "vendor/grpc")) do |fixtures|
  ca = helpers.credentials(1, "health peer CA", authority: true)
  identities = { "server" => helpers.credentials(101, "localhost", issuer: ca, usage: "serverAuth"),
                 "client" => helpers.credentials(201, "client", issuer: ca, usage: "clientAuth") }
  File.write(File.join(fixtures, "ca.pem"), ca.first.to_pem)
  identities.each do |name, (certificate, key)|
    File.write(File.join(fixtures, "#{name}.pem"), certificate.to_pem)
    File.write(File.join(fixtures, "#{name}.key"), key.private_to_pem)
  end
  %w[h2c tls mtls].each do |mode|
    system(binary, "health-control", mode, fixtures, exception: true)
    %w[async falcon].each do |host_kind|
      helpers.reactor do |task|
        tls = helpers.host_tls(identities.fetch("server"), anchors: mode == "mtls" ? [ca] : nil) unless mode == "h2c"
        scheme = mode == "h2c" ? "http" : "https"
        if host_kind == "falcon"
          host, pids = HealthInterop.falcon(scheme, tls)
          port = host.service.port
        else
          host = ConnectRPC::Async::Host.new(server: HealthInterop.server, tls: tls).start
          port = host.port
        end
        origin = "#{scheme}://127.0.0.1:#{port}"
        task.async { system(binary, "health-client", origin, mode, fixtures, exception: true) }.wait
        if host_kind == "falcon"
          host.stop
          host.event(:closed, count: 2)
          closed = host.events.select { |event| event.fetch("kind") == "closed" }
          raise "Falcon leaked owned requests/connections" unless closed.all? do |event|
            event.values_at("requests", "connections") == [0, 0]
          end
        else
          raise "Async host did not drain after health shutdown" unless host.close(grace: 5)
        end
        puts "PASS grpc-go health client -> #{host_kind} #{mode}#{" workers #{pids.join(',')}" if pids}"
      ensure
        host_kind == "falcon" ? host&.close : host&.close(grace: 0)
      end
    end
  end
end
