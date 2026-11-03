# frozen_string_literal: true

require "open3"
require "tmpdir"
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
root = File.expand_path("..", __dir__)
$LOAD_PATH.unshift(File.join(root, "interop/grpc/ruby"), File.join(root, "spec/fixtures/generation/echo"))
require "peer_connect"
require "echo_connect"
require_relative "../gems/connectrpc-async/spec/support/helpers"
host_kind = ARGV.fetch(0, "async")
raise "Expected async or falcon" unless %w[async falcon].include?(host_kind)

module GRPCInterop
  PEER = Connectrpc::Grpcinterop::V1::Peer
  MESSAGE = Connectrpc::Grpcinterop::V1::Message
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new

  def self.server # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    ConnectRPC::Server.new(services: [PEER::SERVICE], binding: BINDING, protocol: :grpc, encoding: :gzip,
                           handlers: PEER.handlers(
                             unary: lambda { |request, context|
                               context.response_metadata["x-head"] = ["ready"]
                               context.response_trailers["x-tail"] = ["done"]
                               request
                             },
                             server_stream: lambda { |request, writer, _context|
                               writer.send_headers(metadata: { "x-head" => ["ready"] })
                               [request.value, "second", ""].each do |value|
                                 writer.send_message(MESSAGE.new(value: value, payload: request.payload))
                               end
                             },
                             client_stream: lambda { |reader, _context|
                               values = []
                               reader.each { |message| values << message.value } # rubocop:disable Style/MapIntoArray -- RequestReader has block-only each.
                               MESSAGE.new(value: values.join(","))
                             },
                             bidi: ->(duplex, _context) { duplex.each { |message| duplex.send_message(message) } }
                           ))
  end

  def self.probe(client) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    reply = client.unary(MESSAGE.new(value: "unary", payload: "\0\xFF".b))
    raise "Unary message/metadata" unless reply.message.value == "unary" && reply.message.payload == "\0\xFF".b &&
                                          reply.metadata["x-head"] == ["ready"] && reply.trailers["x-tail"] == ["done"]

    values = []
    client.server_stream(MESSAGE.new(value: "first")) do |reader|
      raise "Early headers" unless reader.headers["x-head"] == ["ready"]

      reader.each { |message| values << message.value }
    end
    raise "SS messages" unless values == ["first", "second", ""]

    reply = client.client_stream do |writer|
      %w[a b].each { |value| writer.send_message(MESSAGE.new(value: value)) }
    end
    raise "CS reply" unless reply.message.value == "a,b"

    client.bidi do |duplex|
      duplex.send_message(MESSAGE.new(value: "chat"))
      raise "Incremental bidi" unless duplex.receive.value == "chat"

      duplex.close_send
      raise "Bidi EOF" unless duplex.receive.nil?
    end
  end
end

helpers = Object.new.extend(AsyncHelpers)
binary = File.join(root, "vendor/grpc/peer")
system("go", "mod", "verify", chdir: File.join(root, "interop/grpc"), exception: true)
system("go", "build", "-mod=readonly", "-o", binary, ".", chdir: File.join(root, "interop/grpc"), exception: true)
Dir.mktmpdir("grpc-interop-", File.join(root, "vendor/grpc")) do |fixtures| # rubocop:disable Metrics/BlockLength
  ca = helpers.credentials(1, "grpc peer CA", authority: true)
  identities = { "server" => helpers.credentials(101, "localhost", issuer: ca, usage: "serverAuth"),
                 "client" => helpers.credentials(201, "client", issuer: ca, usage: "clientAuth") }
  File.write(File.join(fixtures, "ca.pem"), ca.first.to_pem)
  identities.each do |name, (certificate, key)|
    File.write(File.join(fixtures, "#{name}.pem"), certificate.to_pem)
    File.write(File.join(fixtures, "#{name}.key"), key.private_to_pem)
  end
  untrusted = helpers.credentials(999, "untrusted client", usage: "clientAuth")
  File.write(File.join(fixtures, "untrusted-client.pem"), untrusted.first.to_pem)
  File.write(File.join(fixtures, "untrusted-client.key"), untrusted.last.private_to_pem)
  %w[h2c tls mtls].each do |mode| # rubocop:disable Metrics/BlockLength
    if host_kind == "async"
      system(binary, "control", mode, fixtures, exception: true)
      Open3.popen2(binary, "serve", mode, fixtures) do |input, output, process|
        raise "grpc-go readiness timeout" unless output.wait_readable(10)

        origin = output.gets.strip
        helpers.reactor do
          options = mode == "h2c" ? {} : { ca_file: File.join(fixtures, "ca.pem") }
          options.merge!(helpers.host_tls(identities.fetch("client")).except(:client_ca)) if mode == "mtls"
          transport = ConnectRPC::Async::Transport.new(origin: origin, **options)
          %i[identity gzip].each do |coding|
            core = ConnectRPC::Client.new(base_url: origin, binding: GRPCInterop::BINDING, transport: transport,
                                          protocol: :grpc, encoding: coding)
            GRPCInterop.probe(GRPCInterop::PEER::Client.new(client: core))
            puts "PASS Ruby->grpc-go #{mode}/#{coding} unary/server-stream/client-stream/bidi metadata/halfclose"
          end
        ensure
          transport&.close(grace: 0)
        end
      ensure
        input.close
        unless process.join(5)
          Process.kill("TERM", process.pid)
          process.join
          raise "grpc-go shutdown timeout"
        end
        raise "grpc-go failed" unless process.value.success?
      end
    end
    helpers.reactor do |task| # rubocop:disable Metrics/BlockLength
      tls = helpers.host_tls(identities.fetch("server"), anchors: mode == "mtls" ? [ca] : nil) unless mode == "h2c"
      scheme = mode == "h2c" ? "http" : "https"
      if host_kind == "falcon"
        require "connectrpc/falcon"
        require_relative "../gems/connectrpc-falcon/spec/support/process_cluster"
        reader, writer = IO.pipe
        writer = FalconProcessFixtures::FalconEventWriter.new(writer)
        writer.sync = true
        configuration = Async::Service::Configuration.build do
          service "grpc-interop" do
            include ConnectRPC::Falcon::Environment
            include FalconProcessFixtures::FalconWorkerObservation

            url "#{scheme}://127.0.0.1:0"
            count 2
            health_check_timeout nil
            events writer
            connectrpc_server { GRPCInterop.server }
            tls tls
          end
        end
        host = FalconProcessFixtures::FalconProcessCluster.new(configuration, reader, writer)
        host.event(:running, count: 2)
        pids = host.events.select { |event| event.fetch("kind") == "running" }.map { |event| event.fetch("pid") }
        raise "Real Falcon workers missing" unless pids.uniq.size == 2 && !pids.include?(Process.pid)

        port = host.service.port
      else
        host = ConnectRPC::Async::Host.new(server: GRPCInterop.server, tls: tls).start
        port = host.port
      end
      origin = "#{scheme}://127.0.0.1:#{port}"
      task.async { system(binary, "client", origin, mode, fixtures, exception: true) }.wait
      negatives = case mode
                  when "tls" then %w[untrusted-server hostname]
                  when "mtls" then %w[missing-client untrusted-client]
                  else []
                  end
      negatives.each do |scenario|
        task.async { system(binary, "negative", origin, mode, fixtures, scenario, exception: true) }.wait
      end
      if host_kind == "falcon"
        host.stop
        host.event(:closed, count: 2)
        closed = host.events.select { |event| event.fetch("kind") == "closed" }
        raise "Falcon leaked owned requests/connections" unless closed.all? do |event|
          event.values_at("requests", "connections") == [0, 0]
        end
      end
      puts "PASS grpc-go->#{host_kind} #{mode}/identity+gzip all four shapes#{" workers #{pids.join(',')}" if pids}"
    ensure
      host_kind == "falcon" ? host&.close : host&.close(grace: 0)
    end
  end
end
