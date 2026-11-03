# frozen_string_literal: true

require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "open3"
require "tmpdir"
require "json"
root = File.expand_path("../../..", __dir__)
$LOAD_PATH.unshift(File.join(root, "spec/fixtures/generation/echo"))
require "echo_connect"
require_relative "../../connectrpc-async/spec/support/helpers"
require_relative "../../connectrpc-async/scripts/streaming_fixture"
require_relative "../spec/support/process_cluster"

helpers = Object.new.extend(AsyncHelpers)
binary = File.join(root, "vendor/falcon/connect-go-streaming-peer")
system("go", "mod", "verify", chdir: File.join(root, "interop"),
                              exception: true)
system("go", "build", "-mod=readonly", "-o", binary,
       File.join(root, "gems/connectrpc-async/interop/streaming.go"), chdir: File.join(root, "interop"),
                                                                      exception: true)
Dir.mktmpdir("streaming-interop-", File.join(root, "vendor/falcon")) do |fixtures|
  ca = helpers.credentials(1, "streaming CA", authority: true)
  identities = { "server" => helpers.credentials(101, "localhost", issuer: ca, usage: "serverAuth"),
                 "client" => helpers.credentials(201, "client", issuer: ca, usage: "clientAuth") }
  File.write(File.join(fixtures, "ca.pem"), ca.first.to_pem)
  identities.each do |name, (certificate, key)|
    File.write(File.join(fixtures, "#{name}.pem"), certificate.to_pem)
    File.write(File.join(fixtures, "#{name}.key"), key.private_to_pem)
  end
  %w[h2c tls mtls].each do |mode|
    helpers.reactor do |task|
      tls = helpers.host_tls(identities.fetch("server"), anchors: mode == "mtls" ? [ca] : nil) unless mode == "h2c"
      reader, writer = IO.pipe
      writer = FalconProcessFixtures::FalconEventWriter.new(writer)
      writer.sync = true
      scheme = mode == "h2c" ? "http" : "https"
      configuration = Async::Service::Configuration.build do
        service "streaming-interop" do
          include ConnectRPC::Falcon::Environment
          include FalconProcessFixtures::FalconWorkerObservation

          url "#{scheme}://127.0.0.1:0"
          count 2
          health_check_timeout nil
          events writer
          connectrpc_server { StreamingInterop.server }
          tls tls
        end
      end
      cluster = FalconProcessFixtures::FalconProcessCluster.new(configuration, reader, writer)
      cluster.event(:running, count: 2)
      pids = cluster.events.select { |event| event.fetch("kind") == "running" }.map { |event| event.fetch("pid") }
      raise "Real Falcon streaming workers missing" unless pids.uniq.size == 2 && !pids.include?(Process.pid)

      origin = "#{scheme}://127.0.0.1:#{cluster.service.port}"
      task.async { system(binary, "client", origin, mode, fixtures, exception: true) }.wait
      cluster.stop
      cluster.event(:closed, count: 2)
      closed = cluster.events.select { |event| event.fetch("kind") == "closed" }
      unless closed.all? { |event| event.values_at("requests", "connections") == [0, 0] }
        raise "Streaming worker leaked owned requests/connections: #{closed.inspect}"
      end

      puts "PASS Falcon real streaming workers #{mode}: PIDs #{pids.join(', ')}; " \
           "closed with zero active requests/connections"
    ensure
      cluster&.close
    end
  end
end
