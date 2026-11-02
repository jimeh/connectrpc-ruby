# frozen_string_literal: true

require "connectrpc"
abort "Core loaded optional integrations" if defined?(ConnectRPC::Async) || defined?(Rack) || defined?(Rails)
require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "google/protobuf/wrappers_pb"
require "async/service/configuration"
require "async/container/forked"
require "tmpdir"
require "rbconfig"
require "stringio"

class ExampleService < ConnectRPC::Falcon::Service
  attr_reader :port

  def make_listener(evaluator, endpoint, bound)
    @port = bound.sockets.first.local_address.ip_port
    super
  end

  def emit_running(instance, clock)
    super
    EXAMPLE_EVENTS.write("#{Process.pid}\n")
  end
end

example_file = ARGV.fetch(0) { File.expand_path("../examples/falcon.rb", __dir__) }
configuration = Async::Service::Configuration.load([example_file])
# The normal DSL creates the environment. Only ephemeral caller settings differ.
environment = configuration.environments.fetch(0).with(url: "http://127.0.0.1:0", count: 2,
                                                       health_check_timeout: nil)
reader, writer = IO.pipe
writer.sync = true
Object.const_set(:EXAMPLE_EVENTS, writer)
service = ExampleService.new(environment)
Async do |task|
  service.start
  container = Async::Container::Forked.new
  service.setup(container)
  monitor = task.async { container.wait }
  task.with_timeout(10) do
    pids = 2.times.map { Integer(reader.gets) }
    raise "Expected distinct actual worker PIDs" unless pids.uniq.size == 2 && !pids.include?(Process.pid)

    origin = "http://127.0.0.1:#{service.port}"
    transport = ConnectRPC::Async::Transport.new(origin: origin)
    method = ConnectRPC::Method.new(service: "demo.EchoService", name: "Echo",
                                    request_type: Google::Protobuf::StringValue,
                                    response_type: Google::Protobuf::StringValue)
    %i[proto json].each do |codec|
      client = ConnectRPC::Client.new(base_url: origin, transport: transport, codec: codec,
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      response = client.call(method, Google::Protobuf::StringValue.new(value: "installed Falcon DSL"))
      raise "Installed DSL typed response" unless response.message.value == "installed Falcon DSL"
    end
    raw = IO::Endpoint.tcp("127.0.0.1", service.port).connect
    connection = Async::HTTP::Protocol::HTTP1.client(raw)
    response = connection.call(Protocol::HTTP::Request["GET", "/ordinary", {}, nil, authority: "localhost"])
    raise "Installed Rack fallback" unless response.status == 200 && response.read == "Ordinary Rack fallback\n"

    puts "PASS actual installed Falcon DSL/service: workers #{pids.join(', ')}, h2c proto/JSON and Rack HTTP/1"
  ensure
    transport&.close(grace: 0)
    raw&.close unless raw&.closed?
    connection&.close
  end
ensure
  container&.interrupt
  monitor&.wait
  service.stop
end.wait
reader.close
writer.close

# Core NetHTTP TLS/JSON continues working after Falcon activates the bounded
# OpenSSL/JSON graph. Trust remains scoped to this localhost caller process.
require "webrick"
require "webrick/https"
$LOAD_PATH.unshift(File.expand_path("generated", __dir__))
require "echo_connect"
require_relative "helpers"
include AsyncHelpers

abort "OpenSSL activation drift" unless Gem.loaded_specs.fetch("openssl").version.to_s == "4.0.2"
abort "JSON activation drift" unless Gem.loaded_specs.fetch("json").version.to_s == "3.1.0"
Dir.mktmpdir("falcon-installed-tls-") do |directory|
  ca = credentials(1, "installed CA", authority: true)
  leaf = credentials(101, "localhost", issuer: ca, usage: "serverAuth")
  ca_file = File.join(directory, "ca.pem")
  File.write(ca_file, ca.first.to_pem)
  ready = Thread::Queue.new
  web = WEBrick::HTTPServer.new(
    BindAddress: "127.0.0.1", Port: 0, SSLEnable: true,
    SSLCertificate: leaf.first, SSLPrivateKey: leaf.last,
    Logger: WEBrick::Log.new(File::NULL), AccessLog: [], StartCallback: -> { ready << true }
  )
  method = ConnectRPC::Method.new(service: "demo.EchoService", name: "Echo",
                                  request_type: Google::Protobuf::StringValue,
                                  response_type: Google::Protobuf::StringValue)
  definition = ConnectRPC::Service.new(name: "demo.EchoService", methods: [method])
  implementation = ConnectRPC::Server.new(services: [definition], encoding: :gzip,
                                          binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                          handlers: { method.path => ->(message, _context) { message } })
  web.mount_proc("/") do |request, response|
    output = implementation.call(path: request.path, http_method: request.request_method,
                                 headers: request.header, input: StringIO.new(request.body || ""))
    response.status = output.status
    output.headers.each { |key, values| response[key] = values.join(", ") }
    response.body = output.body
  end
  worker = Thread.new { web.start }
  ready.pop
  code = <<~'CODE'
    require "connectrpc/falcon"
    require "connectrpc/bindings/google_protobuf"
    require "google/protobuf/wrappers_pb"
    method = ConnectRPC::Method.new(service: "demo.EchoService", name: "Echo",
      request_type: Google::Protobuf::StringValue, response_type: Google::Protobuf::StringValue)
    %i[identity gzip].each do |encoding|
      client = ConnectRPC::Client.new(base_url: ARGV.fetch(0), codec: :json, encoding: encoding,
        binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      response = client.call(method, Google::Protobuf::StringValue.new(value: "core TLS JSON"))
      abort "Core TLS JSON regression" unless response.message.value == "core TLS JSON"
      puts "PASS activated Falcon stack, core NetHTTP TLS/JSON/#{encoding}"
    end
  CODE
  system({ "SSL_CERT_FILE" => ca_file }, RbConfig.ruby, "-e", code,
         "https://localhost:#{web.config.fetch(:Port)}", exception: true)
ensure
  web&.shutdown
  worker&.join
end
if ENV["CONNECTRPC_FALCON_INSTALLED"]
  Gem.loaded_specs.each_value do |specification|
    next if specification.default_gem? || specification.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")

    abort "Installed load path escaped isolation: #{specification.name}"
  end
end
puts "Falcon caller integration passed (Ruby #{RUBY_VERSION})"
