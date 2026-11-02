# frozen_string_literal: true

# Copied, with generated fixtures and helper data, to an outside-checkout cwd.
require "connectrpc"
abort "Core loaded optional hosting" if defined?(ConnectRPC::Async) || defined?(Rails) || defined?(Rack)
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
require "tmpdir"
require "rbconfig"
require "stringio"
$LOAD_PATH.unshift(File.expand_path("generated", __dir__))
require "echo_connect"
require_relative "helpers"
include AsyncHelpers

abort "Unexpected activated OpenSSL" unless Gem.loaded_specs.fetch("openssl").version.to_s == "4.0.2"
abort "Unexpected activated JSON" unless Gem.loaded_specs.fetch("json").version.to_s == "3.1.0"
puts "Activated OpenSSL #{OpenSSL::VERSION} (#{OpenSSL::OPENSSL_LIBRARY_VERSION}), JSON #{JSON::VERSION}"
Dir.mktmpdir("integration-", Dir.pwd) do |directory|
  ca = credentials(1, "installed CA", authority: true)
  leaf = credentials(101, "localhost", issuer: ca, usage: "serverAuth")
  identity = credentials(201, "installed client", issuer: ca, usage: "clientAuth")
  ca_file = File.join(directory, "ca.pem")
  File.write(ca_file, ca.first.to_pem)
  reactor do
    [false, true].each do |tls|
      host = ConnectRPC::Async::Host.new(server: server, tls: tls ? host_tls(leaf, anchors: [ca]) : nil).start
      origin = "#{tls ? 'https' : 'http'}://localhost:#{host.port}"
      options = { origin: origin }
      options.merge!(ca_file: ca_file, **host_tls(identity).except(:client_ca)) if tls
      transport = ConnectRPC::Async::Transport.new(**options)
      %i[proto json].each do |codec|
        typed = client(transport, origin, codec: codec)
        message = AsyncHelpers::MESSAGE.new(name: "", count: 9_007_199_254_740_993, data: "\x00\xff".b)
        response = typed.echo(message, metadata: { "x-request" => %w[a b] })
        unless response.message == message && response.message.has_name? &&
               response.metadata["x-echo"] == %w[a b] && response.trailers["x-tail"] == %w[three four]
          raise "Installed typed/presence/metadata"
        end

        begin
          typed.fail(message)
          raise "Expected installed error"
        rescue ConnectRPC::Error => e
          raise "Installed typed error" unless e.code == "invalid_argument" &&
                                               AsyncHelpers::MESSAGE.decode(e.details.first.value) == message
        end
        puts "PASS installed generated #{tls ? 'mTLS h2' : 'h2c'} #{codec} success/error/details/metadata"
      end
    ensure
      transport&.close
      host&.close
    end
  end

  # Exercise the unchanged core NetHTTP transport after this graph activates
  # OpenSSL 4.0.2 and JSON 3.1.0, with trust scoped to this child process.
  require "webrick"
  require "webrick/https"
  ready = Thread::Queue.new
  started = -> { ready << true }
  web = WEBrick::HTTPServer.new(BindAddress: "127.0.0.1", Port: 0, SSLEnable: true,
                                SSLCertificate: leaf.first, SSLPrivateKey: leaf.last,
                                Logger: WEBrick::Log.new(File::NULL), AccessLog: [], StartCallback: started)
  implementation = server
  web.mount_proc("/") do |request, response|
    output = implementation.call(path: request.path, http_method: request.request_method,
                                 headers: request.header, input: StringIO.new(request.body || ""))
    response.status = output.status
    output.headers.each { |key, values| response[key] = values.join(", ") }
    response.body = output.body
  end
  worker = Thread.new { web.start }
  ready.pop
  begin
    smoke = <<~CODE
      require "connectrpc/async"
      require "connectrpc/bindings/google_protobuf"
      $LOAD_PATH.unshift(File.expand_path("generated", Dir.pwd))
      require "echo_connect"
      abort "OpenSSL activation" unless Gem.loaded_specs.fetch("openssl").version.to_s == "4.0.2"
      abort "JSON activation" unless Gem.loaded_specs.fetch("json").version.to_s == "3.1.0"
      core = ConnectRPC::Client.new(base_url: ARGV.fetch(0),
        binding: ConnectRPC::Bindings::GoogleProtobuf.new, codec: :json)
      result = core.call(Example::V1::EchoService::ECHO, Example::V1::EchoMessage.new(name: "core TLS JSON"))
      abort "Core NetHTTP interaction" unless result.message.name == "core TLS JSON"
      puts "PASS installed core NetHTTP TLS/JSON with activated OpenSSL 4.0.2/JSON 3.1.0"
    CODE
    system({ "SSL_CERT_FILE" => ca_file }, RbConfig.ruby, "-e", smoke,
           "https://localhost:#{web.config.fetch(:Port)}", exception: true)
  ensure
    web.shutdown
    worker.join
  end
end
if ENV["CONNECTRPC_ASYNC_INSTALLED"]
  Gem.loaded_specs.each_value do |specification|
    next if specification.default_gem?
    next if specification.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")

    abort "Load path leaked outside isolated GEM_HOME: #{specification.name}"
  end
end
abort "Backend loaded Rails/Rack" if defined?(Rails) || defined?(Rack)
puts "HTTP/2 integration passed (Ruby #{RUBY_VERSION})"
