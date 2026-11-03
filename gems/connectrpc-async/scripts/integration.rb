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
      host = ConnectRPC::Async::Host.new(server: server(encoding: :gzip),
                                         tls: tls ? host_tls(leaf, anchors: [ca]) : nil).start
      origin = "#{tls ? 'https' : 'http'}://localhost:#{host.port}"
      options = { origin: origin }
      options.merge!(ca_file: ca_file, **host_tls(identity).except(:client_ca)) if tls
      transport = ConnectRPC::Async::Transport.new(**options)
      exchanges = []
      observed = lambda do |request|
        transport.call(request).tap { |response| exchanges << [request, response] }
      end
      %i[proto json].product(%i[identity gzip]).each do |codec, encoding|
        typed = client(observed, origin, codec: codec, encoding: encoding)
        message = AsyncHelpers::MESSAGE.new(name: "", count: 9_007_199_254_740_993, data: "\x00\xff".b)
        response = typed.echo(message, metadata: { "x-request" => %w[a b] })
        unless response.message == message && response.message.has_name? &&
               response.metadata["x-echo"] == %w[a b] && response.trailers["x-tail"] == %w[three four]
          raise "Installed typed/presence/metadata"
        end

        outgoing, incoming = exchanges.last
        if encoding == :gzip
          unless outgoing.headers["content-encoding"] == ["gzip"] && incoming.headers["content-encoding"] == ["gzip"]
            raise "Installed gzip coding changed"
          end

          binding = ConnectRPC::Bindings::GoogleProtobuf.new
          [outgoing, incoming].each do |wire|
            unless wire.body.b.start_with?("\x1f\x8b".b) &&
                   binding.decode(Zlib.gunzip(wire.body), type: AsyncHelpers::MESSAGE, codec: codec) == message
              raise "Installed opaque gzip bytes changed"
            end
          end
        elsif incoming.headers.key?("content-encoding")
          raise "Installed identity default changed"
        end

        begin
          typed.fail(message)
          raise "Expected installed error"
        rescue ConnectRPC::Error => e
          raise "Installed typed error" unless e.code == "invalid_argument" &&
                                               AsyncHelpers::MESSAGE.decode(e.details.first.value) == message
        end
        raise "Installed error coding changed" if exchanges.last.last.headers.key?("content-encoding")

        get_typed = client(observed, origin, codec: codec, encoding: encoding, use_get: true)
        get_result = get_typed.echo(message, metadata: { "x-request" => %w[a b] })
        unless get_result.message == message && exchanges.last.first.http_method == "GET" &&
               exchanges.last.first.body.empty? && get_result.trailers["x-tail"] == %w[three four]
          raise "Installed generated native GET"
        end

        puts "PASS installed generated GET #{tls ? 'mTLS h2' : 'h2c'} #{codec}/#{encoding}"
        puts "PASS installed generated #{tls ? 'mTLS h2' : 'h2c'} #{codec}/#{encoding} success/error/details/metadata"
      end
    ensure
      transport&.close
      host&.close
    end
  end

  # Exercise core NetHTTP identity/gzip after this graph activates
  # OpenSSL 4.0.2 and JSON 3.1.0, with trust scoped to this child process.
  require "webrick"
  require "webrick/https"
  ready = Thread::Queue.new
  started = -> { ready << true }
  web = WEBrick::HTTPServer.new(BindAddress: "127.0.0.1", Port: 0, SSLEnable: true,
                                SSLCertificate: leaf.first, SSLPrivateKey: leaf.last,
                                Logger: WEBrick::Log.new(File::NULL), AccessLog: [], StartCallback: started)
  implementation = server(encoding: :gzip)
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
    smoke = <<~'CODE'
      require "connectrpc/async"
      require "connectrpc/bindings/google_protobuf"
      $LOAD_PATH.unshift(File.expand_path("generated", Dir.pwd))
      require "echo_connect"
      abort "OpenSSL activation" unless Gem.loaded_specs.fetch("openssl").version.to_s == "4.0.2"
      abort "JSON activation" unless Gem.loaded_specs.fetch("json").version.to_s == "3.1.0"
      %i[identity gzip].each do |encoding|
        core = ConnectRPC::Client.new(base_url: ARGV.fetch(0),
          binding: ConnectRPC::Bindings::GoogleProtobuf.new, codec: :json, encoding: encoding)
        result = core.call(Example::V1::EchoService::ECHO, Example::V1::EchoMessage.new(name: "core TLS JSON"))
        abort "Core NetHTTP interaction" unless result.message.name == "core TLS JSON"
        puts "PASS installed core NetHTTP TLS/JSON/#{encoding} with activated OpenSSL 4.0.2/JSON 3.1.0/Zlib #{Zlib::VERSION}"
      end
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
