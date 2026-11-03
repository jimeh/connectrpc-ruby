# frozen_string_literal: true

require "json"
require "webrick"
require "connectrpc/falcon"
require "connectrpc/rack"
require "connectrpc/bindings/google_protobuf"
$LOAD_PATH.unshift(File.expand_path("../interop/grpc/ruby", __dir__))
require_relative "../interop/grpc/ruby/peer_connect"

# A real fixture process with explicit local control and observable shutdown.
module BrowserFixture # rubocop:disable Metrics/ModuleLength
  PEER = Connectrpc::Grpcinterop::V1::Peer
  MESSAGE = Connectrpc::Grpcinterop::V1::Message
  ROOT = File.expand_path("..", __dir__)

  def self.page_server # rubocop:disable Metrics/MethodLength
    server = WEBrick::HTTPServer.new(BindAddress: "127.0.0.1", Port: 0, AccessLog: [],
                                     Logger: WEBrick::Log.new(File::NULL))
    server.mount_proc("/") do |request, response|
      if request.path == "/client.js"
        response["content-type"] = "application/javascript"
        response.body = File.binread(File.join(ROOT, "vendor/browser/client.js"))
      else
        response["content-type"] = "text/html"
        response.body = '<!doctype html><title>gRPC-Web fixture</title><script src="/client.js"></script>'
      end
    end
    server
  end

  def self.tls # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    key = OpenSSL::PKey::RSA.new(2048)
    certificate = OpenSSL::X509::Certificate.new
    certificate.version = 2
    certificate.serial = 1
    certificate.subject = certificate.issuer = OpenSSL::X509::Name.parse("/CN=localhost")
    certificate.public_key = key
    certificate.not_before = Time.now - 60
    certificate.not_after = Time.now + 3600
    factory = OpenSSL::X509::ExtensionFactory.new
    factory.subject_certificate = factory.issuer_certificate = certificate
    certificate.add_extension(factory.create_extension("subjectAltName", "DNS:localhost,IP:127.0.0.1"))
    certificate.sign(key, "SHA256")
    { certificate_chain: [certificate.to_pem], private_key: key.private_to_pem, client_ca: nil }
  end

  def self.run # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    pages = [page_server, page_server]
    threads = pages.map { |page| Thread.new { page.start } }
    origins = pages.map { |page| "http://127.0.0.1:#{page.config.fetch(:Port)}" }
    require_relative "rails_fixture"
    rails, rails_thread, rails_origin = BrowserRails.start(origins.first)
    state = { requests: [], handlers: [], calls: [], completed: [], releases: [], sent: [], cancellation: [] }
    gates = {}
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |call|
      state[:calls] << call.protocol
      nil
    end
    instrumentation = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    policy = ConnectRPC::GRPCWebCORS.new(origins: [origins.first], credentials: true,
                                         request_headers: %w[authorization x-fixture], expose_headers: %w[x-leading])
    binding = ConnectRPC::Bindings::GoogleProtobuf.new
    handlers = PEER.handlers(
      unary: lambda { |message, context|
        state[:handlers] << message.value
        context.response_metadata["x-leading"] = ["visible"]
        context.response_trailers["x-duplicate"] = %w[one two]
        if message.value == "error"
          raise ConnectRPC::Error.new(:not_found, "missing % café", details: [
                                        ConnectRPC::ErrorDetail.new(type: "connectrpc.grpcinterop.v1.Message",
                                                                    value: MESSAGE.encode(message))
                                      ])
        end
        message
      },
      server_stream: lambda { |message, writer, context|
        begin
          state[:handlers] << message.value
          writer.send_message(MESSAGE.new(value: "first", payload: message.payload))
          state[:sent] << message.value
          gate = gates[message.value] ||= Async::Queue.new
          gate.dequeue
          writer.send_message(MESSAGE.new(value: "second"))
        ensure
          state[:completed] << message.value
          state[:cancellation] << { id: message.value, reason: context.cancellation_reason }
        end
      },
      client_stream: ->(*) { raise "unsupported handler ran" }, bidi: ->(*) { raise "unsupported handler ran" }
    )
    target = ConnectRPC::Server.new(services: [PEER::SERVICE], binding: binding, handlers: handlers,
                                    protocol: :grpc_web, cors: policy, instrumentation: instrumentation)
    no_credentials = ConnectRPC::Server.new(
      services: [PEER::SERVICE], binding: binding, handlers: handlers, protocol: :grpc_web,
      cors: ConnectRPC::GRPCWebCORS.new(origins: [origins.first]), instrumentation: instrumentation
    )
    outer_owned = ConnectRPC::Server.new(services: [PEER::SERVICE], binding: binding, handlers: handlers,
                                         protocol: :grpc_web, cors: nil, instrumentation: instrumentation)
    fallback = lambda do |env|
      if env["PATH_INFO"] == "/__release"
        id = URI.decode_www_form(env["QUERY_STRING"] || "").to_h.fetch("id")
        state[:releases] << id
        (gates[id] ||= Async::Queue.new).enqueue(true)
      end
      [200, { "content-type" => "application/json", "cache-control" => "no-store" },
       [JSON.generate(state.merge(rails: BrowserRails.events))]]
    end
    host_type = Class.new(ConnectRPC::Falcon::Server) do
      define_method(:call_with_publication) do |request, **options|
        state[:requests] << { method: request.method, path: request.path,
                              protocol: request.respond_to?(:stream) ? "h2" : "http1", host: "falcon" }
        super(request, **options)
      end
      define_method(:call) do |request, **options|
        state[:requests] << { method: request.method, path: request.path,
                              protocol: request.respond_to?(:stream) ? "h2" : "http1", host: "falcon" }
        super(request, **options)
      end
    end
    native_application = Class.new(ConnectRPC::Async::Application) do
      define_method(:call_with_publication) do |request, **options|
        state[:requests] << { method: request.method, path: request.path, protocol: "h2", host: "async" }
        super(request, **options)
      end
    end.new(server: target)
    native_host_type = Class.new(ConnectRPC::Async::Host) do
      define_method(:build_session) do |**options|
        options[:application] = native_application
        options[:lifecycle_factory] = native_application.method(:start_call)
        ConnectRPC::Async::ServerSession.new(**options)
      end
    end
    # Deliberately inconsistent outer-owned policy demonstrates response
    # unreadability after a credentialed side effect, distinct from suppression.
    unreadable_type = Class.new(host_type) do
      define_method(:call) do |request, **options|
        if request.method == "OPTIONS"
          return Protocol::HTTP::Response[204, {
            "access-control-allow-origin" => origins.first, "access-control-allow-credentials" => "true",
            "access-control-allow-methods" => "POST", "access-control-max-age" => "0",
            "access-control-allow-headers" => "content-type,x-grpc-web,x-user-agent"
          }, nil]
        end
        super(request, **options)
      end
      define_method(:call_with_publication) do |request, publication:, **options|
        publication.on_publish { |head| head.headers.add("access-control-allow-origin", origins.first) }
        super(request, publication: publication, **options)
      end
    end
    Async do # rubocop:disable Metrics/BlockLength -- fixture hosts share deterministic teardown
      bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
      secure = ConnectRPC::Falcon::Endpoint.parse("https://127.0.0.1:0").bound
      host = host_type.new(server: target, endpoint: bound, rack_app: fallback)
      secure_host = host_type.new(server: target, endpoint: secure, tls: tls, rack_app: fallback)
      host.run
      secure_host.run
      native_host = native_host_type.new(server: target, tls: tls).start
      no_credentials_bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
      no_credentials_host = host_type.new(server: no_credentials, endpoint: no_credentials_bound)
      no_credentials_host.run
      unreadable_bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
      unreadable_host = unreadable_type.new(server: outer_owned, endpoint: unreadable_bound)
      unreadable_host.run
      puts JSON.generate(page: origins.first, denied_page: origins.last, rails: rails_origin,
                         async: "https://127.0.0.1:#{native_host.port}",
                         no_credentials: "http://127.0.0.1:#{no_credentials_bound.sockets.first.local_address.ip_port}",
                         unreadable: "http://127.0.0.1:#{unreadable_bound.sockets.first.local_address.ip_port}",
                         h1: "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}",
                         h2: "https://127.0.0.1:#{secure.sockets.first.local_address.ip_port}")
      $stdout.flush
      $stdin.gets
    ensure
      gates.each_value { |gate| gate.enqueue(true) }
      host&.close(grace: 2)
      secure_host&.close(grace: 2)
      native_host&.close(grace: 2)
      no_credentials_host&.close(grace: 2)
      unreadable_host&.close(grace: 2)
      bound&.close
      secure&.close
      no_credentials_bound&.close
      unreadable_bound&.close
    end.wait
  ensure
    pages&.each(&:shutdown)
    threads&.each(&:join)
    rails&.shutdown
    rails_thread&.join
    puts "Browser Ruby fixtures stopped"
  end
end

BrowserFixture.run if $PROGRAM_NAME == __FILE__
