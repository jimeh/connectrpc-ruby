# frozen_string_literal: true

module AsyncHelpers
  INTERNAL = ConnectRPC::Async.const_get(:Internal)
  LIFECYCLE_ERROR = ConnectRPC::Async.const_get(:LifecycleError)
  ECHO = Example::V1::EchoService
  MESSAGE = Example::V1::EchoMessage

  def reactor(&block)
    unhandled = []
    # Observe the stack's unhandled-task warning hook without changing Console
    # or any runtime class. A background output failure must fail the example.
    trace = TracePoint.new(:call) do |event|
      unhandled << event.self if event.defined_class == Async::Task && event.method_id == :warn
    end
    result = trace.enable do
      Async(finished: false) do |task|
        task.with_timeout(10) { block.call(task) }
      ensure
        task.children&.to_a&.each { |child| INTERNAL.cancel(child) }
      end.wait
    end
    raise unhandled.first.instance_variable_get(:@promise).value unless unhandled.empty?

    result
  ensure
    trace&.disable
  end

  def server(echo: nil, **)
    binding = ConnectRPC::Bindings::GoogleProtobuf.new
    echo ||= lambda do |request, context|
      context.response_metadata["x-repeat"] = %w[one two]
      context.response_metadata["x-echo"] = context.request_metadata["x-request"] || []
      context.response_trailers["x-tail"] = %w[three four]
      request
    end
    fail_handler = lambda do |request, _context|
      detail = ConnectRPC::ErrorDetail.new(type: "example.v1.EchoMessage", value: MESSAGE.encode(request))
      raise ConnectRPC::Error.new(
        :invalid_argument, "deliberate", details: [detail],
                                         metadata: { "x-error" => %w[a b] }, trailers: { "x-end" => %w[c d] }
      )
    end
    ConnectRPC::Server.new(services: [ECHO::SERVICE], binding: binding,
                           handlers: ECHO.handlers(echo: echo, fail: fail_handler), **)
  end

  def client(transport, origin, codec: :proto, **)
    core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                  binding: ConnectRPC::Bindings::GoogleProtobuf.new, codec: codec, **)
    ECHO::Client.new(client: core)
  end

  def request(url, headers: {}, body: "", deadline: nil, limit: 4 * 1024 * 1024, method: "POST")
    ConnectRPC::TransportRequest.new(url: URI(url), http_method: method, headers: headers, body: body,
                                     deadline: deadline, response_limit: limit)
  end

  def credentials(serial, name, issuer: nil, authority: false, usage: nil, key: nil)
    key ||= OpenSSL::PKey::RSA.new(2048)
    certificate = OpenSSL::X509::Certificate.new
    certificate.version = 2
    certificate.serial = serial
    certificate.subject = OpenSSL::X509::Name.parse("/CN=#{name}")
    certificate.issuer = issuer ? issuer.first.subject : certificate.subject
    certificate.public_key = key
    certificate.not_before = Time.now - 60
    certificate.not_after = Time.now + 3600
    factory = OpenSSL::X509::ExtensionFactory.new
    factory.subject_certificate = certificate
    factory.issuer_certificate = issuer ? issuer.first : certificate
    certificate.add_extension(factory.create_extension("basicConstraints", authority ? "CA:TRUE" : "CA:FALSE", true))
    key_usage = authority ? "keyCertSign,cRLSign" : "digitalSignature,keyEncipherment"
    certificate.add_extension(factory.create_extension("keyUsage", key_usage, true))
    certificate.add_extension(factory.create_extension("extendedKeyUsage", usage)) if usage
    if usage == "serverAuth"
      certificate.add_extension(factory.create_extension("subjectAltName", "DNS:localhost,IP:127.0.0.1"))
    end
    certificate.sign(issuer ? issuer.last : key, "SHA256")
    [certificate, key]
  end

  def tls_material
    @tls_material ||= begin
      root = credentials(1, "server root", authority: true)
      client_ca = credentials(2, "client root", authority: true)
      leaf = credentials(101, "localhost", issuer: root, usage: "serverAuth")
      identity = credentials(201, "client", issuer: client_ca, usage: "clientAuth")
      { root: root, client_ca: client_ca, leaf: leaf, identity: identity }
    end
  end

  def host_tls(identity = tls_material.fetch(:leaf), anchors: nil, chain: [])
    { certificate_chain: [identity.first.to_pem, *chain.map { |entry| entry.first.to_pem }],
      private_key: identity.last.private_to_pem, client_ca: anchors&.map { |entry| entry.first.to_pem } }
  end

  def with_ca(root = tls_material.fetch(:root))
    Dir.mktmpdir("connectrpc-async-ca-", File.expand_path("../../../../vendor/async", __dir__)) do |directory|
      file = File.join(directory, "ca.pem")
      File.write(file, root.first.to_pem)
      yield file
    end
  end

  def wait_until(task, message = "condition")
    task.with_timeout(2) do
      task.yield until yield
    end
  rescue Async::TimeoutError
    raise "Timed out waiting for #{message}"
  end
end
