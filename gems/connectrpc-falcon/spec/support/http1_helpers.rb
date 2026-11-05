# frozen_string_literal: true

require_relative "http1_wire"

module ConnectH1Helpers
  def methods
    @methods ||= [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod,
                  ConnectRPC::BidiStreamMethod].map do |type|
      type.new(service: "test.HTTP1", name: type.name.split("::").last,
               request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
    end
  end

  def target(handlers, **)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: methods.first.service, methods: methods)],
                           binding: ConnectRPC::Bindings::GoogleProtobuf.new, handlers: handlers, **)
  end

  def frame(name)
    bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: name))
    [0, bytes.bytesize].pack("CN") + bytes
  end

  def request_head(rpc, framing, length: 100_000, extra: "", method: "POST", media: "application/connect+proto")
    field = framing == :fixed ? "Content-Length: #{length}" : "Transfer-Encoding: chunked"
    "#{method} #{rpc.path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: #{media}\r\n#{field}\r\n#{extra}\r\n"
  end

  def upload(bytes, framing, finish: false)
    return bytes if framing == :fixed

    "#{bytes.bytesize.to_s(16)}\r\n#{bytes}\r\n#{"0\r\n\r\n" if finish}"
  end

  def tls_context
    context = OpenSSL::SSL::SSLContext.new
    context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER)
    store = OpenSSL::X509::Store.new
    store.add_cert(tls_material[:root].first)
    context.cert_store = store
    context.alpn_protocols = ["http/1.1"]
    context
  end

  def with_host(implementation, secure, **)
    falcon(server: implementation, scheme: secure ? "https" : "http", tls: secure ? host_tls : nil,
           **) do |host, origin|
      peer = ConnectH1Wire.new(origin, context: secure ? tls_context : nil)
      yield host, peer, origin
    ensure
      peer&.close
    end
  end
end
