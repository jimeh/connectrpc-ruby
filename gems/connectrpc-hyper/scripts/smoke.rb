# frozen_string_literal: true

# Smoke test for an installed connectrpc-hyper gem (source or platform build):
# loads the native extension and makes real Connect unary calls over cleartext
# HTTP/1.1 and over TLS (aws-lc-rs, ALPN http/1.1) against servers this script
# hosts on 127.0.0.1. Needs only Ruby's stdlib, connectrpc and the gem.
require "connectrpc/hyper"
require "openssl"
require "socket"
require "tmpdir"

# Real paths: macOS temporary directories resolve through /private.
if ENV["GEM_HOME"]
  gem_path = File.realpath(Gem.loaded_specs.fetch("connectrpc-hyper").full_gem_path)
  gem_home = File.realpath(ENV.fetch("GEM_HOME"))
  abort "Smoke loaded connectrpc-hyper outside GEM_HOME" unless gem_path.start_with?("#{gem_home}/")
end
abort "Optional runtimes loaded" if defined?(Async) || defined?(Rack) || defined?(Google::Protobuf)

# Passes message bytes through unchanged.
class SmokeBinding
  def encode(message, **) = message
  def decode(bytes, **) = bytes
end

def credentials(name, issuer: nil)
  key = OpenSSL::PKey::EC.generate("prime256v1")
  certificate = OpenSSL::X509::Certificate.new
  certificate.version = 2
  certificate.serial = rand(1 << 32)
  certificate.subject = OpenSSL::X509::Name.parse("/CN=#{name}")
  certificate.issuer = issuer ? issuer.first.subject : certificate.subject
  certificate.public_key = key
  certificate.not_before = Time.now - 60
  certificate.not_after = Time.now + 3600
  factory = OpenSSL::X509::ExtensionFactory.new
  factory.subject_certificate = certificate
  factory.issuer_certificate = issuer ? issuer.first : certificate
  certificate.add_extension(factory.create_extension("basicConstraints", issuer ? "CA:FALSE" : "CA:TRUE", true))
  certificate.add_extension(factory.create_extension("subjectAltName", "IP:127.0.0.1")) if issuer
  certificate.sign(issuer ? issuer.last : key, "SHA256")
  [certificate, key]
end

# Echoes each Connect unary request body; one thread per connection.
def serve(context = nil) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  server = TCPServer.new("127.0.0.1", 0)
  Thread.new do
    loop do
      raw = server.accept
      Thread.new(raw) do |connection|
        socket = context ? OpenSSL::SSL::SSLSocket.new(connection, context).tap(&:accept) : connection
        while (line = socket.gets("\r\n"))
          length = 0
          while (header = socket.gets("\r\n")) && header != "\r\n"
            length = header.split(":", 2).last.to_i if header.downcase.start_with?("content-length:")
          end
          body = socket.read(length)
          socket.write("HTTP/1.1 200 OK\r\ncontent-type: application/proto\r\n" \
                       "content-length: #{body.bytesize}\r\n\r\n#{body}")
          line.clear
        end
      rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
        nil
      ensure
        socket&.close
      end
    end
  rescue IOError
    nil
  end
  server
end

method = ConnectRPC::Method.new(service: "smoke.v1.Smoke", name: "Echo", request_type: String, response_type: String)
call = lambda do |origin, transport|
  client = ConnectRPC::Client.new(base_url: origin, binding: SmokeBinding.new, transport: transport)
  client.call(method, "smoke-#{origin}".b, timeout: 10).message
end

plain = serve
root = credentials("smoke CA")
leaf = credentials("127.0.0.1", issuer: root)
context = OpenSSL::SSL::SSLContext.new
context.add_certificate(leaf.first, leaf.last)
context.alpn_select_cb = ->(protocols) { protocols.include?("http/1.1") ? "http/1.1" : nil }
secure = serve(context)
Dir.mktmpdir("connectrpc-hyper-smoke-") do |directory|
  ca_file = File.join(directory, "ca.pem")
  File.write(ca_file, root.first.to_pem)
  cleartext = ConnectRPC::Hyper::Transport.new(http2: false)
  tls = ConnectRPC::Hyper::Transport.new(ca_file: ca_file)
  plain_origin = "http://127.0.0.1:#{plain.addr[1]}"
  tls_origin = "https://127.0.0.1:#{secure.addr[1]}"
  2.times do
    abort "Cleartext smoke failed" unless call.call(plain_origin, cleartext) == "smoke-#{plain_origin}"
    abort "TLS smoke failed" unless call.call(tls_origin, tls) == "smoke-#{tls_origin}"
  end
  abort "Close did not drain" unless cleartext.close(grace: 5) && tls.close(grace: 5)
end
plain.close
secure.close
puts "connectrpc-hyper #{ConnectRPC::Hyper::VERSION} smoke passed on Ruby #{RUBY_VERSION} (#{RUBY_PLATFORM}): " \
     "cleartext HTTP/1.1 and TLS calls, reuse and close"
