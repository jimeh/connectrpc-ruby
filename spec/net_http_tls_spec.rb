# frozen_string_literal: true

require "pp"
require "connectrpc/transports/net_http"
require_relative "support/socket_peer"
require_relative "support/tls_peer"

RSpec.describe "NetHTTP TLS options" do
  include SocketPeer
  include TLSPeer

  def answer
    lambda do |socket|
      read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
      socket.read
    end
  end

  def failure(transport, url)
    transport.call(transport_request(url, timeout: 5))
    raise "Expected a transport failure"
  rescue ConnectRPC::Error => e
    e
  end

  it "verifies a server signed by a ca_file root, sending SNI and ALPN http/1.1" do
    names = Queue.new
    protocols = Queue.new
    context = server_context(names: names)
    context.alpn_select_cb = lambda { |offered|
      protocols << offered
      "http/1.1"
    }
    with_ca_file do |ca_file|
      with_tls_peer(context, answer) do |port|
        transport = ConnectRPC::Transports::NetHTTP.new(ca_file: ca_file, proxy: nil)
        expect(transport.call(transport_request("https://localhost:#{port}")).body).to eq("ok")
      end
    end
    expect([names.pop(timeout: 0), protocols.pop(timeout: 0)]).to eq(["localhost", ["http/1.1"]])
  end

  it "verifies an IP literal against the certificate's IP SAN without sending it as SNI" do
    names = Queue.new
    with_ca_file do |ca_file|
      with_tls_peer(server_context(names: names), answer) do |port|
        transport = ConnectRPC::Transports::NetHTTP.new(ca_file: ca_file)
        expect(transport.call(transport_request("https://127.0.0.1:#{port}")).body).to eq("ok")
      end
    end
    expect(names.pop(timeout: 0)).to be_nil
  end

  it "rejects a server outside the trust store and a certificate for another hostname" do
    failures = Queue.new
    with_tls_peer(server_context, answer, failures: failures) do |port|
      error = failure(ConnectRPC::Transports::NetHTTP.new, "https://localhost:#{port}")
      expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: TLS verification failed"])
    end
    expect(failures.pop(timeout: 0)).to be_a(OpenSSL::SSL::SSLError)
    with_ca_file do |ca_file|
      with_tls_peer(server_context(tls_material.fetch(:other)), answer, failures: failures) do |port|
        error = failure(ConnectRPC::Transports::NetHTTP.new(ca_file: ca_file), "https://localhost:#{port}")
        expect(error.message).to eq("HTTP transport failed: TLS verification failed")
        expect(error.cause).to be_a(OpenSSL::SSL::SSLError)
      end
    end
  end

  describe "the default context" do
    let(:client_tls) { ConnectRPC::Transports::NetHTTP.const_get(:ClientTLS) }

    # Records the client side only; the TLS peer runs in its own thread.
    def contexts
      contexts = Queue.new
      client = Thread.current
      allow(OpenSSL::SSL::SSLSocket).to receive(:new).and_wrap_original do |original, socket, context|
        contexts << context if Thread.current == client
        original.call(socket, context)
      end
      contexts
    end

    # Loading the system trust store costs milliseconds and hundreds of KiB.
    it "loads no trust store at construction and shares one frozen context across transports" do
      allow(OpenSSL::X509::Store).to receive(:new).and_call_original
      transports = Array.new(2) { ConnectRPC::Transports::NetHTTP.new(proxy: nil) }
      expect(OpenSSL::X509::Store).not_to have_received(:new)
      used = contexts
      transports.each do |transport|
        with_tls_peer(server_context, answer) do |port|
          expect(failure(transport, "https://localhost:#{port}").message)
            .to eq("HTTP transport failed: TLS verification failed")
        end
      end
      first, second = Array.new(2) { used.pop(timeout: 0) }
      expect([first, first.frozen?, first.verify_mode]).to eq([second, true, OpenSSL::SSL::VERIFY_PEER])
    end

    it "builds a transport's own context when it has TLS options" do
      with_ca_file do |ca_file|
        allow(OpenSSL::X509::Store).to receive(:new).and_call_original
        transport = ConnectRPC::Transports::NetHTTP.new(ca_file: ca_file, proxy: nil)
        expect(OpenSSL::X509::Store).to have_received(:new).once
        used = contexts
        with_tls_peer(server_context, answer) do |port|
          expect(transport.call(transport_request("https://localhost:#{port}", timeout: 5)).body).to eq("ok")
        end
        expect(used.pop(timeout: 0)).not_to equal(client_tls.default)
      end
    end

    it "reads SSL_CERT_FILE when the default is first used, not at construction" do
      saved = client_tls.instance_variable_get(:@default)
      previous = ENV.fetch("SSL_CERT_FILE", nil)
      client_tls.instance_variable_set(:@default, nil)
      transport = ConnectRPC::Transports::NetHTTP.new(proxy: nil)
      with_ca_file do |ca_file|
        ENV["SSL_CERT_FILE"] = ca_file
        with_tls_peer(server_context, answer) do |port|
          expect(transport.call(transport_request("https://localhost:#{port}", timeout: 5)).body).to eq("ok")
        end
      end
    ensure
      client_tls.instance_variable_set(:@default, saved)
      previous.nil? ? ENV.delete("SSL_CERT_FILE") : ENV["SSL_CERT_FILE"] = previous
    end
  end

  describe "client certificates" do
    def mtls_call(identity)
      peers = Queue.new
      failures = Queue.new
      handler = lambda do |socket|
        peers << socket.peer_cert.subject.to_s
        answer.call(socket)
      end
      result = nil
      with_ca_file do |ca_file|
        context = server_context(client_ca: tls_material.fetch(:client_root))
        with_tls_peer(context, handler, failures: failures) do |port|
          result = ConnectRPC::Transports::NetHTTP.new(ca_file: ca_file, **identity)
                                                  .call(transport_request("https://localhost:#{port}", timeout: 5))
        rescue ConnectRPC::Error => e
          result = e
        end
      end
      [result, peers.pop(timeout: 0), failures.pop(timeout: 0)]
    end

    it "authenticates with a leaf and intermediate to a server that trusts only the root" do
      result, peer, failure = mtls_call(client_identity)
      expect([result.body, peer, failure]).to eq(["ok", "/CN=client", nil])
    end

    [["without an identity", {}], ["without the intermediate", :leaf_only]].each do |label, identity|
      it "is rejected by the server #{label}" do
        identity = client_identity(%i[client]) if identity == :leaf_only
        result, peer, failure = mtls_call(identity)
        expect(result).to be_a(ConnectRPC::Error)
        expect([result.code, peer]).to eq(["unavailable", nil])
        expect(failure).to be_a(OpenSSL::SSL::SSLError)
      end
    end
  end

  describe "input validation, matching ConnectRPC::Async" do
    def invalid(**)
      ConnectRPC::Transports::NetHTTP.new(**)
      raise "Expected an invalid TLS configuration"
    rescue ArgumentError => e
      e
    end

    {
      "an unreadable ca_file" => [:ca_file, -> { { ca_file: "/nonexistent/private-ca.pem" } }],
      "a missing private key" => [:private_key, -> { client_identity.except(:private_key) }],
      "a missing certificate chain" => [:certificate_chain, -> { client_identity.except(:certificate_chain) }],
      "an empty certificate chain" => [:certificate_chain, -> { client_identity.merge(certificate_chain: []) }],
      "a non-certificate chain entry" => [:certificate_chain,
                                          -> { client_identity.merge(certificate_chain: ["not a certificate"]) }],
      "a mismatched key" => [:private_key, -> { client_identity(key: :leaf) }],
      "an expired certificate" => [:certificate_chain, -> { client_identity(%i[expired client_intermediate]) }],
      "a broken chain" => [:certificate_chain, -> { client_identity(%i[client root]) }],
      "a non-key private key" => [:private_key, -> { client_identity.merge(private_key: "not a key") }]
    }.each do |label, (input, options)|
      it "names only #{input} for #{label}, with no cause" do
        error = invalid(**instance_exec(&options))
        expect([error.message, error.cause]).to eq(["Invalid TLS client configuration: #{input}", nil])
        expect(error.message).not_to include("nonexistent", "not a", "BEGIN")
      end
    end

    it "requires an HTTPS URL when any TLS option is set, before connecting" do
      with_ca_file do |ca_file|
        transport = ConnectRPC::Transports::NetHTTP.new(ca_file: ca_file)
        expect(Socket).not_to receive(:new)
        expect { transport.call(transport_request("http://127.0.0.1:1")) }
          .to raise_error(ArgumentError, "TLS options require an HTTPS origin")
      end
    end

    it "keeps TLS material and proxy URLs out of inspect" do
      transport = ConnectRPC::Transports::NetHTTP.new(proxy: "http://user:secret@proxy.test:3128", **client_identity)
      expect(transport.inspect).to eq("#<ConnectRPC::Transports::NetHTTP proxy=explicit>")
      expect(PP.pp(transport, +"")).to eq("#<ConnectRPC::Transports::NetHTTP proxy=explicit>\n")
      expect(ConnectRPC::Transports::NetHTTP.new.inspect).to eq("#<ConnectRPC::Transports::NetHTTP proxy=environment>")
    end
  end
end
