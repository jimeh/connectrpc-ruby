# frozen_string_literal: true

RSpec.describe "gRPC native TLS rejection" do
  def grpc_client(transport, origin)
    ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: :grpc,
                           binding: ConnectRPC::Bindings::GoogleProtobuf.new)
  end

  %i[untrusted_server hostname missing_client untrusted_client].each do |failure|
    it "rejects #{failure} before dispatch and releases owned sockets" do
      reactor do
        seen = []
        identity = tls_material.fetch(:leaf)
        if failure == :hostname
          certificate = OpenSSL::X509::Certificate.new(identity.first.to_der)
          certificate.extensions = certificate.extensions.reject { |entry| entry.oid == "subjectAltName" }
          factory = OpenSSL::X509::ExtensionFactory.new
          factory.subject_certificate = certificate
          factory.issuer_certificate = tls_material.fetch(:root).first
          certificate.add_extension(factory.create_extension("subjectAltName", "DNS:elsewhere.invalid"))
          certificate.sign(tls_material.fetch(:root).last, "SHA256")
          identity = [certificate, identity.last]
        end
        target = server(protocol: :grpc, echo: lambda { |message, _|
          seen << true
          message
        })
        anchors = %i[missing_client untrusted_client].include?(failure) ? [tls_material.fetch(:client_ca)] : nil
        host = ConnectRPC::Async::Host.new(server: target, tls: host_tls(identity, anchors: anchors)).start
        origin = "https://localhost:#{host.port}"
        with_ca do |ca_file|
          options = failure == :untrusted_server ? {} : { ca_file: ca_file }
          if failure == :untrusted_client
            certificate, key = credentials(999, "untrusted client", usage: "clientAuth")
            options.merge!(certificate_chain: [certificate.to_pem], private_key: key.private_to_pem)
          end
          transport = ConnectRPC::Async::Transport.new(origin: origin, **options)
          expect do
            grpc_client(transport, origin).call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new, timeout: nil)
          end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
          expect(seen).to be_empty
          expect(transport.instance_variable_get(:@calls)).to be_empty
        ensure
          transport&.close(grace: 0)
        end
      ensure
        host&.close(grace: 0)
      end
    end
  end

  [nil, "http/1.1"].each do |alpn|
    it "rejects negotiated ALPN #{alpn.inspect} before sending HTTP bytes" do
      reactor do |task|
        with_ca do |ca_file|
          identity = tls_material.fetch(:leaf)
          context = OpenSSL::SSL::SSLContext.new
          context.add_certificate(identity.first, identity.last, [])
          context.min_version = OpenSSL::SSL::TLS1_2_VERSION
          context.alpn_select_cb = ->(_offers) { alpn } if alpn
          bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
          eof = Async::Queue.new
          peer = task.async do
            raw, = bound.sockets.first.accept
            ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
            ssl.sync_close = true
            ssl.accept
            begin
              eof.enqueue(ssl.read(1))
            rescue OpenSSL::SSL::SSLError, EOFError
              eof.enqueue(nil)
            end
          ensure
            AsyncHelpers::INTERNAL.raw_close(raw)
            ssl&.close
          end
          origin = "https://localhost:#{bound.sockets.first.local_address.ip_port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
          expect do
            grpc_client(transport, origin).call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new, timeout: nil)
          end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
          expect(eof.dequeue).to be_nil
          peer.wait
        ensure
          transport&.close(grace: 0)
          peer&.cancel
          peer&.wait
          bound&.close
        end
      end
    end
  end
end
