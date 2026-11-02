# frozen_string_literal: true

RSpec.describe "HTTP/2 client TLS setup" do
  [nil, "http/1.1"].each do |alpn|
    it "rejects a server negotiating #{alpn.inspect} and closes SSL/raw setup sockets without sending HTTP" do
      reactor do |task|
        with_ca do |ca_file|
          identity = tls_material.fetch(:leaf)
          context = OpenSSL::SSL::SSLContext.new
          context.add_certificate(identity.first, identity.last, [])
          context.min_version = OpenSSL::SSL::TLS1_2_VERSION
          context.alpn_select_cb = ->(_offers) { alpn } if alpn
          bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
          listener = bound.sockets.first
          eof = Async::Queue.new
          peer = task.async do
            raw, = listener.accept
            ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
            ssl.sync_close = true
            ssl.accept
            begin
              bytes = ssl.read(1)
              eof.enqueue(bytes)
            rescue OpenSSL::SSL::SSLError, EOFError
              eof.enqueue(nil)
            end
          ensure
            AsyncHelpers::INTERNAL.raw_close(raw)
            ssl&.close
          end
          origin = "https://localhost:#{listener.local_address.ip_port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
          expect do
            client(transport, origin).echo(AsyncHelpers::MESSAGE.new, timeout: nil)
          end.to raise_error(ConnectRPC::Error) { |e|
                   expect(e.code).to eq("unavailable")
                 }
          expect(eof.dequeue).to be_nil
          peer.wait
          record = transport.instance_variable_get(:@resources).first
          expect(record.raw).to be_closed
          expect(record.socket).to be_closed
          expect(record.task).to be_finished
        ensure
          transport&.close(grace: 0)
          peer&.cancel
          peer&.wait
          bound&.close
        end
      end
    end
  end

  it "verifies server trust and hostname before any handler, including localhost" do
    reactor do |task|
      seen = []
      implementation = server(echo: lambda { |message, _|
        seen << true
        message
      })
      host = ConnectRPC::Async::Host.new(server: implementation, tls: host_tls).start
      origin = "https://localhost:#{host.port}"
      untrusted = ConnectRPC::Async::Transport.new(origin: origin)
      expect { client(untrusted, origin).echo(AsyncHelpers::MESSAGE.new) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("unavailable")
      }
      untrusted.close
      host.close(grace: 0)
      identity = tls_material.fetch(:leaf)
      bad = OpenSSL::X509::Certificate.new(identity.first.to_der)
      bad.extensions = bad.extensions.reject { |extension| extension.oid == "subjectAltName" }
      factory = OpenSSL::X509::ExtensionFactory.new
      factory.subject_certificate = bad
      factory.issuer_certificate = tls_material.fetch(:root).first
      bad.add_extension(factory.create_extension("subjectAltName", "DNS:elsewhere.invalid"))
      bad.sign(tls_material.fetch(:root).last, "SHA256")
      host = ConnectRPC::Async::Host.new(server: implementation, tls: host_tls([bad, identity.last])).start
      origin = "https://localhost:#{host.port}"
      with_ca do |ca_file|
        mismatch = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        expect { client(mismatch, origin).echo(AsyncHelpers::MESSAGE.new) }.to raise_error(ConnectRPC::Error) { |e|
          expect(e.code).to eq("unavailable")
        }
        task.yield
        expect(seen).to be_empty
        expect(mismatch.instance_variable_get(:@resources).first.raw).to be_closed
      ensure
        mismatch&.close(grace: 0)
      end
    ensure
      untrusted&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "requires complete paired client identity, rejects plaintext TLS options and invalid origins and bounds" do
    reactor do
      %w[http://localhost/ http://user@localhost http://localhost?query https://localhost/#fragment
         ftp://localhost].each do |origin|
        expect { ConnectRPC::Async::Transport.new(origin: origin) }.to raise_error(ArgumentError)
      end
      expect do
        ConnectRPC::Async::Transport.new(origin: "http://localhost", ca_file: "invalid")
      end.to raise_error(ArgumentError)
      expect do
        ConnectRPC::Async::Transport.new(origin: "https://localhost",
                                         private_key: tls_material.fetch(:leaf).last.private_to_pem)
      end.to raise_error(ArgumentError)
      [0, -1, 0.5, Float::INFINITY, Float::NAN, "1"].each do |value|
        expect do
          ConnectRPC::Async::Transport.new(origin: "http://localhost", max_requests: value)
        end.to raise_error(ArgumentError)
        expect { ConnectRPC::Async::Host.new(server: server, max_connections: value) }.to raise_error(ArgumentError)
      end
      [0, -1, Float::INFINITY, Float::NAN, "1"].each do |value|
        expect do
          ConnectRPC::Async::Transport.new(origin: "http://localhost", connect_timeout: value)
        end.to raise_error(ArgumentError)
        expect { ConnectRPC::Async::Host.new(server: server, handshake_timeout: value) }.to raise_error(ArgumentError)
      end
    end
  end
end
