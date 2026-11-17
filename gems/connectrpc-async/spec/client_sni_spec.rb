# frozen_string_literal: true

# RFC 6066 section 3: a client sends no server name for an IP-literal host.
RSpec.describe "Client TLS server name indication" do
  def ipv6_loopback?
    TCPServer.new("::1", 0).close
    true
  rescue SystemCallError, SocketError
    false
  end

  # Records the server names offered in each handshake, then closes.
  def with_sni_peer(task, address)
    bound = IO::Endpoint.tcp(address, 0).bound
    listener = bound.sockets.first
    names = []
    handshakes = Async::Queue.new
    context = OpenSSL::SSL::SSLContext.new
    context.min_version = OpenSSL::SSL::TLS1_2_VERSION
    context.add_certificate(*tls_material.fetch(:leaf))
    context.alpn_select_cb = lambda(&:first)
    context.servername_cb = lambda { |(_socket, name)|
      names << name
      nil
    }
    acceptor = task.async do
      loop do
        raw, = listener.accept
        ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
        ssl.sync_close = true
        begin
          ssl.accept
        rescue OpenSSL::SSL::SSLError, IOError, SystemCallError
          nil
        ensure
          AsyncHelpers::INTERNAL.raw_close(raw)
          handshakes.enqueue(true)
        end
      end
    end
    yield listener.local_address.ip_port, names, handshakes
  ensure
    AsyncHelpers::INTERNAL.cancel(acceptor)
    bound&.close
  end

  [ConnectRPC::Async::Transport, ConnectRPC::Async::HTTP1Transport].each do |kind|
    [["127.0.0.1", "127.0.0.1", []], ["[::1]", "::1", []], ["localhost", "127.0.0.1", ["localhost"]]]
      .each do |host, address, expected|
      sent = expected.empty? ? "no server name" : "the host name"
      it "sends #{sent} from #{kind.name.split('::').last} to #{host}" do
        skip "IPv6 loopback is unavailable" if address == "::1" && !ipv6_loopback?
        with_ca do |ca_file|
          reactor do |task|
            with_sni_peer(task, address) do |port, names, handshakes|
              origin = "https://#{host}:#{port}"
              transport = kind.new(origin: origin, ca_file: ca_file)
              begin
                transport.call(request("#{origin}/path", deadline: ConnectRPC::Deadline.new(5)))
              rescue ConnectRPC::Error
                nil
              end
              handshakes.dequeue
              expect(names).to eq(expected)
            ensure
              transport&.close(grace: 0)
            end
          end
        end
      end
    end
  end
end
