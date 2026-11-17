# frozen_string_literal: true

require_relative "../../../../spec/support/socket_peer"

# One echo RPC over the core Net::HTTP transport to a localhost socket peer.
module NetHTTPPeer
  include SocketPeer

  # Returns the request headers the peer received, by lowercase name.
  def net_http_headers(instrumentation, metadata: {})
    received = nil
    reply = Example::V1::EchoMessage.new(name: "outgoing").to_proto
    handler = lambda do |socket|
      received = read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\ncontent-type: application/proto\r\n" \
                   "content-length: #{reply.bytesize}\r\nconnection: close\r\n\r\n#{reply}")
    end
    with_peer(handler) do |url, _listener|
      core = ConnectRPC::Client.new(base_url: url, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                    instrumentation: instrumentation)
      typed = Example::V1::EchoService::Client.new(client: core)
      typed.echo(Example::V1::EchoMessage.new(name: "outgoing"), metadata: metadata, timeout: 5)
    end
    received.split("\r\n").drop(1).each_with_object(Hash.new { |hash, key| hash[key] = [] }) do |line, headers|
      name, value = line.split(": ", 2)
      headers[name.downcase] << value
    end
  end

  # Baggage member keys across every baggage header line.
  def baggage_keys(headers)
    headers["baggage"].flat_map { |value| value.split(",") }.map { |member| member.strip.split("=", 2).first }
  end
end
