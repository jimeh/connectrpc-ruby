# frozen_string_literal: true

RSpec.describe "User-Agent on the wire" do
  connect_agent = "connectrpc-ruby/#{ConnectRPC::VERSION} (ruby #{RUBY_VERSION})"
  grpc_agent = "grpc-ruby-connectrpc/#{ConnectRPC::VERSION} (ruby #{RUBY_VERSION})"

  # The user-agent fields the peer received, in order.
  def agents(origin, protocol:, metadata: {}, **)
    from = peer.mark
    client(origin, transport: hyper(**), protocol: protocol)
      .unary(MSG.new(value: "user-agent"), metadata: metadata, timeout: 5)
    tag = protocol == :grpc ? "grpc" : "connect"
    JSON.parse(peer.wait_event(/\A#{tag} user-agent /, from: from).split(" ", 3).last)
  end

  [
    ["gRPC over h2c", "grpc_h2c", :grpc, {}, grpc_agent],
    ["Connect over h2c", "connect_h2c", :connect, {}, connect_agent],
    ["Connect over HTTP/1.1", "connect_h2c", :connect, { http2: false }, connect_agent]
  ].each do |label, name, protocol, options, default|
    context label do
      it "sends core's default unchanged" do
        expect(agents(peer[name], protocol: protocol, **options)).to eq([default])
      end

      {
        "a caller value" => [{ "user-agent" => "custom/1" }, ["custom/1"]],
        "repeated values" => [{ "user-agent" => ["first/1", "second/2"] }, ["first/1", "second/2"]],
        "an empty value" => [{ "user-agent" => [""] }, [""]],
        "an empty list" => [{ "user-agent" => [] }, []]
      }.each do |case_label, (metadata, expected)|
        it "sends #{case_label} exactly as given" do
          expect(agents(peer[name], protocol: protocol, metadata: metadata, **options)).to eq(expected)
        end
      end
    end
  end
end
