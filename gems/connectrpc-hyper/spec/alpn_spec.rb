# frozen_string_literal: true

RSpec.describe "ALPN with http2: true against an HTTP/1.1-only TLS server", :interop do
  it "fails streaming calls before any request reaches the peer and lets Connect unary use HTTP/1.1" do
    from = peer.mark
    transport = hyper(**tls_options)
    grpc = client(peer["connect_tls_h1"], transport: transport)
    connect = client(peer["connect_tls_h1"], transport: transport, protocol: :connect)
    failures = [
      error_of { grpc.unary(MSG.new(value: "x"), timeout: 5) },
      error_of { grpc.server_stream(MSG.new(value: "x"), timeout: 5) { |reader| values(reader) } },
      error_of { connect.server_stream(MSG.new(value: "x"), timeout: 5) { |reader| values(reader) } },
      error_of { connect.bidi(timeout: 5) { |stream| stream.send_message(MSG.new) } }
    ]
    failures.each do |error|
      expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: ALPN negotiation failed"])
    end
    expect(peer.since(from).grep(/connecth1 http/)).to be_empty
    expect(connect.unary(MSG.new(value: "h1"), timeout: 5).message.value).to eq("h1")
    expect(peer.since(from).grep(/connecth1 http/))
      .to eq(["connecth1 http HTTP/1.1 /connectrpc.grpcinterop.v1.Peer/Unary"])
    # The untouched HTTP/1.1 socket stayed available: one connection served it all.
    expect(peer.since(from).grep(/connecth1 accept/).size).to eq(1)
  end
end
