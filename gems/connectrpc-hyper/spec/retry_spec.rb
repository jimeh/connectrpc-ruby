# frozen_string_literal: true

RSpec.describe "Retries" do
  def requests(tag, from) = peer.since(from).grep(/\A#{tag} request /)
  def accepts(tag, from) = peer.count(/\A#{tag} accept /, from: from)

  # The peer allows one stream per connection. With stream 1 held, one queued
  # call waits inside hyper for a stream slot and the other stays queued; a
  # GOAWAY then ends the connection's admission, and hyper hands the queued
  # request back unsent. hyper reports the stream-slot waiter as failed.
  { grpc: "Exchange#write", connect: "Driver#unary" }.each do |protocol, frame|
    it "sends a request hyper hands back unsent once more on a new connection (#{protocol})" do
      from = peer.mark
      transport = hyper
      origin = peer["raw_mcs1"]
      tag = "retry-unsent-#{protocol}"
      held = Thread.new { raw_call(origin, "hold:#{tag}", transport: transport, timeout: 20).message }
      peer.wait_event(/\Arawmcs1 request hold:#{tag} /, from: from)
      queued = Array.new(2) do
        Thread.new do
          raw_call(origin, "ok", transport: transport, protocol: protocol, timeout: 20).message
        rescue ConnectRPC::Error => e
          e
        end
      end
      queued.each { |thread| wait_parked(thread, frame) }
      peer.command("raw-goaway rawmcs1")
      succeeded, failed = queued.map(&:value).partition { |outcome| outcome.is_a?(MSG) }
      expect(succeeded.size).to eq(1)
      expect(failed.map { |error| [error.code, error.message] })
        .to eq([["unavailable", "HTTP transport failed: connection closed"]])
      # Sent exactly once, as the first stream of a new connection.
      peer.wait_event(/\Arawmcs1 request ok /, from: from)
      expect(requests("rawmcs1", from).grep(/request ok /).map { |event| event[/n=\d+/] }).to eq(["n=1"])
      expect(accepts("rawmcs1", from)).to eq(2)
      peer.command("release #{tag}")
      expect(held.value).to eq(MSG.new)
    end
  end

  it "retries a Connect unary call refused with REFUSED_STREAM once, on a fresh connection" do
    from = peer.mark
    expect(raw_call(peer["raw"], "refuse-once:connect", transport: hyper, protocol: :connect).message).to eq(MSG.new)
    expect(requests("raw", from).map { |event| event[/n=\d+/] }).to eq(%w[n=1 n=1])
    expect(accepts("raw", from)).to eq(2)
  end

  it "never retries twice" do
    from = peer.mark
    error = error_of { raw_call(peer["raw"], "refuse-always", transport: hyper, protocol: :connect) }
    expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: stream reset"])
    expect(requests("raw", from).size).to eq(2)
  end

  it "does not replay a refused gRPC unary call, which open_grpc_stream cannot tell from a stream" do
    from = peer.mark
    error = error_of { raw_call(peer["raw"], "refuse-once:grpc", transport: hyper) }
    expect([error.code, error.message]).to eq(["unavailable", "HTTP/2 stream reset"])
    expect(requests("raw", from).size).to eq(1)
  end

  it "does not replay an ambiguous disconnect after the handler started" do
    from = peer.mark
    %i[connect grpc].each do |protocol|
      error = error_of { raw_call(peer["raw"], "disconnect", transport: hyper, protocol: protocol) }
      expect(error.code).to eq("unavailable")
    end
    expect(peer.since(from).grep(/raw handler-start/).size).to eq(2)
    expect(requests("raw", from).size).to eq(2)
  end
end
