# frozen_string_literal: true

require_relative "support/http1_client_helpers"

# A server may close an idle keep-alive connection while the next request
# crosses it. A safe GET that failed that way before any response head is sent
# once more on another connection; nothing else is resent.
RSpec.describe ConnectRPC::Async::HTTP1Transport, "stale keep-alive resend" do
  include HTTP1ClientHelpers

  # `script.call(connection, request)` answers :respond, :close (after reading
  # the request, without a response) or :partial (a complete head and a
  # truncated body). Each connection's served request count is recorded.
  def with_scripted_peer(script, **)
    served = Hash.new(0)
    peer = lambda do |native, socket, _raw, index|
      loop do
        incoming = native.read_request
        break unless incoming

        incoming.last&.each { |_| nil }
        served[index] += 1
        case script.call(index, served[index])
        when :close then break
        when :partial
          socket.write("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nok")
          break
        else
          socket.write(http1_wire)
          native.send_end_stream!
        end
      end
    end
    with_raw_http1(peer: peer, **) do |transport, origin, accepted, task|
      yield transport, origin, accepted, served, task
    end
  end

  def get(origin, **) = request("#{origin}/get", method: "GET", **)

  def failure
    yield
    raise "Expected a transport failure"
  rescue ConnectRPC::Error => e
    e
  end

  it "resends a GET once on a new connection after the reused one closes before any response" do
    retired = nil
    script = lambda do |index, count|
      retired = [@transport.instance_variable_get(:@leases).size, @first.finished?] if index == 2
      index == 1 && count == 2 ? :close : :respond
    end
    with_scripted_peer(script) do |transport, origin, accepted, served|
      @transport = transport
      expect(transport.call(get(origin)).body).to eq("ok")
      @first = transport.instance_variable_get(:@resources).first
      expect(transport.call(get(origin)).body).to eq("ok")
      expect([accepted.size, served]).to eq([2, { 1 => 2, 2 => 1 }])
      # The first lease was retired before the resend dialed.
      expect(retired).to eq([1, true])
      expect(transport.instance_variable_get(:@calls)).to be_empty
    end
  end

  it "never resends a POST" do
    script = ->(index, count) { index == 1 && count == 2 ? :close : :respond }
    with_scripted_peer(script) do |transport, origin, accepted, served|
      expect(transport.call(request(origin)).body).to eq("ok")
      error = failure { transport.call(request(origin)) }
      expect([error.code, error.message]).to eq(["unavailable", "HTTP/1 transport failed: connection closed"])
      expect([accepted.size, served]).to eq([1, { 1 => 2 }])
    end
  end

  it "surfaces the resent GET's own failure without a third attempt" do
    held = Async::Promise.new
    script = lambda do |index, count|
      # Hold the first response until a second connection exists.
      held.wait if index == 1 && count == 1
      held.resolve(nil) if index == 2
      count == 2 ? :close : :respond
    end
    with_scripted_peer(script, connection_limit: 2) do |transport, origin, accepted, served, task|
      calls = 2.times.map { task.async { transport.call(get(origin)).body } }
      expect(calls.map(&:wait)).to eq(%w[ok ok])
      expect(transport.instance_variable_get(:@idle).size).to eq(2)
      error = failure { transport.call(get(origin)) }
      expect([error.code, error.message]).to eq(["unavailable", "HTTP/1 transport failed: connection closed"])
      expect([accepted.size, served]).to eq([2, { 1 => 2, 2 => 2 }])
    end
  end

  it "does not resend a GET whose new connection closed" do
    script = ->(_index, _count) { :close }
    with_scripted_peer(script) do |transport, origin, accepted, served|
      error = failure { transport.call(get(origin)) }
      expect([error.code, error.message]).to eq(["unavailable", "HTTP/1 transport failed: connection closed"])
      expect([accepted.size, served]).to eq([1, { 1 => 1 }])
    end
  end

  it "does not resend a GET that failed after its complete response head" do
    script = ->(index, count) { index == 1 && count == 2 ? :partial : :respond }
    with_scripted_peer(script) do |transport, origin, accepted, served|
      expect(transport.call(get(origin)).body).to eq("ok")
      error = failure { transport.call(get(origin)) }
      expect(error.code).to eq("unavailable")
      expect([accepted.size, served]).to eq([1, { 1 => 2 }])
    end
  end

  it "does not resend a GET cancelled when its reused connection closed" do
    source = ConnectRPC::CancellationSource.new
    script = lambda do |index, count|
      next :respond unless index == 1 && count == 2

      source.cancel
      :close
    end
    with_scripted_peer(script) do |transport, origin, accepted, served|
      expect(transport.call(get(origin)).body).to eq("ok")
      outgoing = get(origin)
      outgoing.cancellation = source.token
      error = failure { transport.call(outgoing) }
      expect(error.code).to eq("canceled")
      expect([accepted.size, served]).to eq([1, { 1 => 2 }])
    end
  end
end
