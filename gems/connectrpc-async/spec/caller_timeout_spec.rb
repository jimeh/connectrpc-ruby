# frozen_string_literal: true

require_relative "support/http1_client_helpers"

# A caller's enclosing Async `with_timeout` is not the call's own deadline. The
# caller's timer is the elapsed-time boundary under test throughout: each
# timer is much shorter than the bound on the blocked peer, and every wait for
# the outcome is bounded, so a transport that ignores the timer fails at the
# outcome assertion instead of hanging.
RSpec.describe "Caller Async timeouts around client calls" do
  include HTTP1ClientHelpers

  # A distinct subclass and message prove the caller receives its own
  # exception, not a transport error or an internal expiry.
  let(:caller_timeout) { Class.new(Async::TimeoutError) }

  # Waits for the call to end, then releases the blocked peer either way. A
  # transport that ignores the timer completes only after the release.
  def outcome_after(task, call, release, within: 2)
    begin
      wait_until(task, "the caller's timer to end the call", within: within) { call.finished? }
    rescue RuntimeError
      nil
    end
    release.resolve(nil)
    call.wait
  end

  def timed_call(task, seconds = 0.2, message = "caller budget", &)
    task.async(finished: false) do |caller|
      caller.with_timeout(seconds, caller_timeout, message, &)
    rescue StandardError => e
      e
    end
  end

  def h2_host(entered, release)
    deadline_host(server: server(echo: lambda { |message, _|
      unless message.name == "after"
        entered.enqueue(message.name)
        release.wait
      end
      message
    }))
  end

  [["a 30-second", 30], ["a nil", nil]].each do |label, timeout|
    it "raises the caller's own timeout from an HTTP/2 unary call with #{label} deadline" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Promise.new
        host = h2_host(entered, release)
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        typed = client(transport, origin)
        call = timed_call(task) { typed.echo(AsyncHelpers::MESSAGE.new(name: "blocked"), timeout: timeout) }
        expect(entered.dequeue).to eq("blocked")
        outcome = outcome_after(task, call, release)
        expect([outcome.class, outcome.message]).to eq([caller_timeout, "caller budget"])
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(lease_usage(transport)).to eq(0)
        expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after"), timeout: timeout).message.name).to eq("after")
      ensure
        release&.resolve(nil)
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end

    it "raises the caller's own timeout from an HTTP/1 unary call with #{label} deadline" do
      entered = Async::Queue.new
      release = Async::Promise.new
      body = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "ok"))
      response = lambda do |index, _incoming|
        if index == 1
          entered.enqueue(index)
          release.wait
        end
        http1_wire(body, fields: "Content-Type: application/proto\r\n")
      end
      with_http1_peer(response: response) do |transport, origin, accepted, _requests, task|
        typed = client(transport, origin)
        call = timed_call(task) { typed.echo(AsyncHelpers::MESSAGE.new(name: "blocked"), timeout: timeout) }
        expect(entered.dequeue).to eq(1)
        outcome = outcome_after(task, call, release)
        expect([outcome.class, outcome.message]).to eq([caller_timeout, "caller budget"])
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after"), timeout: timeout).message.name).to eq("ok")
        expect(accepted.size).to eq(2)
      ensure
        release.resolve(nil)
      end
    end
  end

  it "raises the caller's own timeout from an HTTP/2 stream operation" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Promise.new
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first) do |incoming|
        if incoming.path == "/held"
          entered.enqueue(true)
          # The client resets this stream; it never gets a response.
          release.wait
          next
        end
        incoming.send_response(Protocol::HTTP::Response[200, {}, ["ok"]])
      end
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      stream = ConnectRPC::StreamRequest.new(url: URI("#{origin}/held"), http_method: "POST", headers: {})
      call = timed_call(task) { transport.open_stream(stream, &:response_head) }
      entered.dequeue
      outcome = outcome_after(task, call, release)
      expect([outcome.class, outcome.message]).to eq([caller_timeout, "caller budget"])
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(lease_usage(transport)).to eq(0)
      expect(transport.call(request("#{origin}/after")).body).to eq("ok")
    ensure
      release&.resolve(nil)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "re-raises a caller's timeout that lands during HTTP/2 cleanup after completing disposition" do
    # The outer timer fires while cleanup waits behind a held DATA frame.
    stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 0.5)
    reactor do |task|
      host = deadline_host(server: server)
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport.call(request("#{origin}/warm")).status).not_to eq(0)
      resource, gate = gate_data(transport)
      call = timed_call(task, 0.25, "outer budget") do
        task.with_timeout(0.05, caller_timeout, "inner budget") do
          transport.call(request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}", body: "x" * 4096))
        end
      end
      gate.started.dequeue
      outcome = begin
        wait_until(task, "the caller's timer to end the call", within: 3) { call.finished? }
        call.wait
      ensure
        gate.release.enqueue(true)
      end
      expect([outcome.class, outcome.message]).to eq([caller_timeout, "outer budget"])
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(lease_usage(transport)).to eq(0)
      # The stuck frame did not retire within cleanup, so its connection failed.
      expect(resource.raw).to be_closed
      expect(transport.call(request("#{origin}/after")).status).not_to eq(0)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  def gate_data(transport)
    resource = transport.instance_variable_get(:@resources).first
    gate = FrameGate.new(resource.socket, frame_type: Protocol::HTTP2::DataFrame::TYPE)
    resource.connection.stream.instance_variable_set(:@io, gate)
    gate.armed = true
    [resource, gate]
  end
end
