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

  # Runs `transport_call` under a long caller timer and returns the timer's
  # handle with the caller task. The spec fires the timer at a chosen point.
  def held_call(task, &transport_call)
    handle = Async::Promise.new
    call = task.async(finished: false) do |caller|
      caller.with_timeout(5, caller_timeout, "caller budget") do |timeout|
        handle.resolve(timeout)
        transport_call.call
      end
    rescue StandardError => e
      e
    end
    [call, handle.wait]
  end

  def finished_outcome(task, call)
    wait_until(task, "the caller's timer to end the call") { call.finished? }
    call.wait
  end

  it "re-raises a caller's timeout that lands during HTTP/2 cleanup after completing disposition" do
    # The caller's zero-duration timer must fire inside the cleanup budget; a
    # wider budget keeps that margin under load and still ends the stuck
    # frame's wait well inside the outcome bound.
    stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 0.5)
    reactor do |task|
      host = deadline_host(server: server)
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport.call(request("#{origin}/warm")).status).not_to eq(0)
      resource, gate = gate_data(transport)
      waiting = Async::Promise.new
      retained = nil
      allow(AsyncHelpers::INTERNAL::Retaining).to receive(:wait).and_wrap_original do |wait, cleanup, expires, list|
        waiting.resolve(nil)
        wait.call(cleanup, expires, list)
      ensure
        retained ||= list.dup
      end
      inner = Async::Promise.new
      call, outer = held_call(task) do
        task.with_timeout(5, caller_timeout, "inner budget") do |timeout|
          inner.resolve(timeout)
          transport.call(request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}", body: "x" * 4096))
        end
      end
      gate.started.dequeue
      # The inner timer ends the call while its DATA frame is held; the outer
      # one fires once cleanup waits behind that frame.
      inner.wait.duration = 0
      waiting.wait
      outer.duration = 0
      outcome = begin
        finished_outcome(task, call)
      ensure
        gate.release.enqueue(true)
      end
      expect([outcome.class, outcome.message]).to eq([caller_timeout, "caller budget"])
      expect(retained.size).to eq(1)
      expect(retained.first).to equal(outcome)
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

  it "re-raises a caller's timeout that lands during HTTP/1 cleanup after completing disposition" do
    # The caller's zero-duration timer must fire inside the cleanup budget.
    stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 0.5)
    with_http1_peer(response: http1_wire) do |transport, origin, accepted, _requests, task|
      held = Async::Promise.new
      outgoing = request("#{origin}/held")
      # The cleanup task's lifecycle detachment holds it while the caller waits.
      outgoing.lifecycle = detach_lifecycle do
        operation = transport.instance_variable_get(:@calls).each_key.first
        next if held.resolved? || !operation&.fetch(:cleanup_task, nil).equal?(Async::Task.current)

        held.resolve(nil)
        Async::Promise.new.wait
      end
      call, timeout = held_call(task) { transport.call(outgoing) }
      held.wait
      timeout.duration = 0
      outcome = finished_outcome(task, call)
      expect([outcome.class, outcome.message]).to eq([caller_timeout, "caller budget"])
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(transport.instance_variable_get(:@leases)).to be_empty
      expect(transport.call(request("#{origin}/after")).body).to eq("ok")
      expect(accepted.size).to eq(2)
    end
  end

  it "retains a caller's timeout that lands while gRPC cleanup aborts its connection" do
    reactor do |task|
      transport = ConnectRPC::Async::Transport.new(origin: "http://127.0.0.1:1")
      aborting = Async::Promise.new
      resource = Object.new
      resource.define_singleton_method(:abort) do
        aborting.resolve(nil)
        Async::Promise.new.wait
      end
      # A failed cleanup task makes cleanup abort the connection in the caller's fiber.
      inject_stream_worker(transport, IOError.new("worker failed"), resource: resource)
      call, timeout = held_call(task) { transport.open_grpc_stream(grpc_request) { :completed } }
      aborting.wait
      timeout.duration = 0
      outcome = finished_outcome(task, call)
      expect([outcome.class, outcome.message]).to eq([caller_timeout, "caller budget"])
      expect(transport.instance_variable_get(:@calls)).to be_empty
    ensure
      transport&.close(grace: 0)
    end
  end

  # A transport task's own Async::TimeoutError is a transport failure, never
  # mistaken for the caller's timer, whatever its class.
  context "with a transport task's own Async::TimeoutError" do
    let(:owned) { Async::TimeoutError.new("owned task timed out") }

    def failure
      yield
      raise "Expected a transport failure"
    rescue StandardError => e
      e
    end

    it "fails gRPC stream cleanup whose cleanup task failed with it" do
      reactor do
        transport = ConnectRPC::Async::Transport.new(origin: "http://127.0.0.1:1")
        inject_stream_worker(transport, owned)
        error = failure { transport.open_grpc_stream(grpc_request) { :completed } }
        expect([error.class, error.message]).to eq([ConnectRPC::Error, "HTTP/2 transport cleanup failed"])
        expect(error.code).to eq("unavailable")
        expect(error.cause).to equal(owned)
        expect(transport.instance_variable_get(:@calls)).to be_empty
      ensure
        transport&.close(grace: 0)
      end
    end

    it "fails HTTP/1 cleanup whose cleanup task failed with it" do
      reactor do
        transport = ConnectRPC::Async::HTTP1Transport.new(origin: "http://127.0.0.1:1")
        failure = owned
        transport.define_singleton_method(:exchange) do |operation|
          worker = Async::Task.new(@lifetime, finished: false) { raise failure }
          operation.fetch(:tasks)[worker] = false
          worker.run
          ConnectRPC::TransportResponse.new(status: 200, headers: {}, body: "")
        end
        error = failure { transport.call(request("http://127.0.0.1:1/path")) }
        expect([error.class, error.message]).to eq([ConnectRPC::Error, "HTTP/1 transport cleanup failed"])
        expect(error.code).to eq("unavailable")
        expect(error.cause).to equal(owned)
        expect(transport.instance_variable_get(:@calls)).to be_empty
      ensure
        transport&.close(grace: 0)
      end
    end

    {
      "an HTTP/2 unary worker" => [ConnectRPC::Async::Transport, :exchange, "HTTP/2", :call],
      "an HTTP/2 stream worker" => [ConnectRPC::Async::Transport, :dispatch_stream, "HTTP/2", :stream],
      "an HTTP/1 worker" => [ConnectRPC::Async::HTTP1Transport, :dispatch, "HTTP/1", :call]
    }.each do |label, (kind, work, prefix, entry)|
      it "fails a call whose #{label} raised it" do
        reactor do
          transport = kind.new(origin: "http://127.0.0.1:1")
          failure = owned
          transport.define_singleton_method(work) { |*, **| raise failure }
          error = failure do
            if entry == :call
              transport.call(request("http://127.0.0.1:1/path"))
            else
              transport.open_stream(grpc_request) { :completed }
            end
          end
          expect([error.class, error.message]).to eq([ConnectRPC::Error, "#{prefix} transport failed: timed out"])
          expect(error.code).to eq("unavailable")
          expect(error.cause).to equal(owned)
          expect(transport.instance_variable_get(:@calls)).to be_empty
        ensure
          transport&.close(grace: 0)
        end
      end
    end
  end

  def detach_lifecycle(&detach)
    observer = Object.new
    observer.define_singleton_method(:attach) { nil }
    observer.define_singleton_method(:detach) { |_token| detach.call }
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
                               .start(owner: self, role: :client, method: nil, carrier: {})
  end

  def grpc_request
    ConnectRPC::StreamRequest.new(url: URI("http://127.0.0.1:1/path"), http_method: "POST", headers: {})
  end

  # Dispatch registers a worker that fails at once; the stream's cleanup joins it.
  def inject_stream_worker(transport, failure, resource: nil)
    transport.define_singleton_method(:dispatch_stream) do |operation, _body|
      stream = Struct.new(:state).new(:closed)
      operation[:response] = Struct.new(:stream) { def close = nil }.new(stream)
      operation[:resource] = resource if resource
      worker = Async::Task.new(@lifetime, finished: false) { raise failure }
      operation.fetch(:tasks)[worker] = true
      worker.run
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
