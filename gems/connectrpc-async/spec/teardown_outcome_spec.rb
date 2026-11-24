# frozen_string_literal: true

require_relative "support/retirement_helpers"
require_relative "support/http1_client_helpers"

# A teardown step's failure is written as a fixed warning naming only its
# class. Every later step still runs, and the failure never replaces a close's
# drained or forced result (docs/design.md, rule 4).
RSpec.describe "Teardown failure outcomes" do
  include RetirementHelpers
  include HTTP1ClientHelpers

  def two_listeners(host)
    sockets = 2.times.flat_map { IO::Endpoint.tcp("127.0.0.1", 0).bound.sockets }
    bound = Struct.new(:sockets).new(sockets)
    host.define_singleton_method(:bind) { bound }
    sockets
  end

  def finalization(session)
    tls = session.instance_variable_get(:@tls)
    [session.instance_variable_get(:@state), tls.status[:state], tls.instance_variable_get(:@timer)&.finished?,
     session.instance_variable_get(:@lifetime).finished?]
  end

  it "drains a held RPC, closes TLS and the other listener, and returns true when a listener close fails" do
    reactor do |task|
      with_ca do |ca_file|
        entered = Async::Queue.new
        release = Async::Queue.new
        tls, settings = retirement_tls(:mtls_provider, ca_file)
        host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _context|
          entered.enqueue(Async::Task.current)
          release.dequeue
          message
        }), idle_timeout: nil, **tls)
        sockets = two_listeners(host)
        close = sockets.first.method(:close)
        sockets.first.define_singleton_method(:close) { raise Errno::EBADF, "listener close failed" }
        host.start
        listeners = host.instance_variable_get(:@listeners).dup
        session = host_session(host)
        origin = "https://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, **settings)
        call = task.async(finished: false) do
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
        end
        handler = entered.dequeue
        expect do
          closer = task.async(finished: false) { host.close(grace: 30) }
          wait_until(task, "the connection drain to start") do
            session.instance_variable_get(:@resources).first&.instance_variable_get(:@drain_task)
          end
          # The failed close skipped neither the other listener, TLS nor the drain.
          expect(sockets.last).to be_closed
          expect(listeners).to all(be_finished)
          expect(session.instance_variable_get(:@tls).status[:state]).to eq(:closed)
          expect(closer).not_to be_finished
          expect(session.instance_variable_get(:@close_decided)).to be_nil
          expect(handler).to be_alive
          release.enqueue(true)
          expect(call.wait.message.name).to eq("held")
          transport.close(grace: 0)
          expect(closer.wait).to be(true)
        end.to output(teardown_warnings(Errno::EBADF)).to_stderr
        expect(finalization(session)).to eq([:closed, :closed, true, true])
      ensure
        release&.enqueue(true)
        transport&.close(grace: 0)
        close&.call
        host&.close(grace: 0)
      end
    end
  end

  # Records whether `task` had finished when `raw` first closed.
  def observe_raw_close(raw, task)
    observed = []
    raw.define_singleton_method(:close) do
      observed << task.finished? if observed.empty?
      super()
    end
    observed
  end

  it "keeps the descriptor open for a held handler and completes retirement when the protocol close fails" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        session, resource, gate, handler = state.values_at(:session, :resource, :gate, :handler)
        raw = resource.raw
        closed_after_handler = observe_raw_close(raw, handler)
        resource.connection.define_singleton_method(:close!) { raise IOError, "protocol close failed" }
        expect do
          aborter = task.async(finished: false) { session.abort }
          waiter = task.async(finished: false) { state.fetch(:accepted).wait_closed }
          # The failed close still cancels the handler, whose unwind holds.
          wait_until(task, "the handler to hold its unwind or raw to close") { !gate.entered.empty? || raw.closed? }
          expect(raw).not_to be_closed
          expect(gate.entered.dequeue).to equal(handler)
          expect(handler).to be_alive
          expect(aborter).not_to be_finished
          gate.release
          join_retirement_tasks(task, aborter, waiter, handler)
          expect(aborter.result).to equal(session)
        end.to output(teardown_warnings(IOError)).to_stderr
        expect(closed_after_handler).to eq([true])
        expect(retirement_complete?(resource)).to be(true)
        expect(session.instance_variable_get(:@state)).to eq(:closed)
        expect(session.close(grace: 0)).to be(false)
      end
    end
  end

  it "reports failed final disposal steps and still closes raw and completes retirement" do
    reactor do |task|
      with_retirement_connection(task, mode: :tls) do |state|
        session, resource, gate, handler = state.values_at(:session, :resource, :gate, :handler)
        raw = resource.raw
        closed_after_handler = observe_raw_close(raw, handler)
        resource.connection.define_singleton_method(:finish_close) { |*| raise IOError, "protocol finish failed" }
        resource.socket.define_singleton_method(:close) { raise OpenSSL::SSL::SSLError, "TLS close failed" }
        expect do
          aborter = task.async(finished: false) { session.abort }
          expect(gate.entered.dequeue).to equal(handler)
          gate.release
          join_retirement_tasks(task, aborter, handler)
          expect(aborter.result).to equal(session)
        end.to output(teardown_warnings(IOError, OpenSSL::SSL::SSLError)).to_stderr
        expect(closed_after_handler).to eq([true])
        expect(raw).to be_closed
        expect(retirement_complete?(resource)).to be(true)
        expect(session.instance_variable_get(:@state)).to eq(:closed)
      end
    end
  end

  # The protocol and TLS closes fail without closing raw, so the final raw
  # close is the step that meets the fault.
  it "reports a failed final raw close and still completes retirement" do
    reactor do |task|
      with_retirement_connection(task, mode: :tls) do |state|
        session, resource, gate, handler = state.values_at(:session, :resource, :gate, :handler)
        raw = resource.raw
        closed_after_handler = fail_first_raw_close(raw, handler)
        resource.connection.define_singleton_method(:finish_close) { |*| raise IOError, "protocol finish failed" }
        resource.socket.define_singleton_method(:close) { raise OpenSSL::SSL::SSLError, "TLS close failed" }
        expect do
          aborter = task.async(finished: false) { session.abort }
          waiter = task.async(finished: false) { state.fetch(:accepted).wait_closed }
          expect(gate.entered.dequeue).to equal(handler)
          gate.release
          join_retirement_tasks(task, aborter, waiter, handler)
          expect(aborter.result).to equal(session)
        end.to output(teardown_warnings(IOError, OpenSSL::SSL::SSLError, RuntimeError)).to_stderr
        expect(closed_after_handler).to eq([true])
        expect(retirement_complete?(resource)).to be(true)
        expect(raw).to be_closed
      end
    end
  end

  describe "HTTP/1 client resource close" do
    # A stage whose close runs `action` and records each call.
    def stage(calls, name, &action)
      owned = Object.new
      owned.define_singleton_method(:close) do
        calls << name
        action&.call
      end
      owned
    end

    def resource_with(connection, socket)
      raw, peer = Socket.pair(:UNIX, :STREAM)
      resource = AsyncHelpers::INTERNAL.const_get(:HTTP1Resource).new
      resource.connection = connection
      resource.socket = socket
      resource.raw = raw
      [resource, peer]
    end

    it "raises the first failure, reports a later one and closes raw" do
      calls = []
      first = IOError.new("connection close failed")
      resource, peer = resource_with(stage(calls, :connection) { raise first },
                                     stage(calls, :socket) { raise OpenSSL::SSL::SSLError, "TLS close failed" })
      expect do
        expect { resource.close }.to(raise_error { |error| expect(error).to equal(first) })
      end.to output(teardown_warnings(OpenSSL::SSL::SSLError)).to_stderr
      expect(calls).to eq(%i[connection socket])
      expect(resource.raw).to be_closed
    ensure
      peer&.close
    end

    it "raises an interruption delivered after a failure, reports the failure and closes raw" do
      reactor do |task|
        calls = []
        entered = Async::Promise.new
        held = Async::Promise.new
        socket = stage(calls, :socket) do
          # The interrupted close is retried and then completes.
          next if entered.resolved?

          entered.resolve(true)
          held.wait
        end
        resource, peer = resource_with(stage(calls, :connection) { raise IOError, "connection close failed" }, socket)
        expect do
          closer = task.async(finished: false) do
            resource.close
          rescue Exception => e # rubocop:disable Lint/RescueException -- observe what close raises
            e
          end
          entered.wait
          closer.cancel
          expect(closer.wait).to be_a(Async::Cancel)
        end.to output(teardown_warnings(IOError)).to_stderr
        expect(calls).to eq(%i[connection socket socket])
        expect(resource.raw).to be_closed
      ensure
        peer&.close
      end
    end
  end

  it "keeps a unary H2 call's response when its cleanup and the connection's retirement both fail" do
    reactor do
      host, origin = retirement_host(server)
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      rpc = client(transport, origin)
      expect(rpc.echo(AsyncHelpers::MESSAGE.new(name: "warm")).message.name).to eq("warm")
      resource = transport.instance_variable_get(:@resources).first
      connection = resource.connection
      # The call's cleanup fails closing its response, so the call aborts the
      # connection, whose protocol close then fails too.
      connection.define_singleton_method(:create_response) do
        super().tap { |response| response.define_singleton_method(:close) { raise "response close failed" } }
      end
      connection.define_singleton_method(:close!) { raise IOError, "protocol close failed" }
      expect do
        expect(rpc.echo(AsyncHelpers::MESSAGE.new(name: "kept")).message.name).to eq("kept")
      end.to output(teardown_warnings(IOError)).to_stderr
      expect(retirement_complete?(resource)).to be(true)
      expect(resource.raw).to be_closed
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "returns false from a forced close and closes TLS when a TLS shutdown step fails" do
    reactor do |task|
      with_retirement_connection(task, mode: :mtls_provider) do |state|
        session, gate, resource = state.values_at(:session, :gate, :resource)
        tls = session.instance_variable_get(:@tls)
        timer = tls.instance_variable_get(:@timer)
        progress = tls.instance_variable_get(:@timer_progress)
        # The cancel itself fails; the timer leaves on its own once woken.
        timer.define_singleton_method(:cancel) do |*|
          progress.signal
          raise "timer cancel failed"
        end
        expect do
          closer = task.async(finished: false) { state.fetch(:host).close(grace: 0) }
          expect(gate.entered.dequeue).to equal(state.fetch(:handler))
          gate.release
          expect(closer.wait).to be(false)
        end.to output(teardown_warnings(RuntimeError)).to_stderr
        expect(finalization(session)).to eq([:closed, :closed, true, true])
        expect(retirement_complete?(resource)).to be(true)
        expect(resource.raw).to be_closed
      end
    end
  end

  it "returns false from an H2 client close whose connection manager close fails" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        transport, call = state.values_at(:transport, :call)
        resources = transport.instance_variable_get(:@resources).dup
        manager = transport.instance_variable_get(:@manager)
        failed = false
        manager.define_singleton_method(:close) do
          next super() if failed

          failed = true
          raise "manager close failed"
        end
        expect do
          expect(task.async(finished: false) { transport.close(grace: 30) }.wait).to be(false)
        end.to output(teardown_warnings(RuntimeError)).to_stderr
        expect(transport.instance_variable_get(:@state)).to eq(:closed)
        expect(resources).to all(satisfy { |resource| retirement_complete?(resource) && resource.raw.closed? })
        expect(call.wait).to be_a(ConnectRPC::Error)
        expect(transport.close(grace: 0)).to be(false)
      end
    end
  end

  it "returns false from a forced HTTP/1 client close whose connection close fails" do
    entered = Async::Queue.new
    peer = lambda do |native, *|
      native.read_request
      entered.enqueue(true)
      Async::Condition.new.wait
    end
    with_raw_http1(peer: peer, connection_limit: 1) do |transport, origin, accepted, task|
      call = task.async(finished: false) do
        client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
      rescue ConnectRPC::Error => e
        e
      end
      entered.dequeue
      resource = transport.instance_variable_get(:@leases).first
      resource.connection.define_singleton_method(:close) { raise IOError, "HTTP/1 connection close failed" }
      expect do
        expect(task.async(finished: false) { transport.close(grace: 0) }.wait).to be(false)
      end.to output(teardown_warnings(IOError)).to_stderr
      expect(transport.instance_variable_get(:@state)).to eq(:closed)
      expect(resource.raw).to be_closed
      expect(transport.instance_variable_get(:@leases)).to be_empty
      expect(call.wait).to be_a(ConnectRPC::Error)
      wait_until(task, "the peer to read end of stream") { accepted.first.read_nonblock(1, exception: false).nil? }
    end
  end

  it "finishes teardown when the warning cannot be written" do
    reactor do
      session = ConnectRPC::Async::ServerSession.new(application: ConnectRPC::Async::Application.new(server: server),
                                                     tls_provider: -> { host_tls.merge(version: "one") },
                                                     stop_accepting: -> { raise "listener cleanup failed" }).start
      writes = 0
      sink = Object.new
      sink.define_singleton_method(:write) do |*|
        writes += 1
        raise Errno::EPIPE, "stderr closed"
      end
      original = $stderr
      begin
        $stderr = sink
        expect(session.close(grace: 0)).to be(true)
      ensure
        $stderr = original
      end
      expect(writes).to eq(2)
      expect(finalization(session)).to eq([:closed, :closed, true, true])
    end
  end

  describe ConnectRPC::Async::TLSConfiguration do
    # The provider's refresh waits until shutdown cancels it, then holds its
    # unwind at `gate`.
    def held_configuration(task, gate)
      checks = 0
      configuration = described_class.new(on_error: ->(_) {}, tls_provider: lambda {
        checks += 1
        next host_tls.merge(version: "one") if checks == 1

        begin
          Async::Condition.new.wait
        ensure
          gate.hold
        end
      }).start
      refresh = task.async(finished: false) do
        configuration.refresh
      rescue described_class::Error => e
        e
      end
      wait_until(task, "the provider refresh to start") { configuration.instance_variable_get(:@callback) }
      [configuration, refresh]
    end

    # The first close hands shutdown to the guard, which cancels the provider
    # callback, and the interrupted caller is gone before that callback's held
    # unwind is released. A follower joins the same shutdown.
    def expect_follower_joins(task, configuration, gate, refresh)
      expect(configuration.status[:state]).to eq(:closing)
      follower = task.async(finished: false) { configuration.close }
      expect(follower).not_to be_finished
      gate.release
      expect(follower.wait).to equal(configuration)
      expect(configuration.status[:state]).to eq(:closed)
      expect(configuration.close).to equal(configuration)
      expect(refresh.wait).to be_a(described_class::Error)
    end

    it "raises at once into a cancelled close caller while a follower joins the held shutdown" do
      reactor do |task|
        gate = RetirementHelpers::CleanupGate.new
        configuration, refresh = held_configuration(task, gate)
        first = task.async(finished: false) { configuration.close }
        gate.entered.dequeue
        first.cancel
        expect(first).to be_cancelled
        expect_follower_joins(task, configuration, gate, refresh)
      ensure
        gate&.release
      end
    end

    it "raises a close caller's own timeout at its bound while the held shutdown continues" do
      reactor do |task|
        gate = RetirementHelpers::CleanupGate.new
        configuration, refresh = held_configuration(task, gate)
        # The timed-out caller runs in its own task, so a close that kept it
        # would fail the bounded wait below rather than hold this one.
        timed = task.async(finished: false) do |caller|
          caller.with_timeout(0.05) { configuration.close }
        rescue Async::TimeoutError => e
          e
        end
        gate.entered.dequeue
        wait_until(task, "the timed-out close caller to leave") { timed.finished? }
        expect(timed.result).to be_a(Async::TimeoutError)
        expect_follower_joins(task, configuration, gate, refresh)
      ensure
        gate&.release
      end
    end

    it "shuts down when its owner is cancelled" do
      reactor do |task|
        created = Async::Promise.new
        owner = task.async(finished: false) do
          created.resolve(described_class.new(tls_provider: -> { host_tls.merge(version: "one") },
                                              on_error: ->(_) {}).start)
          Async::Condition.new.wait
        end
        configuration = created.wait
        timer = configuration.instance_variable_get(:@timer)
        owner.cancel
        wait_until(task, "the owner to finish") { owner.finished? }
        expect(configuration.status[:state]).to eq(:closed)
        expect(timer).to be_finished
        expect(configuration.close).to equal(configuration)
      end
    end

    it "finishes a held shutdown for its closer when its owner is cancelled during it" do
      reactor do |task|
        gate = RetirementHelpers::CleanupGate.new
        created = Async::Promise.new
        owner = task.async(finished: false) do
          created.resolve(held_configuration(Async::Task.current, gate))
          Async::Condition.new.wait
        end
        configuration, refresh = created.wait
        closer = task.async(finished: false) { configuration.close }
        gate.entered.dequeue
        owner.cancel
        expect(closer).not_to be_finished
        expect(configuration.status[:state]).to eq(:closing)
        gate.release
        expect(closer.wait).to equal(configuration)
        expect(configuration.status[:state]).to eq(:closed)
        wait_until(task, "the owner to finish") { owner.finished? }
        expect(refresh).to be_finished
      ensure
        gate&.release
      end
    end
  end
end
