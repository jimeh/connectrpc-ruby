# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/retirement_helpers"
require_relative "../../connectrpc-async/spec/support/retirement_corrections"
require_relative "../../connectrpc-async/spec/support/pending_close"

RSpec.describe "Falcon shared retirement completion" do
  include RetirementHelpers

  def retirement_host(core, tls: nil, tls_provider: nil, **)
    scheme = tls || tls_provider ? "https" : "http"
    endpoint = ConnectRPC::Falcon::Endpoint.parse("#{scheme}://127.0.0.1:0")
    host = ConnectRPC::Falcon::Server.new(server: core, endpoint: endpoint, tls: tls, tls_provider: tls_provider, **)
    host.run
    port = host.instance_variable_get(:@owned_bound).sockets.first.local_address.ip_port
    [host, "#{scheme}://127.0.0.1:#{port}"]
  end

  include_examples "retirement close corrections", :h1
  include_examples "retirement close corrections", :h2
  include_examples "owned pending close", :h1
  include_examples "owned pending close", :h2

  %i[h1 h2].product(%i[h2c tls mtls]).each do |protocol, mode|
    it "retains actual #{protocol} #{mode} connection metrics and competing close until processor cleanup" do
      reactor do |task|
        metrics = Async::Utilization::Registry.new
        with_retirement_connection(task, mode: mode, protocol: protocol, utilization_registry: metrics) do |state|
          resource, gate = state.values_at(:resource, :gate)
          aborter = task.async(finished: false) { resource.abort }
          gate.entered.dequeue
          expect(metrics.metric(:connections_active).value).to eq(1)
          expect(retirement_complete?(resource)).to be(false)
          expect(state.fetch(:session).instance_variable_get(:@identities)).to have_key(state.fetch(:handler))
          waiter = task.async(finished: false) { state.fetch(:accepted).wait_closed }
          closer = task.async(finished: false) { state.fetch(:host).close(grace: 0) }
          interrupted = task.async(finished: false) do
            resource.abort
          rescue Exception => e # rubocop:disable Lint/RescueException -- assert the local H1/H2 joiner's interruption
            e
          end
          first = Async::Cancel.new("first #{protocol} joiner interruption")
          Fiber.scheduler.raise(interrupted.fiber, first)
          Fiber.scheduler.raise(interrupted.fiber, RuntimeError.new("later interruption"))
          expect([aborter, waiter, closer, interrupted]).to all(satisfy { |entry| !entry.finished? })
          expect(metrics.metric(:connections_active).value).to eq(1)
          gate.release
          join_retirement_tasks(task, aborter, waiter, closer, interrupted, state.fetch(:handler))
          expect(closer.result).to be(true)
          expect(interrupted.result).to equal(first)
          expect(retirement_complete?(resource)).to be(true)
          expect(metrics.metric(:connections_active).value).to eq(0)
          expect(state.fetch(:session).instance_variable_get(:@identities)).to be_empty
        end
      end
    end
  end

  %i[h1 h2].each do |protocol|
    it "allows an owned #{protocol} processor to reenter session abort without waiting on itself" do
      reactor do |task|
        with_retirement_connection(task, protocol: protocol) do |state|
          state[:on_cleanup] = -> { state.fetch(:session).abort }
          state.fetch(:commands).enqueue(->(_) { state.fetch(:session).abort })
          state.fetch(:gate).entered.dequeue
          aborter = task.async(finished: false) { state.fetch(:session).abort }
          expect(aborter).not_to be_finished
          expect(state.fetch(:session).instance_variable_get(:@admitted)).not_to be_empty
          state.fetch(:gate).release
          join_retirement_tasks(task, aborter, state.fetch(:handler))
          expect(state.fetch(:host).close(grace: 0)).to be(false)
          expect(state.fetch(:session).instance_variable_get(:@admitted)).to be_empty
        end
      end
    end
  end

  # Session abort fails a plaintext peer whose protocol is still undetected,
  # so its setup reads end of stream and would choose HTTP/1. Once the abort
  # has started, setup keeps the original resource, so the accepted handle,
  # the session's mapping and retirement all name one retired resource.
  it "never maps an HTTP/1 replacement that session abort cannot retire" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      port = bound.sockets.first.local_address.ip_port
      session = ConnectRPC::Async::ServerSession.new(
        application: ConnectRPC::Async::Application.new(server: server), alpn_protocols: %w[h2 http/1.1],
        http1: ConnectRPC::Falcon.const_get(:HTTP1Resource).method(:new)
      ).start
      peer = IO::Endpoint.tcp("127.0.0.1", port).connect
      raw, = bound.sockets.first.accept
      handle = session.accept(raw)
      original = session.instance_variable_get(:@resources).first
      serving = task.async(finished: false) { handle.serve }
      wait_until(task, "protocol detection reading the peer") do
        original.tasks.any? { |setup| !setup.finished? }
      end
      aborter = task.async(finished: false) { session.abort }
      join_retirement_tasks(task, aborter, serving)
      mapped = session.instance_variable_get(:@resources)
      current = handle.instance_variable_get(:@resource)
      expect([current, *mapped]).to all(satisfy { |resource| retirement_complete?(resource) })
      expect(mapped - [current]).to be_empty
      task.with_timeout(2) { handle.wait_closed }
      expect(raw).to be_closed
      expect(session.instance_variable_get(:@state)).to eq(:closed)
    ensure
      peer&.close
      bound&.close
    end
  end

  # Listener cleanup yields before the abort pass reaches the original
  # resource, so its setup finishes first, having refused the replacement.
  # Serving then stops instead of starting work on a resource with no
  # connection.
  it "stops serving a resource whose HTTP/1 replacement session abort refused" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      port = bound.sockets.first.local_address.ip_port
      boundary = Async::Queue.new
      release = Async::Queue.new
      session = ConnectRPC::Async::ServerSession.new(
        application: ConnectRPC::Async::Application.new(server: server), alpn_protocols: %w[h2 http/1.1],
        http1: ConnectRPC::Falcon.const_get(:HTTP1Resource).method(:new),
        stop_accepting: lambda {
          boundary.enqueue(true)
          release.dequeue
        }
      ).start
      peer = IO::Endpoint.tcp("127.0.0.1", port).connect
      raw, = bound.sockets.first.accept
      handle = session.accept(raw)
      original = session.instance_variable_get(:@resources).first
      serving = task.async(finished: false) { handle.serve }
      wait_until(task, "protocol detection reading the peer") do
        original.tasks.any? { |setup| !setup.finished? }
      end
      aborter = task.async(finished: false) { session.abort }
      task.with_timeout(2) { boundary.dequeue }
      # The abort failed raw first, so detection read end of stream and chose
      # HTTP/1 while listener cleanup is still held.
      task.with_timeout(2) { serving.wait }
      expect(original.ready).not_to be(true)
      expect(session.instance_variable_get(:@resources)).to eq([original])
      expect(handle.instance_variable_get(:@resource)).to equal(original)
      release.enqueue(true)
      join_retirement_tasks(task, aborter)
      expect(retirement_complete?(original)).to be(true)
      expect(session.instance_variable_get(:@state)).to eq(:closed)
    ensure
      release&.enqueue(true)
      peer&.close
      bound&.close
    end
  end
end
