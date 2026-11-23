# frozen_string_literal: true

require "spec_helper"

# An interrupted close caller leaves at once. The session owner's closer keeps
# draining under the original grace, and only owner cancellation or a later
# zero-grace close forces it.
RSpec.describe "Interrupted Host close" do
  def with_held_call(task)
    entered = Async::Queue.new
    release = Async::Queue.new
    host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _context|
      entered.enqueue(Async::Task.current)
      release.dequeue
      message
    }), idle_timeout: nil).start
    origin = "http://127.0.0.1:#{host.port}"
    transport = ConnectRPC::Async::Transport.new(origin: origin)
    call = task.async(finished: false) do
      client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
    rescue ConnectRPC::Error => e
      e
    end
    # The RPC is on the wire and admitted before any close starts.
    handler = entered.dequeue
    yield host, handler, call, release, transport
  ensure
    release&.enqueue(true)
    transport&.close(grace: 0)
    host&.close(grace: 0)
  end

  def drain_started?(host)
    host_session(host).instance_variable_get(:@resources).first&.instance_variable_get(:@drain_task)
  end

  def expect_drain_pending(host, handler)
    session = host_session(host)
    expect(session.instance_variable_get(:@state)).to eq(:closing)
    expect(session.instance_variable_get(:@close_decided)).to be_nil
    expect(handler).to be_alive
  end

  # The drain finishes because its admitted RPC does, not because grace ran
  # out, and a later close joins it for the drained result.
  def expect_graceful_finish(host, call, release, transport)
    follower = Async::Task.current.async(finished: false) { host.close(grace: 30) }
    expect(follower).not_to be_finished
    release.enqueue(true)
    expect(call.wait.message.name).to eq("held")
    transport.close(grace: 0)
    expect(follower.wait).to be(true)
    expect(host_session(host).instance_variable_get(:@state)).to eq(:closed)
    expect(host.close(grace: 0)).to be(true)
  end

  it "raises an enclosing with_timeout at its own bound and keeps draining under the close grace" do
    reactor do |task|
      with_held_call(task) do |host, handler, call, release, transport|
        expect { task.with_timeout(0.05) { host.close(grace: 30) } }.to raise_error(Async::TimeoutError)
        expect_drain_pending(host, handler)
        wait_until(task, "the connection drain to start") { drain_started?(host) }
        expect_drain_pending(host, handler)
        expect_graceful_finish(host, call, release, transport)
      end
    end
  end

  it "lets a cancelled close caller finish at once and keeps draining under the close grace" do
    reactor do |task|
      with_held_call(task) do |host, handler, call, release, transport|
        closer = task.async(finished: false) { host.close(grace: 30) }
        wait_until(task, "the connection drain to start") { drain_started?(host) }
        closer.cancel
        expect(closer).to be_cancelled
        expect_drain_pending(host, handler)
        expect_graceful_finish(host, call, release, transport)
      end
    end
  end

  # With every close caller gone, the drain still ends at its own grace.
  # Elapsed grace is the behavior here, so the example uses a short one.
  it "forces the drain at its original grace after every close caller left" do
    reactor do |task|
      with_held_call(task) do |host, handler, call, _release, _transport|
        expect { task.with_timeout(0.01) { host.close(grace: 0.2) } }.to raise_error(Async::TimeoutError)
        expect_drain_pending(host, handler)
        session = host_session(host)
        AsyncHelpers::INTERNAL.join_complete(session.instance_variable_get(:@closer))
        expect(session.instance_variable_get(:@state)).to eq(:closed)
        expect(session.instance_variable_get(:@close_outcome)).to be(false)
        expect(handler).to be_finished
        expect(call.wait).to be_a(ConnectRPC::Error)
      end
    end
  end

  # The closer is a child of the session owner, so cancelling the owner
  # cancels it too. Its ensure, like the owner's lifetime guard, then forces
  # raw-first retirement and joins it. The owner's task tree finishes only
  # after that, though Task#wait on the cancelled owner returns earlier.
  it "forces the drain when the session owner is cancelled during a graceful close" do
    reactor do |task|
      ready = Async::Queue.new
      entered = Async::Queue.new
      go = Async::Queue.new
      owner = task.async(finished: false) do
        host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _context|
          entered.enqueue(Async::Task.current)
          Async::Queue.new.dequeue
          message
        }), idle_timeout: nil).start
        ready.enqueue(host)
        go.dequeue
        host.close(grace: 30)
      end
      host = ready.dequeue
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      call = task.async(finished: false) do
        client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
      rescue ConnectRPC::Error => e
        e
      end
      handler = entered.dequeue
      go.enqueue(true)
      wait_until(task, "the connection drain to start") { drain_started?(host) }
      session = host_session(host)
      closer = session.instance_variable_get(:@closer)
      resource = session.instance_variable_get(:@resources).first
      owner.cancel
      expect(owner).to be_cancelled
      expect(owner).not_to be_finished
      expect(resource.send(:retirement_completed?)).to be(false)
      AsyncHelpers::INTERNAL.join_complete(closer)
      expect(session.instance_variable_get(:@state)).to eq(:closed)
      expect(session.instance_variable_get(:@close_outcome)).to be(false)
      expect(resource.send(:retirement_completed?)).to be(true)
      expect(handler).to be_finished
      expect(call.wait).to be_a(ConnectRPC::Error)
      AsyncHelpers::INTERNAL.join_complete(owner)
      expect(owner).to be_finished
    ensure
      transport&.close(grace: 0)
      AsyncHelpers::INTERNAL.cancel(owner)
    end
  end
end
