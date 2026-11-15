# frozen_string_literal: true

# Internal yield windows between connection bookkeeping steps. A blocking
# observer holds the one step that may yield; the contract asks observers to
# return quickly, so this is a source-coupled fixture for that window.
RSpec.describe "HTTP/2 connection manager yield windows" do
  def blocking_observer(entered, release, &selected)
    armed = true
    lambda do |event|
      next unless armed && selected.call(event)

      armed = false
      entered.enqueue(event)
      release.dequeue
    end
  end

  it "completes a started retirement when the aborting fiber is cancelled during the manager notification" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      entered = Async::Queue.new
      release = Async::Queue.new
      armed = false
      observer = blocking_observer(entered, release) { |event| armed && event.type == :state_changed }
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer)
      expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
      resource = transport.instance_variable_get(:@resources).first
      armed = true
      aborter = task.async(finished: false) { resource.abort }
      expect(entered.dequeue.state).to eq(:idle)
      aborter.cancel
      expect { aborter.wait }.not_to raise_error
      expect(aborter).to be_cancelled
      wait_until(task, "retirement completed") { resource.send(:retirement_completed?) }
      expect(resource.raw).to be_closed
      # A later abort joins the completed retirement instead of an unstarted task.
      expect(task.async(finished: false) { resource.abort }.wait).to be_nil
      expect(connection_manager(transport).connections).to be_empty
      expect(transport.call(request("#{origin}/after")).body).to eq("ok")
      expect(peer.accepted).to eq(2)
    ensure
      release&.enqueue(true)
      # Without the correction the retirement task is never started, and close
      # would join it forever. Start it here so that failure stays bounded.
      cleanup = resource&.instance_variable_get(:@cleanup)
      cleanup.run if cleanup&.status == :initialized
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "cancels an attempt when close runs during its connecting publication" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      entered = Async::Queue.new
      release = Async::Queue.new
      events = []
      held = blocking_observer(entered, release) { |event| event.state == :connecting }
      observer = lambda do |event|
        events << event
        held.call(event)
      end
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer)
      manager = connection_manager(transport)
      acquirer = task.async(finished: false) { manager.acquire }
      entered.dequeue
      attempt = manager.instance_variable_get(:@attempt)
      transport.close(grace: 0)
      release.enqueue(true)
      expect { acquirer.wait }.to unavailable_error
      # No late dial reached the listener after close.
      expect(listener.accept_nonblock(exception: false)).to eq(:wait_readable)
      expect(attempt.task).to be_finished
      expect(attempt_outcomes(events)).to be_empty
      expect(manager.instance_variable_get(:@attempt)).to be_nil
      expect(manager.connections).to be_empty
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      bound&.close
    end
  end
end
