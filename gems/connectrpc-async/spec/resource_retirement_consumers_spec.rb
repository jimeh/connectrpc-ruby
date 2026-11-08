# frozen_string_literal: true

require_relative "support/retirement_helpers"

module RetirementOutputFixture
  class Body < Protocol::HTTP::Body::Readable
    attr_reader :started, :gate, :child_gate, :child
    attr_accessor :resource

    def initialize(transient:)
      super()
      @transient = transient
      @started = Async::Queue.new
      @gate = RetirementHelpers::CleanupGate.new
      @child_gate = RetirementHelpers::CleanupGate.new
    end

    def read
      Async::Task.current.transient = @transient
      @started.enqueue(Async::Task.current)
      Async::Condition.new.wait
    end

    def close(error = nil)
      # Exercise real output unwind and reentry, including a child created only
      # after cancellation. Neither task is a substitute for a lifecycle owner.
      @resource.abort
      @child = Async::Task.current.async(finished: false) { @child_gate.hold }
      @gate.hold(error)
    end

    def release
      @gate.release
      @child_gate.release
    end
  end
end

RSpec.describe "Shared retirement output and client consumers" do
  include RetirementHelpers

  it "leaves a joined task's stored failure with its supervisor while retirement joins its child" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        failure = RuntimeError.new("original owned task failure")
        child_gate = RetirementHelpers::CleanupGate.new
        release_failure = Async::Queue.new
        resource = state.fetch(:resource)
        root = Async::Task.new(state.fetch(:session).instance_variable_get(:@lifetime), finished: false) do |current|
          current.async(finished: false) { child_gate.hold }
          release_failure.dequeue
          raise failure
        end
        resource.track(root)
        root.run
        child = child_gate.entered.dequeue
        supervisor = task.async(finished: false) do
          root.wait
        rescue RuntimeError => e
          e
        end
        release_failure.enqueue(true)
        expect(supervisor.wait).to equal(failure)
        expect(root).to be_failed
        expect(root).not_to be_finished
        aborter = task.async(finished: false) { resource.abort }
        state.fetch(:gate).entered.dequeue
        state.fetch(:gate).release
        state.fetch(:handler).wait
        task.yield
        expect(aborter).not_to be_finished
        expect(retirement_complete?(resource)).to be(false)
        child_gate.release
        join_retirement_tasks(task, aborter, child)
        expect(root).to be_finished
        expect(root.result).to equal(failure)
        expect(aborter.result).to be_nil
        expect(retirement_complete?(resource)).to be(true)
      ensure
        child_gate&.release
        release_failure&.enqueue(true)
      end
    end
  end

  [false, true].each do |transient|
    it "joins actual #{transient ? 'transient' : 'ordinary'} response output and its cleanup child before disposal" do
      reactor do |task|
        body = RetirementOutputFixture::Body.new(transient: transient)
        application = ->(_) { Protocol::HTTP::Response[200, {}, body] }
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        listener = bound.sockets.first
        session = ConnectRPC::Async::ServerSession.new(application: application, idle_timeout: nil).start
        handles = Async::Queue.new
        acceptor = task.async(finished: false) do
          raw, = listener.accept
          handle = session.accept(raw)
          handles.enqueue(handle)
          session.schedule { handle.serve }
        end
        origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        call = task.async(finished: false) do
          transport.call(request("#{origin}/output"))
        rescue ConnectRPC::Error => e
          e
        end
        output = body.started.dequeue
        resource = session.instance_variable_get(:@resources).first
        body.resource = resource
        stream = resource.connection.streams.values.first
        expect(stream.response_task).to equal(output)
        expect(output.transient?).to be(transient)
        first_error = IOError.new("first retirement error")
        disposals = []
        cancellations = []
        trace = TracePoint.new(:call) do |event|
          if event.self.equal?(resource.connection) && event.method_id == :finish_close
            disposals << event.binding.local_variable_get(:error)
          elsif event.self.equal?(output) && event.method_id == :cancel
            cancellations << output.finished?
          end
        end
        trace.enable
        aborter = task.async(finished: false) { resource.abort(first_error) }
        body.gate.entered.dequeue
        child = body.child_gate.entered.dequeue
        competitor = task.async(finished: false) { resource.abort(IOError.new("later error")) }
        waiter = task.async(finished: false) { handles.dequeue.wait_closed }
        expect([aborter, competitor, waiter, output, child]).to all(satisfy { |entry| !entry.finished? })
        expect(disposals).to be_empty
        body.gate.release
        output.wait
        # The output promise has resolved, but its actual cleanup child has not.
        task.yield
        expect(child).not_to be_finished
        expect(aborter).not_to be_finished
        expect(retirement_complete?(resource)).to be(false)
        expect(disposals).to be_empty
        body.child_gate.release
        join_retirement_tasks(task, aborter, competitor, waiter, output, child, resource.task)
        expect(disposals).to eq([first_error])
        expect(disposals.first).to equal(first_error)
        # Pinned protocol disposal also stops the already-finished output.
        # Count cancellation of live work, including ordinary parent cascade.
        expect(cancellations.count(false)).to eq(1)
        expect(retirement_complete?(resource)).to be(true)
        expect(call.wait).to be_a(ConnectRPC::Error)
      ensure
        trace&.disable
        body&.release
        transport&.close(grace: 0)
        session&.abort
        acceptor&.wait
        bound&.close
      end
    end
  end

  it "retains an H2 client resource across a subsequent connect and close without retaining its pool slot" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        transport = state.fetch(:transport)
        resource = transport.instance_variable_get(:@resources).first
        state.fetch(:commands).enqueue(->(_) {})
        state.fetch(:gate).entered.dequeue
        state.fetch(:gate).release
        expect(state.fetch(:call).wait.message.name).to eq("held")
        lifetime = transport.instance_variable_get(:@lifetime)
        writer_gate = RetirementHelpers::CleanupGate.new
        writer_entered = Async::Queue.new
        writer = lifetime.async(finished: false) do
          resource.connection.synchronize do
            writer_entered.enqueue(true)
            Async::Condition.new.wait
          end
        ensure
          writer_gate.hold($ERROR_INFO)
        end
        writer_entered.dequeue
        pool = transport.instance_variable_get(:@pool)
        expect(pool.resources.values).to eq([0])
        aborter = task.async(finished: false) { pool.retire(resource.connection) }
        writer_gate.entered.dequeue
        # Existing Pool#retire deletes the slot before its close can join. A
        # later RPC can connect even while the old resource's writer unwinds.
        wait_until(task, "old pool slot retired") { pool.resources.empty? }
        expect(aborter).not_to be_finished
        fresh = client(transport, state.fetch(:origin)).echo(AsyncHelpers::MESSAGE.new(name: "fresh"))
        expect(fresh.message.name).to eq("fresh")
        expect(pool.resources.size).to eq(1)
        expect(pool.resources.values.sum).to eq(0)
        expect(transport.instance_variable_get(:@resources)).to include(resource)
        expect(retirement_complete?(resource)).to be(false)
        closer = task.async(finished: false) { transport.close(grace: 0) }
        task.yield
        expect(closer).not_to be_finished
        expect(writer).not_to be_finished
        writer_gate.release
        join_retirement_tasks(task, aborter, closer, writer, resource.connection.reader_task, state.fetch(:call))
        expect(retirement_complete?(resource)).to be(true)
        expect(closer.result).to be(false)
        expect(pool.resources).to be_empty
        expect(transport.instance_variable_get(:@calls)).to be_empty
      ensure
        writer_gate&.release
      end
    end
  end
end
