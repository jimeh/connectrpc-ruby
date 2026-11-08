# frozen_string_literal: true

require_relative "support/retirement_helpers"

module EarlyRetirementOutput
  class Body < Protocol::HTTP::Body::Readable
    attr_accessor :session
    attr_reader :started, :gate, :child_gate, :child, :publication, :resource, :error

    def initialize
      super
      @started = Async::Queue.new
      @gate = RetirementHelpers::CleanupGate.new
      @child_gate = RetirementHelpers::CleanupGate.new
      @error = IOError.new("early output retirement")
    end

    def read
      current = Async::Task.current
      current.transient = true
      @resource = @session.instance_variable_get(:@resources).first
      @publication = @resource.connection.streams.values.first.response_task
      @started.enqueue(current)
      # Initiate before the first suspension or return from upstream Output#start.
      @resource.abort(@error)
      nil
    ensure
      @child = current.async(finished: false) { @child_gate.hold }
      @gate.hold($ERROR_INFO)
    end

    def close(*) = nil

    def release
      @gate.release
      @child_gate.release
    end
  end
end

RSpec.describe "Early transient output retirement publication" do
  include RetirementHelpers

  it "joins output initiating abort in its first transient body read before eager start returns" do
    reactor do |task|
      body = EarlyRetirementOutput::Body.new
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      session = ConnectRPC::Async::ServerSession.new(
        application: ->(_) { Protocol::HTTP::Response[200, {}, body] }, idle_timeout: nil
      ).start
      body.session = session
      disposals = []
      trace = TracePoint.new(:call) do |event|
        if event.method_id == :finish_close && event.self.equal?(body.resource&.connection)
          disposals << event.binding.local_variable_get(:error)
        end
      end
      trace.enable
      acceptor = task.async(finished: false) do
        raw, = listener.accept
        accepted = session.accept(raw)
        session.schedule { accepted.serve }
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      call = task.async(finished: false) do
        transport.call(request("#{origin}/early-output"))
      rescue ConnectRPC::Error => e
        e
      end
      output = task.with_timeout(2) { body.started.dequeue }
      resource = session.instance_variable_get(:@resources).first
      expect(body.publication).to equal(output)
      competitor = task.async(finished: false) { resource.abort }
      begin
        child = task.with_timeout(2) { body.child_gate.entered.dequeue }
      rescue Async::TimeoutError
        $stdout.puts "early-output timeout published=#{body.publication&.object_id.inspect} " \
                     "members=#{resource.instance_variable_get(:@retirement_members).keys.map(&:object_id)}"
        [output, resource.task, resource.instance_variable_get(:@cleanup), competitor].each do |entry|
          $stdout.puts "task=#{entry.object_id} parent=#{entry.parent&.object_id} transient=#{entry.transient?} " \
                       "finished=#{entry.finished?}\n#{entry.fiber&.backtrace&.join("\n")}"
        end
        $stdout.flush
        raise
      end
      task.with_timeout(2) { body.gate.entered.dequeue }
      puts "early-output output=#{output.object_id} child=#{child.object_id} " \
           "published=#{body.publication&.object_id.inspect} " \
           "cleanup=#{resource.instance_variable_get(:@cleanup).object_id} " \
           "completed=#{retirement_complete?(resource)} competitor-finished=#{competitor.finished?}"
      expect(retirement_complete?(resource)).to be(false), "unpublished transient output still owns held cleanup"
      expect(competitor).not_to be_finished
      expect(disposals).to be_empty
      body.gate.release
      output.wait
      task.yield
      expect(retirement_complete?(resource)).to be(false)
      expect(disposals).to be_empty
      body.child_gate.release
      join_retirement_tasks(task, competitor, output, child, resource.task, resource.connection.reader_task)
      expect(retirement_complete?(resource)).to be(true)
      expect(disposals).to eq([body.error])
      expect(disposals.first).to equal(body.error)
      expect(call.wait).to be_a(ConnectRPC::Error)
    rescue Async::TimeoutError
      ObjectSpace.each_object(Async::Task) do |entry|
        next unless entry.alive?

        $stdout.puts "early-output live task=#{entry.object_id} parent=#{entry.parent&.object_id} " \
                     "transient=#{entry.transient?}\n#{entry.fiber&.backtrace&.join("\n")}"
      end
      $stdout.flush
      raise
    ensure
      trace&.disable
      body&.release
      # Explicitly unblock the fixture's early output if retirement missed it.
      output&.cancel unless output&.finished?
      transport&.close(grace: 0)
      session&.abort
      task.with_timeout(2) { [acceptor, output, body&.child, call].compact.each(&:wait) }
      bound&.close
    end
  end
end
