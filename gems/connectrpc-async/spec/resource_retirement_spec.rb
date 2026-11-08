# frozen_string_literal: true

RSpec.describe "HTTP/2 resource retirement completion" do
  def retirement_states(tasks)
    tasks.transform_values do |task|
      { id: task.object_id, status: task.status, alive: !task.alive?.nil?, finished: task.finished? }
    end
  end

  %i[resource_abort session_abort host_close].each do |operation|
    it "joins reader-initiated retirement before competing #{operation} returns" do
      reactor do |task|
        entered = Async::Queue.new
        cleanup_entered = Async::Queue.new
        release = Async::Queue.new
        released = Async::Queue.new
        cancellations = []
        owned = {}
        host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _context|
          first_cancel = nil
          begin
            entered.enqueue([Async::Task.current, message.name])
            Async::Condition.new.wait
          rescue Async::Cancel => e
            cancellations << e
            first_cancel = e
            raise
          ensure
            cleanup_entered.enqueue(true)
            loop do
              release.dequeue
              break
            rescue Async::Cancel => e
              # A second cancellation must not escape the held cleanup gate
              # or replace the original cancellation after release.
              cancellations << e
              first_cancel ||= e
            end
            released.enqueue(first_cancel)
            raise first_cancel if first_cancel
          end
        }), idle_timeout: nil).start
        session = host_session(host)
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        call = task.async(finished: false) do
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "retirement"), timeout: nil)
        rescue ConnectRPC::Error => e
          e
        end
        handler, name = entered.dequeue
        expect(name).to eq("retirement")
        resource = session.instance_variable_get(:@resources).first
        # Capture before abort can prune tracking or close stream ownership.
        wait_until(task, "handler and request worker tracked") do
          resource.tasks.include?(handler) && resource.tasks.any? { |entry| entry.parent.equal?(resource.task) }
        end
        owned = { serve: resource.task, handler: handler,
                  worker: resource.tasks.find { |entry| entry.parent.equal?(resource.task) },
                  reader: resource.connection.reader_task }
        tracked = resource.tasks.dup
        writers = resource.connection.writers.keys
        expect(owned.values).to all(be_a(Async::Task))
        expect(owned.values).not_to include(nil)
        expect(owned.values).to all(be_alive)
        puts "retirement #{operation} before-client-close #{retirement_states(owned).inspect}"

        retirement_callers = []
        trace = TracePoint.new(:call) do |event|
          retirement_callers << Async::Task.current if event.self.equal?(resource) && event.method_id == :abort
        end
        trace.enable
        client_close = task.async(finished: false) { transport.close(grace: 0) }
        cleanup_entered.dequeue
        expect(retirement_callers.first).to equal(owned.fetch(:reader))
        expect(resource.raw).to be_closed
        expect(handler).not_to be_finished

        started = Async::Queue.new
        competitor = task.async(finished: false) do |current|
          started.enqueue(current)
          case operation
          when :resource_abort then resource.abort
          when :session_abort then session.abort
          when :host_close then host.close(grace: 0)
          end
        end
        expect(started.dequeue).to equal(competitor)
        # Async starts a child through its first suspension before returning.
        # Cross another reactor turn after that operation-entry boundary while
        # handler cleanup remains blocked on the unreleased queue.
        task.yield
        held = retirement_states(owned.merge(competitor: competitor))
        puts "retirement #{operation} held #{held.inspect}; cancellations=#{cancellations.map(&:object_id)}"
        expect(handler).not_to be_finished
        expect(competitor).not_to be_finished,
                                  "#{operation} returned while handler cleanup was held: #{held.inspect}"
      ensure
        # This runs on the baseline assertion failure too. Join retained task
        # references before host cleanup can hide an incomplete resource abort.
        release&.enqueue(true)
        if owned && !owned.empty?
          task.with_timeout(2) do
            retained_cancel = released.dequeue
            [*owned.values, *tracked, *writers, competitor, client_close, call].compact.uniq.each(&:wait)
            wait_until(task, "retained retirement tasks finished") { owned.values.all?(&:finished?) }
            puts "retirement #{operation} after-release #{retirement_states(owned).inspect}; " \
                 "competitor=#{competitor&.status}/#{competitor&.result.inspect}; " \
                 "cancellations=#{cancellations.map(&:object_id)}; retained=#{retained_cancel&.object_id}"
            expect(retained_cancel).to equal(cancellations.first)
            expect(retained_cancel).to be_a(Async::Cancel)
            expect(handler).to be_cancelled
            expect(owned.values + tracked + writers).to all(be_finished)
            expect(owned.values).not_to include(have_attributes(status: :failed))
            expect(resource.raw).to be_closed
            expect(competitor.result).to be(true) if operation == :host_close && competitor
          end
        end
        trace&.disable
        transport&.close(grace: 0)
        if host
          outcome = host.close(grace: 0)
          puts "retirement #{operation} final-host-close=#{outcome.inspect}"
          # A resource-level abort already counts as drained for normal close.
          expect(outcome).to be(operation != :session_abort)
        end
      end
    end
  end
end
