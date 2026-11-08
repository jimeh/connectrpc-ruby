# frozen_string_literal: true

require_relative "retirement_helpers"

RSpec.shared_examples "owned pending close" do |protocol|
  include RetirementHelpers

  %i[h2c mtls].product(%i[normal expired abort interrupted]).each do |mode, outcome|
    it "returns nil for owned #{protocol} #{mode} positive grace until actual #{outcome} completion" do
      reactor do |task|
        with_ca do |ca_file|
          entered = Async::Queue.new
          requested = Async::Queue.new
          commands = Async::Queue.new
          progressed = Async::Queue.new
          gate = RetirementHelpers::CleanupGate.new
          host = nil
          grace = outcome == :expired ? 0.5 : 5
          core = server(echo: lambda { |message, _|
            if message.name == "sibling"
              begin
                entered.enqueue(Async::Task.current)
                while (command = commands.dequeue)
                  command.call
                  progressed.enqueue(true)
                end
              ensure
                gate.hold($ERROR_INFO)
              end
            else
              requested.enqueue([Async::Task.current, host.close(grace: grace)])
            end
            message
          })
          tls, settings = retirement_tls(mode, ca_file)
          host, origin = retirement_host(core, tls: tls, idle_timeout: nil)
          origin = origin.sub("http:", "https:") if tls
          session = host.instance_variable_get(:@session)
          closed_reentries = []
          allow(session).to receive(:finish_closed).and_wrap_original do |finish, value|
            finish.call(value)
            closed_reentries << [session.close(grace: 0), host.close(grace: 0)]
          end
          expiries = []
          allow(session).to receive(:drain_resources).and_wrap_original do |drain, resources, expires|
            expiries << expires
            drain.call(resources, expires)
          end
          type = protocol == :h1 ? ConnectRPC::Async::HTTP1Transport : ConnectRPC::Async::Transport
          sibling_transport = type.new(origin: origin, **settings)
          initiating_transport = protocol == :h1 ? type.new(origin: origin, **settings) : sibling_transport
          sibling_call = task.async(finished: false) do
            client(sibling_transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "sibling"), timeout: nil)
          rescue ConnectRPC::Error => e
            e
          ensure
            sibling_transport.close(grace: 0)
          end
          sibling = entered.dequeue
          initiating_call = task.async(finished: false) do
            client(initiating_transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "initiate"), timeout: nil)
          ensure
            initiating_transport.close(grace: 0) if protocol == :h1
          end
          initiator, result = requested.dequeue
          expect(result).to be_nil
          expect(initiating_call.wait.message.name).to eq("initiate")
          expect(initiator).to be_finished
          expect(sibling).to be_alive
          expect(session.instance_variable_get(:@admitted)).not_to be_empty
          expect(session.instance_variable_get(:@close_decided)).to be_nil
          closer = session.instance_variable_get(:@closer)
          expect(expiries.size).to eq(1)
          first_expiry = expiries.first
          commands.enqueue(lambda {
            expect(host.close(grace: 3600)).to be_nil
            expect(session.close(grace: 0)).to be_nil
            expect(session.instance_variable_get(:@closer)).to equal(closer)
          })
          progressed.dequeue
          expect(expiries).to eq([first_expiry])
          expect(session.instance_variable_get(:@close_decided)).to be_nil
          first = RuntimeError.new("owned-initiated close external interruption")
          external_started = Async::Queue.new
          external = task.async(finished: false) do
            external_started.enqueue(true)
            host.close(grace: 3600)
          rescue Exception => e # rubocop:disable Lint/RescueException -- retain the caller's exact interruption
            e
          end
          external_started.dequeue
          commands.enqueue(-> { expect(sibling).to be_alive })
          progressed.dequeue
          expect(external).not_to be_finished
          expect(closer).not_to be_finished
          resources = session.instance_variable_get(:@resources).dup
          sibling_resource = resources.find { |resource| resource.send(:owns_task?, sibling) }
          expect(sibling_resource).not_to be_nil
          expect(retirement_complete?(sibling_resource)).to be(false)
          # Reentry during actual retirement must remain nil even after the
          # forced outcome is decided but before cleanup is released.
          reentries = []
          allow(gate).to receive(:hold).and_wrap_original do |hold, error|
            reentries << session.close(grace: 0)
            hold.call(error)
          end
          case outcome
          when :normal then commands.enqueue(nil)
          when :abort then aborter = task.async(finished: false) { session.abort }
          when :interrupted then Fiber.scheduler.raise(external.fiber, first)
          end
          expect(gate.entered.dequeue).to equal(sibling)
          expect(reentries).to eq([nil])
          expect(external).not_to be_finished
          expect(session.instance_variable_get(:@state)).to eq(:closing)
          expect(resources.any? { |resource| !retirement_complete?(resource) }).to be(true)
          gate.release
          join_retirement_tasks(task, sibling_call, initiating_call, external, aborter, sibling, initiator, closer)
          final = outcome == :normal
          expect(external.result).to equal(outcome == :interrupted ? first : final)
          expect(sibling_call.result.message.name).to eq("sibling") if final
          expect(sibling_call.result).to be_a(ConnectRPC::Error) unless final
          expect(resources).to all(satisfy { |resource| retirement_complete?(resource) && resource.raw.closed? })
          expect(resources.map { |resource| resource.instance_variable_get(:@cleanup) }).to all(be_finished)
          expect(session.instance_variable_get(:@admitted)).to be_empty
          expect(session.instance_variable_get(:@identities)).to be_empty
          expect(host.close(grace: 0)).to be(final)
          expect(closed_reentries).to eq([[final, final]])
          puts "pending #{protocol}/#{mode}/#{outcome}: initiator_finished=true sibling_progress=true " \
               "expiries=#{expiries.size} final=#{final} resources_completed=#{resources.size}"
        ensure
          commands&.enqueue(nil)
          gate&.release
          session&.abort
          initiating_transport&.close(grace: 0)
          sibling_transport&.close(grace: 0)
          join_retirement_tasks(task, sibling_call, initiating_call, external, aborter)
          host&.instance_variable_get(:@owned_bound)&.close
        end
      end
    end
  end
end
