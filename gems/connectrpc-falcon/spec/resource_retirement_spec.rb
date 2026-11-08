# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/retirement_helpers"

RSpec.describe "Falcon shared retirement completion" do
  include RetirementHelpers

  def retirement_host(core, tls: nil, **)
    endpoint = ConnectRPC::Falcon::Endpoint.parse("#{tls ? 'https' : 'http'}://127.0.0.1:0")
    host = ConnectRPC::Falcon::Server.new(server: core, endpoint: endpoint, tls: tls, **)
    host.run
    port = host.instance_variable_get(:@owned_bound).sockets.first.local_address.ip_port
    [host, "#{tls ? 'https' : 'http'}://127.0.0.1:#{port}"]
  end

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
end
