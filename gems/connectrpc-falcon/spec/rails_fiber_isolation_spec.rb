# frozen_string_literal: true

RSpec.describe "Falcon Rails fallback under fiber isolation" do
  before(:context) do
    require_relative "support/rails_fiber_app"
    @isolation_level = ActiveSupport::IsolatedExecutionState.isolation_level
    ActiveSupport::IsolatedExecutionState.isolation_level = :fiber
  end

  after(:context) do
    ActiveRecord::Base.connection_handler.clear_all_connections!
    ActiveSupport::IsolatedExecutionState.isolation_level = @isolation_level if @isolation_level
  end

  before do
    ActiveRecord::Base.connection_pool.disconnect!
    FalconRailsFixture.calls.clear
    FalconRailsFixture.resets.clear
  end

  after { FalconRailsFixture.on_complete = nil }

  %i[http1 h2].each do |protocol|
    it "returns the connection and resets Rails state on each request's fiber over #{protocol}" do
      reactor do |task|
        completions = Async::Queue.new
        FalconRailsFixture.on_complete = ->(record) { completions.enqueue(record) }
        expect(ActiveRecord::Base.connection_pool.reaper.frequency).to eq(0)
        falcon(rack_app: FalconRailsFixture::Application) do |_host, origin|
          finished = Array.new(3) do |index|
            path = "/probe/#{index}"
            response = protocol == :h2 ? h2(origin, path) : http1(origin, path, body: nil)
            expect(response.values_at(0, 2)).to eq([200, "request-#{index}"])
            task.with_timeout(2) { completions.dequeue }
          end
          fibers = FalconRailsFixture.calls.map(&:first)
          expect(fibers.uniq.size).to eq(3)
          expect(FalconRailsFixture.calls.map(&:last)).to eq([true] * 3)
          # The executor completes on the fiber that ran the app, after
          # query-cache teardown and CurrentAttributes.clear_all there.
          expect(finished.map(&:first)).to eq(fibers)
          expect(finished.map { |record| record.drop(1) }).to eq([[false, nil]] * 3)
          values = Array.new(3) { |index| "request-#{index}" }
          expect(FalconRailsFixture.resets).to eq(fibers.zip(values))
          expect(ActiveRecord::Base.connection_pool.stat.slice(:connections, :busy, :dead, :idle))
            .to eq(connections: 1, busy: 0, dead: 0, idle: 1)
        end
      end
    end
  end
end
