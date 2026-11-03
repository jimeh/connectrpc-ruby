# frozen_string_literal: true

RSpec.describe "Native processing completion ownership" do
  it "holds delivery completion through original cleanup even before core processing starts" do
    completions = []
    processing = []
    observer = Object.new
    observer.define_singleton_method(:start) { |_| observer }
    observer.define_singleton_method(:processing_finished) { |value| processing << value }
    observer.define_singleton_method(:finished) { |value| completions << value }
    owner = Object.new
    bus = ConnectRPC::Instrumentation.new(subscribers: [observer])
    handle = bus.start(owner: owner, role: :server, method: nil, carrier: {})
    handle.activate do
      handle.send(:with_processing_completion) do
        handle.finish(delivery: :aborted)
        expect(completions).to be_empty
        expect(processing).to be_empty
      end
    end
    expect(processing).to be_empty
    expect(completions.size).to eq(1)
    expect(completions.first.processing_state).to eq(:not_started)
    expect(completions.first.outcome).to be_nil
    expect(completions.first.delivery).to eq(:aborted)
    handle.finish(delivery: :completed)
    expect(completions.size).to eq(1)
  end
end
