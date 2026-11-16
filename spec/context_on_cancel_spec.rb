# frozen_string_literal: true

RSpec.describe ConnectRPC::Context, "#on_cancel" do
  it "runs a listener once in the cancelling thread when the host cancels the call" do
    source = ConnectRPC::CancellationSource.new
    context = described_class.new(cancellation: source.token)
    threads = []
    registration = context.on_cancel { threads << Thread.current }
    expect(registration).to be_a(ConnectRPC::CancellationRegistration)
    expect(threads).to be_empty
    canceller = Thread.new { source.cancel }
    canceller.join
    expect(threads).to eq([canceller])
    expect(context.cancellation_reason).to eq(:canceled)
    expect(registration.unregister).to be(false)
  ensure
    context&.close
  end

  it "runs immediately on registration when the call is already cancelled" do
    source = ConnectRPC::CancellationSource.new
    source.cancel
    context = described_class.new(cancellation: source.token)
    calls = []
    context.on_cancel { calls << :immediate }
    expect(calls).to eq([:immediate])
  ensure
    context&.close
  end

  it "runs immediately when registration observes an expired deadline" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    context = described_class.new(deadline: deadline)
    calls = []
    context.on_cancel { calls << :early }
    expect(calls).to be_empty
    now = 2.0
    context.on_cancel { calls << :expired }
    # Observation of the expired deadline also releases the earlier listener.
    expect(calls).to contain_exactly(:early, :expired)
    expect(context.cancellation_reason).to eq(:deadline_exceeded)
  ensure
    context&.close
  end

  it "skips an unregistered listener and isolates a failing one" do
    source = ConnectRPC::CancellationSource.new
    context = described_class.new(cancellation: source.token)
    calls = []
    context.on_cancel { raise "application failure" }
    removed = context.on_cancel { calls << :removed }
    context.on_cancel { calls << :kept }
    expect(removed.unregister).to be(true)
    expect(source.cancel).to be(true)
    expect(calls).to eq([:kept])
  ensure
    context&.close
  end
end
