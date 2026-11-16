# frozen_string_literal: true

require_relative "../conformance/native_threaded_ledger"

RSpec.describe Conformance::NativeThreadedLedger do
  let(:name) { described_class::INVENTORIES.fetch("connect-identity").fetch("client").first }
  let(:raw) do
    <<~LOG
      Computed 336 test case permutation(s) across 6 server configuration(s).
      Filtered tests to 1 test case permutation(s) across 1 server configuration(s).
      Running 1 tests with reference-server for server config #1 {HTTP_VERSION_2, PROTOCOL_CONNECT, TLS:false}...
      Sending request for #{JSON.generate(name)}...
      Received response for #{JSON.generate(name)}...
      Total cases: 1
      1 passed, 0 failed
    LOG
  end
  let(:closed) do
    { "event" => "native_closed_lifetime", "name" => name, "state" => "closed", "calls" => 0, "acquiring" => 0,
      "tasks_finished" => true, "streams_closed" => true, "resources_finished" => true,
      "lifetime_children_finished" => true, "raw_closed" => true, "front_state" => "closed",
      "close_outcome" => true, "admitted" => 0, "mailboxes" => 0, "owner_started" => true, "owner_exited" => true,
      "source_canceled" => false, "counts" => { "library" => { "entries" => 1, "exits" => 1 } } }
  end
  let(:events) do
    %w[received responded completed native_call_started native_call_finished].map do |kind|
      { "event" => kind, "name" => name, "cancellation" => false }
    end + [
      { "event" => "client_eof" },
      { "event" => "native_scope_enter", "pid" => 1, "id" => 1, "phase" => "library", "name" => name,
        "thread" => "caller" },
      { "event" => "native_scope_exit", "pid" => 1, "id" => 1, "phase" => "library", "name" => name },
      closed
    ]
  end

  def parse = described_class.new(direction: "client", selected: name).parse(raw, events, 0)

  it "accepts a closed lifetime whose close returned true" do
    expect(parse).to include(executed: 1, passed: 1, failed: 0, accounting_valid: true)
  end

  it "rejects a close that returned false or was never recorded" do
    closed["close_outcome"] = false
    expect { parse }.to raise_error(RuntimeError, /Threaded close did not succeed/)
    closed.delete("close_outcome")
    expect { parse }.to raise_error(RuntimeError, /Threaded close did not succeed/)
  end
end
