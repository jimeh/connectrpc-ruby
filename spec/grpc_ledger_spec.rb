# frozen_string_literal: true

require_relative "../conformance/grpc_ledger"

RSpec.describe Conformance::GRPCLedger do
  let(:name) do
    described_class::INVENTORIES.fetch("identity").fetch("client").find do |entry|
      entry.start_with?("Basic/")
    end
  end
  let(:raw) do
    <<~LOG
      Computed 348 test case permutation(s) across 4 server configuration(s).
      Filtered tests to 1 test case permutation(s) across 1 server configuration(s).
      Running 1 tests with grpc-reference-server for server config #4 {HTTP_VERSION_2, PROTOCOL_GRPC, TLS:false}...
      Sending request for #{JSON.generate(name)}...
      Received response for #{JSON.generate(name)}...
      Total cases: 1
      1 passed, 0 failed
    LOG
  end
  let(:events) do
    %w[received responded completed native_call_started native_call_finished].map do |kind|
      { "event" => kind, "name" => name, "cancellation" => false }
    end + [
      { "event" => "client_eof" },
      { "event" => "native_scope_enter", "pid" => 1, "id" => 1, "phase" => "library", "name" => name },
      { "event" => "native_scope_exit", "pid" => 1, "id" => 1, "phase" => "library", "name" => name },
      { "event" => "native_closed_lifetime", "name" => name, "state" => "closed", "calls" => 0, "acquiring" => 0,
        "tasks_finished" => true, "streams_closed" => true, "resources_finished" => true,
        "lifetime_children_finished" => true,
        "source_canceled" => false, "counts" => { "library" => { "entries" => 1, "exits" => 1 } } }
    ]
  end

  def parse(log = raw, facts = events, code = 0)
    described_class.new(direction: "client", selected: name).parse(log, facts, code)
  end

  it "accepts exact official grpc-reference labels only with balanced actual library execution" do
    expect(parse).to include(executed: 1, passed: 1, failed: 0, accounting_valid: true)
  end

  it "rejects missing, unknown, duplicate, unexecuted, setup and cancellation-forgery evidence" do
    sending = "Sending request for #{JSON.generate(name)}...\n"
    [raw.sub(sending, ""), raw.sub(sending, sending * 2), raw.gsub(name, "unknown case"),
     raw.sub("Computed 348", "Computed 347"), "#{raw}expected failure\n"].each do |log|
      expect { parse(log) }.to raise_error(RuntimeError, /Conformance accounting/)
    end
    bad_events = [events.reject { |event| event["event"].start_with?("native_scope_") },
                  events + [{ "event" => "adapter_error" }], events + [events.first],
                  events.map { |event| event.merge("cancellation" => true) }]
    bad_events.each { |facts| expect { parse(raw, facts) }.to raise_error(RuntimeError, /Conformance accounting/) }
    expect { parse(raw, events, 1) }.to raise_error(RuntimeError, /Runner exit/)
  end
end
