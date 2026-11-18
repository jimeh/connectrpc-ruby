# frozen_string_literal: true

require_relative "../conformance/native_ledger"

RSpec.describe Conformance::NativeLedger do
  let(:name) { described_class::INVENTORIES.fetch("identity").fetch("server").first }
  let(:raw) do
    <<~LOG
      Computed 262 test case permutation(s) across 3 server configuration(s).
      Filtered tests to 1 test case permutation(s) across 1 server configuration(s).
      Running 1 tests with reference-client for server config #1 {HTTP_VERSION_2, PROTOCOL_CONNECT, TLS:false}...
      Sending request for #{JSON.generate(name)}...
      Received response for #{JSON.generate(name)}...
      Total cases: 1
      1 passed, 0 failed
    LOG
  end
  let(:served) do
    [{ "event" => "server_ready", "pid" => 7 },
     { "event" => "native_server_dispatch_enter", "pid" => 7, "id" => 1, "name" => name }]
  end

  def parse(events)
    described_class.new(direction: "server", selected: name).parse(raw, events, 0)
  end

  it "reports the exception from a close that raised" do
    events = served + [{ "event" => "server_close_failed", "pid" => 7, "error_class" => "Async::Cancel",
                         "message" => "Cancelling task!" },
                       { "event" => "server_stopped", "pid" => 7 }]

    expect { parse(events) }.to raise_error(
      RuntimeError, "Conformance accounting: Native server close failed in pid 7: Async::Cancel: Cancelling task!"
    )
  end

  it "names a server process that never stopped and its last recorded event" do
    expect { parse(served) }.to raise_error(
      RuntimeError, "Conformance accounting: Native server lifetime mismatch: pid 7 never stopped; " \
                    "last event native_server_dispatch_enter #{name}"
    )
  end
end
