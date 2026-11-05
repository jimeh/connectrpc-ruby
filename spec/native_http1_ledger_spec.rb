# frozen_string_literal: true

require_relative "../conformance/native_http1_ledger"

RSpec.describe Conformance::NativeHTTP1Ledger do
  let(:name) do
    described_class::INVENTORIES.fetch("identity").fetch("client").find do |entry|
      entry.start_with?("TLS Client Certs/")
    end
  end
  let(:raw) do
    <<~LOG
      Computed 386 test case permutation(s) across 3 server configuration(s).
      Filtered tests to 1 test case permutation(s) across 1 server configuration(s).
      Running 1 tests with reference-server for server config #3 {HTTP_VERSION_1, PROTOCOL_CONNECT, TLS:true (with client certs)}...
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
      { "event" => "native_http1_response", "name" => name, "version" => "HTTP/1.1", "tls" => true,
        "alpn" => "http/1.1" },
      { "event" => "native_closed_lifetime", "name" => name, "state" => "closed", "calls" => 0,
        "leases" => 0, "idle" => 0, "tasks_finished" => true, "operations_retired" => true,
        "resources_finished" => true, "raw_closed" => true, "lifetime_children_finished" => true,
        "response_heads" => 1, "source_canceled" => false,
        "counts" => { "library" => { "entries" => 1, "exits" => 1 } } }
    ]
  end

  def parse(log = raw, facts = events)
    described_class.new(direction: "client", selected: name).parse(log, facts, 0)
  end

  it "requires TLS for the pinned reliesOnTls client-cert naming without a TLS suffix" do
    expect(name).not_to include("TLS:true")
    expect(parse).to include(executed: 1, passed: 1, failed: 0, accounting_valid: true)
    response = events.find { |event| event["event"] == "native_http1_response" }
    response["tls"] = false
    expect { parse }.to raise_error(RuntimeError, /TLS observation/)
  end

  it "accepts actual absent ALPN and rejects contradictory protocols or missing observations" do
    response = events.find { |event| event["event"] == "native_http1_response" }
    response["alpn"] = nil
    expect(parse).to include(accounting_valid: true)
    response["alpn"] = "h2"
    expect { parse }.to raise_error(RuntimeError, /ALPN observation/)
    response["alpn"] = "http/1.1"
    response["version"] = "HTTP/2.0"
    expect { parse }.to raise_error(RuntimeError, /status line/)
    expect { parse(raw, events - [response]) }.to raise_error(RuntimeError, /response observations/)
  end

  %i[missing duplicate].each do |corruption|
    it "rejects a #{corruption} response observation while another case's valid observation remains" do
      other = described_class::INVENTORIES.fetch("identity").fetch("client").find do |entry|
        entry.start_with?("TLS Client Certs/") && entry != name
      end
      ledger = described_class.new(direction: "client", selected: name)
      ledger.instance_variable_set(:@expected, [name, other])
      log = raw.sub("Filtered tests to 1", "Filtered tests to 2").sub("Running 1 tests", "Running 2 tests")
               .sub("Total cases: 1", "Sending request for #{JSON.generate(other)}...\n" \
                                      "Received response for #{JSON.generate(other)}...\nTotal cases: 2")
               .sub("1 passed", "2 passed")
      facts = events + events.reject { |event| event["event"] == "client_eof" }.map do |event|
        event.merge("name" => other, "id" => 2)
      end
      expect(ledger.parse(log, facts, 0)).to include(executed: 2, accounting_valid: true)
      response = facts.find { |event| event["event"] == "native_http1_response" && event["name"] == name }
      invalid = corruption == :missing ? facts - [response] : facts + [response.dup]
      fresh = described_class.new(direction: "client", selected: name)
      fresh.instance_variable_set(:@expected, [name, other])
      expect { fresh.parse(log, invalid, 0) }.to raise_error(RuntimeError, /response observations/)
    end
  end

  it "permits genuine zero-head cases and rejects missing, invalid or contradictory head counts" do
    response = events.find { |event| event["event"] == "native_http1_response" }
    closed = events.last
    closed["response_heads"] = 0
    expect(parse(raw, events - [response])).to include(accounting_valid: true)
    expect { parse }.to raise_error(RuntimeError, /response observations/)
    [nil, false, "1", 1.0, -1].each do |invalid|
      closed["response_heads"] = invalid
      expect { parse }.to raise_error(RuntimeError, /response head count/)
    end
    closed.delete("response_heads")
    expect { parse }.to raise_error(RuntimeError, /response head count/)
  end

  it "rejects unfinished tasks, retained leases, live raw sockets and missing scope evidence" do
    closed = events.last
    { "tasks_finished" => false, "leases" => 1, "raw_closed" => false }.each do |key, invalid|
      original = closed.fetch(key)
      closed[key] = invalid
      expect { parse }.to raise_error(RuntimeError, /Conformance accounting/)
      closed[key] = original
    end
    facts = events.reject { |event| event["event"].start_with?("native_scope_") }
    expect { parse(raw, facts) }.to raise_error(RuntimeError, /scope counts/)
    expect { parse(raw.sub("Computed 386", "Computed 385")) }.to raise_error(RuntimeError, /Conformance accounting/)
  end
end
