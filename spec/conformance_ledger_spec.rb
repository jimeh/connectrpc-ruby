# frozen_string_literal: true

require_relative "../conformance/ledger"

RSpec.describe Conformance::Ledger do
  let(:name) do
    described_class::BASELINE.fetch("client").find do |entry|
      entry.start_with?("Basic/") && entry.end_with?("unary/success")
    end
  end
  let(:raw) do
    <<~LOG
      Computed 94 test case permutation(s) across 1 server configuration(s).
      Loaded 2 known failing test case pattern(s) that match 2 test case permutation(s).
      Filtered tests to 1 test case permutation(s) across 1 server configuration(s).
      Sending request for #{JSON.generate(name)}...
      Received response for #{JSON.generate(name)}...
      Total cases: 1
      1 passed, 0 failed
    LOG
  end
  let(:events) do
    %w[received dispatch completed responded].map do |kind|
      { "event" => kind, "name" => name, "result" => { "testName" => name, "response" => {} } }
    end + [{ "event" => "client_eof" }]
  end

  def parse(log = raw, telemetry = events, code = 0)
    described_class.new(direction: "client", selected: name).parse(log, telemetry, code)
  end

  it "records discovered configured count separately from selected execution and outcomes" do
    result = parse
    expect(result).to include(configured_discovered: 94, executed: 1, passed: 1, failed: 0, could_not_run: 0,
                              unsupported_instructions: 0, accounting_valid: true)
    expect(result.fetch(:outcomes)).to eq(name => "passed")
  end

  it "rejects reductions, missing responses, duplicates, unknown names and incorrect counts even with exit zero" do
    sending = "Sending request for #{JSON.generate(name)}...\n"
    receiving = "Received response for #{JSON.generate(name)}...\n"
    [raw.sub("Computed 94", "Computed 93"), raw.sub(sending, ""), raw.sub(receiving, ""),
     raw.sub(sending, sending * 2), raw.sub(receiving, receiving * 2), raw.sub("Total cases: 1", "Total cases: 0"),
     raw.sub(receiving, "Received response for \"different\"...\n"),
     "#{raw}Another 1 could not be run due to client timing out or exiting prematurely.\n"].each do |log|
      expect { parse(log) }.to raise_error(RuntimeError, /Conformance accounting/)
    end
  end

  it "rejects parsing ambiguity, unknown output, unauthorized waivers and inconsistent runner exits" do
    ["#{raw}Total cases: 1\n", "#{raw}unexpected diagnostic\n", "#{raw}\tunattributed failure\n",
     raw + "INFO: #{name} failed (as expected):\n\texpecting an error but received none\n"].each do |log|
      expect { parse(log) }.to raise_error(RuntimeError, /Conformance accounting/)
    end
    expect { parse(raw, events, 1) }.to raise_error(RuntimeError, /Runner exit/)
    expect do
      described_class.new(direction: "client", selected: "Basic/*")
    end.to raise_error(RuntimeError, /exact configured/)
  end

  it "rejects missing or duplicate real dispatch/outcomes, setup rejection and out-of-order results" do
    [events.reject { |event| event["event"] == "dispatch" }, events + [events[1]],
     events + [{ "event" => "adapter_error", "name" => name }], events.reverse,
     events.map { |event| event.merge("name" => "wrong name") }].each do |telemetry|
      expect { parse(raw, telemetry) }.to raise_error(RuntimeError, /Conformance accounting/)
    end
  end

  it "retains exact failure assertions and rejects a green exit that hides them" do
    log = raw.sub("Total cases: 1", "FAILED: #{name}:\n\tresponse #1: expecting data 01, got 02\n\nTotal cases: 1")
             .sub("1 passed, 0 failed", "0 passed, 1 failed")
    summary = parse(log, events, 1)
    expect(summary.fetch(:failure_assertions)).to eq(name => ["response #1: expecting data 01, got 02"])
    expect { parse(log, events, 0) }.to raise_error(RuntimeError, /Runner exit/)
  end

  def cancellation_fixture(profile = "identity")
    cancel_name = (profile == "gzip" ? described_class::GZIP_CANCELLATIONS : described_class::CANCELLATIONS).first
    assertions = described_class::CANCELLATION_FAILURE.join("\n\t")
    failure = "INFO: #{cancel_name} failed (as expected):\n\t#{assertions}\n\nTotal cases: 1"
    log = raw.gsub(name, cancel_name).sub("Total cases: 1", failure)
             .sub("1 passed, 0 failed", "0 passed, 0 failed")
    log += "(Another 1 failed as expected due to being known failures/flakes.)\n"
    telemetry = events.map { |event| event.merge("name" => cancel_name) }
    telemetry[1]["deadline"] = false
    telemetry[2]["outcome"] = "success"
    telemetry[3]["result"] = { "testName" => cancel_name, "response" => {
      "payloads" => [{ "data" => "dGVzdCByZXNwb25zZQ==", "requestInfo" => {
        "requestHeaders" => [{ "name" => "X-Test-Case-Name", "value" => [cancel_name] }],
        "requests" => [{ "@type" => "type.googleapis.com/connectrpc.conformance.v1.UnaryRequest",
                         "responseDefinition" => {
                           "responseHeaders" => [{ "name" => "x-custom-header", "value" => ["foo"] }],
                           "responseData" => "dGVzdCByZXNwb25zZQ==",
                           "responseTrailers" => [{ "name" => "x-custom-trailer",
                                                    "value" => ["bing"] }],
                           "responseDelayMs" => 200
                         } }]
      } }],
      "responseHeaders" => [{ "name" => "x-custom-header", "value" => ["foo"] }],
      "responseTrailers" => [{ "name" => "x-custom-trailer", "value" => ["bing"] }]
    } }
    telemetry << { "event" => "unsupported_cancellation", "name" => cancel_name,
                   "instruction" => { "afterCloseSendMs" => 5 } }
    if profile == "gzip"
      log = log.sub("Computed 94", "Computed 88")
      telemetry[0]["compression"] = "COMPRESSION_GZIP"
      telemetry[1].merge!("monotonic" => 1.0, "request_headers" => {},
                          "wire" => { "encoding" => "gzip", "bytes" => 42, "gzip_magic" => true })
      telemetry[2]["monotonic"] = 1.201
      headers = telemetry[3].fetch("result").fetch("response").fetch("payloads").first
                            .fetch("requestInfo").fetch("requestHeaders")
      headers.push({ "name" => "Content-Encoding", "value" => ["gzip"] },
                   { "name" => "Content-Length", "value" => ["42"] })
      telemetry.insert(2, { "event" => "wire_response", "name" => cancel_name, "status" => 200,
                            "wire" => { "encoding" => "gzip", "bytes" => 200, "gzip_magic" => true,
                                        "content_length" => "200" } })
    end
    [log, telemetry, cancel_name]
  end

  it "requires actual uncanceled success, payload, metadata and only the exact cancellation assertion blocks" do
    log, telemetry, cancel_name = cancellation_fixture
    ledger = lambda { |text, records|
      described_class.new(direction: "client", selected: cancel_name).parse(text, records, 0)
    }
    expect(ledger.call(log,
                       telemetry)).to include(failed: 0, passed: 0, expected_failed: 1, unsupported_instructions: 1)
    expect { ledger.call(log.sub("failed (as expected):", "was expected to fail but did not"), telemetry) }
      .to raise_error(RuntimeError, /Conformance accounting/)
    expect { ledger.call(log.sub("Another 1 failed as expected", "Another 0 failed as expected"), telemetry) }
      .to raise_error(RuntimeError, /Expected failure count/)
    expect { ledger.call(log.sub("expecting an error but received none", "deadline exceeded"), telemetry) }
      .to raise_error(RuntimeError, /Cancellation failure assertions changed/)
    altered = Marshal.load(Marshal.dump(telemetry))
    altered[2]["outcome"] = "error"
    expect { ledger.call(log, altered) }.to raise_error(RuntimeError, /Unrelated cancellation RPC error/)
    altered[2]["outcome"] = "success"
    altered[3]["result"]["response"]["payloads"][0]["data"] = "wrong"
    expect { ledger.call(log, altered) }.to raise_error(RuntimeError, /Cancellation payload changed/)
  end

  it "keeps the gzip inventory disjoint, exact and additional to identity" do
    %w[client server].zip([94, 88], [88, 82]).each do |direction, identity_count, gzip_count|
      identity = described_class::BASELINE.fetch(direction)
      gzip = described_class::GZIP.fetch(direction)
      expect(identity.size).to eq(identity_count)
      expect(gzip.size).to eq(gzip_count)
      expect(gzip.uniq).to eq(gzip)
      expect(identity & gzip).to be_empty
    end
    expect(described_class::KNOWN_FAILURES & described_class::GZIP_CANCELLATIONS).to be_empty
    expect(described_class::GZIP_KNOWN_FAILURES.sort).to eq(described_class::GZIP_CANCELLATIONS.sort)
    expect(described_class::GZIP_KNOWN_FAILURES.size).to eq(2)
  end

  it "requires approved gzip expected failures to retain strict causal and wire proof" do
    log, telemetry, name = cancellation_fixture("gzip")
    ledger = lambda { |text, events|
      described_class.new(direction: "client", profile: "gzip", selected: name).parse(text, events, 0)
    }
    summary = ledger.call(log, telemetry)
    expect(summary).to include(executed: 1, failed: 0, passed: 0, expected_failed: 1, accounting_valid: true)
    expect(summary.fetch(:outcomes)).to eq(name => "expected_failure")
    expect { ledger.call(log.sub("expecting an error but received none", "different error"), telemetry) }
      .to raise_error(RuntimeError, /Cancellation failure assertions/)
    altered = Marshal.load(Marshal.dump(telemetry))
    altered[1]["wire"]["gzip_magic"] = false
    expect { ledger.call(log, altered) }.to raise_error(RuntimeError, /wire evidence/)
    altered = Marshal.load(Marshal.dump(telemetry))
    altered[1]["deadline"] = true
    expect { ledger.call(log, altered) }.to raise_error(RuntimeError, /substituted a deadline/)
    altered = Marshal.load(Marshal.dump(telemetry))
    altered[3]["outcome"] = "error"
    expect { ledger.call(log, altered) }.to raise_error(RuntimeError, /Unrelated cancellation RPC error/)
    expect { ledger.call(log.sub("Another 1 failed as expected", "Another 0 failed as expected"), telemetry) }
      .to raise_error(RuntimeError, /Expected failure count/)
    expect { ledger.call(log.sub("failed (as expected):", "was expected to fail but did not"), telemetry) }
      .to raise_error(RuntimeError, /Conformance accounting/)
  end
end
