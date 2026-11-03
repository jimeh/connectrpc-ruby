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
      Computed 96 test case permutation(s) across 1 server configuration(s).
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
    end + [{ "event" => "wire_response", "name" => name, "status" => 200 }, { "event" => "client_eof" }]
  end

  def parse(log = raw, telemetry = events, code = 0)
    described_class.new(direction: "client", selected: name).parse(log, telemetry, code)
  end

  it "records discovered configured count separately from selected execution and outcomes" do
    result = parse
    expect(result).to include(configured_discovered: 96, executed: 1, passed: 1, failed: 0, could_not_run: 0,
                              accounting_valid: true)
    expect(result.fetch(:outcomes)).to eq(name => "passed")
  end

  it "rejects reductions, missing responses, duplicates, unknown names and incorrect counts even with exit zero" do
    sending = "Sending request for #{JSON.generate(name)}...\n"
    receiving = "Received response for #{JSON.generate(name)}...\n"
    [raw.sub("Computed 96", "Computed 93"), raw.sub(sending, ""), raw.sub(receiving, ""),
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

  it "requires actual GET, separate query payload and measured-empty body for the named GET cases" do
    get_name = described_class::GET_CASES.first
    log = raw.gsub(name, get_name)
    payload = { "bytes" => 30, "sha256" => "a" * 64, "encoding" => "identity", "gzip_magic" => false }
    body = { "bytes" => 0, "sha256" => "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" }
    telemetry = events.map { |event| event.merge("name" => get_name) }
    telemetry[1].merge!("http_method" => "GET", "request_headers" => {}, "wire" => body,
                        "query" => { "controls" => { "encoding" => "json", "connect" => "v1" }, "payload" => payload })
    telemetry[3]["result"] = { "testName" => get_name, "response" => {} }
    telemetry.reject! { |event| event["event"] == "wire_response" }
    telemetry.insert(2, { "event" => "wire_response", "name" => get_name, "status" => 200 })
    check = ->(records) { described_class.new(direction: "client", selected: get_name).parse(log, records, 0) }
    expect(check.call(telemetry)).to include(executed: 1, passed: 1)
    [->(data) { data[1]["http_method"] = "POST" },
     ->(data) { data[1]["query"]["payload"]["bytes"] = 0 },
     ->(data) { data[1]["wire"]["bytes"] = 1 },
     ->(data) { data[2]["status"] = 415 }].each do |mutate|
      changed = Marshal.load(Marshal.dump(telemetry))
      mutate.call(changed)
      expect { check.call(changed) }.to raise_error(RuntimeError, /Conformance accounting/)
    end
  end

  def cancellation_fixture(profile = "identity")
    cancel_name = (profile == "gzip" ? described_class::GZIP_CANCELLATIONS : described_class::CANCELLATIONS).first
    log = raw.gsub(name, cancel_name)
    log = log.sub("Computed 96", "Computed 88") if profile == "gzip"
    telemetry = events.map { |event| event.merge("name" => cancel_name) }
    wire = { "encoding" => profile == "gzip" ? "gzip" : "identity", "bytes" => 42,
             "sha256" => "a" * 64, "gzip_magic" => profile == "gzip", "content_length" => "42" }
    telemetry.reject! { |event| event["event"] == "wire_response" }
    telemetry[0]["compression"] = profile == "gzip" ? "COMPRESSION_GZIP" : "COMPRESSION_IDENTITY"
    telemetry[1].merge!("deadline" => false, "request_headers" => {}, "wire" => wire)
    telemetry[2].merge!("outcome" => "error", "code" => "CODE_CANCELED")
    telemetry[3]["result"] = { "testName" => cancel_name, "response" => { "error" => { "code" => "CODE_CANCELED" } } }
    request = { "responseDefinition" => {
      "responseHeaders" => [{ "name" => "x-custom-header", "value" => ["foo"] }],
      "responseData" => "dGVzdCByZXNwb25zZQ==",
      "responseTrailers" => [{ "name" => "x-custom-trailer", "value" => ["bing"] }], "responseDelayMs" => 200
    } }
    extra_events = [
      { "event" => "cancellation_instruction", "name" => cancel_name,
        "instruction" => { "afterCloseSendMs" => 5 } },
      { "event" => "request_written", "name" => cancel_name, "bytes" => 250,
        "headers_complete" => true,
        "body" => wire, "timeout_header" => false, "request" => request,
        "test_name_header" => cancel_name,
        "content_length" => "42", "written_at" => 1.0 },
      { "event" => "cancellation_signal", "name" => cancel_name, "changed" => true,
        "reason" => "canceled", "requested_at" => 1.005 },
      { "event" => "socket_cleanup", "name" => cancel_name, "closed" => true, "interrupted" => true,
        "retired" => true },
      { "event" => "wire_failure", "name" => cancel_name, "code" => "canceled" }
    ]
    telemetry.concat(extra_events)
    [log, telemetry, cancel_name]
  end

  %w[identity gzip].each do |profile|
    it "requires actual wire cancellation and zero waived outcomes for #{profile}" do
      log, telemetry, cancel_name = cancellation_fixture(profile)
      check = lambda do |text, records|
        described_class.new(direction: "client", selected: cancel_name, profile: profile).parse(text, records, 0)
      end
      expect(check.call(log, telemetry)).to include(passed: 1, failed: 0, expected_failed: 0, accounting_valid: true)
      %w[request_written cancellation_signal socket_cleanup wire_failure].each do |kind|
        expect { check.call(log, telemetry.reject { |event| event["event"] == kind }) }.to raise_error(RuntimeError)
      end
      %w[bytes headers_complete timeout_header request].each do |field|
        changed = Marshal.load(Marshal.dump(telemetry))
        write = changed.find { |event| event["event"] == "request_written" }
        write[field] =
          { "bytes" => 0, "headers_complete" => false, "timeout_header" => true, "request" => {} }.fetch(field)
        expect { check.call(log, changed) }.to raise_error(RuntimeError, /Conformance accounting/)
      end
      expect { check.call("#{log}(Another 1 failed as expected due to being known failures/flakes.)\n", telemetry) }
        .to raise_error(RuntimeError, /Unrecognized runner output/)
    end
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
    expect(described_class::CANCELLATIONS.size).to eq(2)
    expect(described_class::GZIP_CANCELLATIONS.size).to eq(2)
  end
end
