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
    end.insert(2, { "event" => "wire_response", "name" => name, "status" => 200 }) + [{ "event" => "client_eof" }]
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
    telemetry[4]["result"] = { "testName" => get_name, "response" => {} }
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

  def cancellation_fixture(profile = "identity") # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    cancel_name = (profile == "gzip" ? described_class::GZIP_CANCELLATIONS : described_class::CANCELLATIONS).first
    log = raw.gsub(name, cancel_name)
    log = log.sub("Computed 96", "Computed 88") if profile == "gzip"
    telemetry = events.map { |event| event.merge("name" => cancel_name) }
    wire = { "encoding" => profile == "gzip" ? "gzip" : "identity", "bytes" => 42,
             "sha256" => "a" * 64, "gzip_magic" => profile == "gzip", "content_length" => "42" }
    telemetry.reject! { |event| event["event"] == "wire_response" }
    telemetry[0]["compression"] = profile == "gzip" ? "COMPRESSION_GZIP" : "COMPRESSION_IDENTITY"
    telemetry[0]["codec"] = "CODEC_JSON"
    telemetry[1].merge!("deadline" => false, "http_method" => "POST",
                        "request_headers" => { "content-type" => ["application/json"] }, "wire" => wire)
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
    terminal = { "code" => "canceled", "cause" => nil, "error_class" => "ConnectRPC::Error" }
    admission = { "event" => "call_admitted", "name" => cancel_name, "timeout_present" => false, "request" => request }
    telemetry[1]["request"] = request
    extra_events[1].merge!("attempts" => 2, "completed_writes" => 2, "observer_complete" => true,
                           "prepared_body_prefix" => wire)
    extra_events[3].merge!("attached" => 1, "close_calls" => 1)
    extra_events[4].merge!("cause" => nil, "error_class" => "ConnectRPC::Error")
    finish = terminal.merge("event" => "call_finished", "name" => cancel_name, "observer_complete" => true)
    %w[transport setup exchange].each do |phase|
      finish["#{phase}_entries"] = 1
      finish["#{phase}_exits"] = 1
    end
    scoped = ->(kind) { terminal.merge("event" => kind, "name" => cancel_name, "timeout" => nil) }
    telemetry = [telemetry[0], extra_events[0], admission,
                 { "event" => "cancellation_armed", "name" => cancel_name, "armed_at" => 1.0, "expires" => 1.005 },
                 scoped.call("call_started"), scoped.call("transport_started"), scoped.call("setup_started"),
                 scoped.call("exchange_started"), telemetry[1], extra_events[2], extra_events[4], extra_events[1],
                 scoped.call("exchange_finished"), extra_events[3], scoped.call("setup_finished"),
                 scoped.call("transport_finished"), finish, telemetry[2], telemetry[3], { "event" => "client_eof" }]
    telemetry.each { |event| event["monotonic"] = event["event"] == "call_started" ? 1.0 : 1.006 }
    [log, telemetry, cancel_name]
  end

  %w[identity gzip].each do |profile|
    it "requires complete evidence for fully written cancellation and zero waived outcomes for #{profile}" do
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

  %w[before_transport before_setup during_setup before_write write_incomplete].each do |stage|
    it "accepts positively closed #{stage} evidence and rejects missing or contradictory observations" do
      log, telemetry, cancel_name = cancellation_fixture
      finish = telemetry.find { |event| event["event"] == "call_finished" }
      stages = { "before_transport" => 0, "before_setup" => 1, "during_setup" => 2,
                 "before_write" => 3, "write_incomplete" => 3 }
      depth = stages.fetch(stage)
      %w[transport setup exchange].each_with_index do |phase, index|
        present = depth > index
        finish["#{phase}_entries"] = present ? 1 : 0
        finish["#{phase}_exits"] = present ? 1 : 0
        telemetry.reject! { |event| ["#{phase}_started", "#{phase}_finished"].include?(event["event"]) } unless present
      end
      telemetry.reject! { |event| event["event"] == "socket_cleanup" } if depth < 2
      telemetry.reject! { |event| %w[dispatch wire_failure request_written].include?(event["event"]) } if depth < 3
      if depth == 3
        write = telemetry.find { |event| event["event"] == "request_written" }
        write.merge!("attempts" => stage == "before_write" ? 0 : 1, "completed_writes" => 0,
                     "bytes" => 0, "written_at" => nil, "headers_complete" => false,
                     "body" => { "bytes" => 0 }, "prepared_body_prefix" => { "bytes" => 0 }, "request" => nil,
                     "test_name_header" => nil, "content_length" => nil)
      end
      check = ->(data) { described_class.new(direction: "client", selected: cancel_name).parse(log, data, 0) }
      result = check.call(telemetry)
      expect(result.fetch(:cancellation_stages)).to eq(cancel_name => stage)
      expect(result.fetch(:dispatched)).to eq(depth == 3 ? [cancel_name] : [])
      # Every retained event is required. A removed event is never early-cancel proof.
      telemetry.each_index do |index|
        expect { check.call(telemetry.each_with_index.reject { |_, i| index == i }.map(&:first)) }
          .to raise_error(StandardError), "accepted missing #{telemetry[index].fetch('event')} for #{stage}"
      end
      telemetry.each do |event|
        expect { check.call(telemetry + [event]) }.to raise_error(StandardError)
      end
      [->(data) { data.find { |event| event["event"] == "call_finished" }["observer_complete"] = false },
       ->(data) { data.find { |event| event["event"] == "call_finished" }["transport_exits"] = 2 },
       ->(data) { data.find { |event| event["event"] == "call_finished" }["cause"] = "IOError" },
       ->(data) { data.find { |event| event["event"] == "call_admitted" }["timeout_present"] = true },
       ->(data) { data.find { |event| event["event"] == "call_started" }["timeout"] = 30 },
       ->(data) { data.find { |event| event["event"] == "call_admitted" }["request"] = {} },
       ->(data) { data.find { |event| event["event"] == "cancellation_armed" }["expires"] = 1.2 },
       ->(data) { data.find { |event| event["event"] == "cancellation_signal" }["requested_at"] = 1.001 },
       lambda do |data|
         completed = data.find { |event| event["event"] == "completed" }
         completed["code"] = "CODE_DEADLINE_EXCEEDED"
       end].each do |mutate|
        changed = Marshal.load(Marshal.dump(telemetry))
        mutate.call(changed)
        expect { check.call(changed) }.to raise_error(StandardError)
      end
    end
  end

  it "rejects body, deadline, terminal, cleanup and write lifetime contradictions on dispatched cancellation" do
    log, telemetry, cancel_name = cancellation_fixture
    mutations = {
      "dispatch" => { "deadline" => true, "request" => {}, "request_headers" => { "connect-timeout-ms" => ["1"] } },
      "wire_failure" => { "code" => "unavailable" },
      "transport_finished" => { "code" => nil, "cause" => "IOError" },
      "request_written" => { "attempts" => 1, "observer_complete" => false,
                             "body" => { "bytes" => 41 }, "test_name_header" => "wrong", "written_at" => nil },
      "socket_cleanup" => { "closed" => false, "interrupted" => false, "retired" => false, "attached" => 0 }
    }
    mutations.each do |kind, fields|
      fields.each do |field, value|
        changed = Marshal.load(Marshal.dump(telemetry))
        changed.find { |event| event["event"] == kind }[field] = value
        expect { described_class.new(direction: "client", selected: cancel_name).parse(log, changed, 0) }
          .to raise_error(StandardError), "accepted #{kind}.#{field}=#{value.inspect}"
      end
    end
  end

  %w[identity gzip].each do |profile|
    it "requires a complete observed transport response separately from the canceled Client for #{profile}" do
      log, telemetry, cancel_name = cancellation_fixture(profile)
      %w[transport_finished setup_finished exchange_finished].each do |kind|
        telemetry.find { |event| event["event"] == kind }.merge!("code" => nil, "cause" => nil, "error_class" => nil)
      end
      wire = telemetry.find { |event| event["event"] == "dispatch" }.fetch("wire")
      telemetry.find { |event| event["event"] == "wire_failure" }.replace(
        "event" => "wire_response", "name" => cancel_name, "status" => 200, "wire" => wire
      )
      telemetry.find { |event| event["event"] == "socket_cleanup" }.merge!("interrupted" => false, "close_calls" => 0)
      check = lambda do |data|
        described_class.new(direction: "client", selected: cancel_name, profile: profile).parse(log, data, 0)
      end
      expect(check.call(telemetry).fetch(:cancellation_stages)).to eq(cancel_name => "response_observed")
      telemetry.each_index do |index|
        expect { check.call(telemetry.each_with_index.reject { |_, i| index == i }.map(&:first)) }
          .to raise_error(StandardError), "accepted missing #{telemetry[index].fetch('event')}"
      end
      mutations = {
        "wire_response" => { "status" => 503, "wire" => { "bytes" => 0 } },
        "call_finished" => { "code" => nil, "cause" => "IOError" },
        "completed" => { "outcome" => "success", "code" => nil },
        "request_written" => { "attempts" => 3, "observer_complete" => false, "bytes" => 0 },
        "socket_cleanup" => { "retired" => false, "closed" => false, "interrupted" => true },
        "transport_finished" => { "code" => "canceled", "error_class" => "ConnectRPC::Error" },
        "setup_finished" => { "cause" => "IOError" },
        "exchange_finished" => { "error_class" => "EOFError" },
        "responded" => { "result" => { "testName" => cancel_name, "response" => { "payloads" => [{}] } } }
      }
      mutations.each do |kind, fields|
        fields.each do |field, value|
          changed = Marshal.load(Marshal.dump(telemetry))
          changed.find { |event| event["event"] == kind }[field] = value
          expect { check.call(changed) }.to raise_error(StandardError), "accepted #{kind}.#{field}=#{value.inspect}"
        end
      end
      contradiction = { "event" => "wire_failure", "name" => cancel_name, "code" => "canceled" }
      expect { check.call(telemetry + [contradiction]) }.to raise_error(StandardError)
    end
  end

  it "rejects cancellation-only proof or late wire outcomes on ordinary cases" do
    changed = events + [{ "event" => "call_finished", "name" => name, "observer_complete" => true }]
    expect { parse(raw, changed) }.to raise_error(RuntimeError, /Unexplained cancellation/)
    late = events.reject { |event| event["event"] == "wire_response" }
    late << events.find { |event| event["event"] == "wire_response" }
    expect { parse(raw, late) }.to raise_error(RuntimeError, /Wire terminal ordering/)
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
