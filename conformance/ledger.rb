# frozen_string_literal: true

require "json"

module Conformance
  # v1.0.6 has no machine-readable/list mode. Account for its --vv protocol
  # strictly, then cross-check independent actual adapter dispatch telemetry.
  class Ledger
    BASELINE = JSON.parse(File.read(File.join(__dir__, "cases.json"))).freeze
    GZIP = JSON.parse(File.read(File.join(__dir__, "cases-gzip.json"))).freeze
    PROFILES = { "identity" => BASELINE, "gzip" => GZIP }.freeze
    GZIP_CANCELLATIONS = GZIP.fetch("client").select { |name| name.start_with?("Client Cancellation/") }.freeze
    CANCELLATIONS = BASELINE.fetch("client").select { |name| name.start_with?("Client Cancellation/") }.freeze
    MANIFEST = File.join(__dir__, "known-failing-client.txt")
    KNOWN_FAILURES = File.readlines(MANIFEST, chomp: true).reject { |line| line.empty? || line.start_with?("#") }.freeze
    unless KNOWN_FAILURES.size == 2 && KNOWN_FAILURES.sort == CANCELLATIONS.sort
      raise "Known failures must name exactly the two configured caller-cancellation cases"
    end

    GZIP_MANIFEST = File.join(__dir__, "known-failing-gzip-client.txt")
    GZIP_KNOWN_FAILURES = File.readlines(GZIP_MANIFEST, chomp: true)
                              .reject { |line| line.empty? || line.start_with?("#") }.freeze
    unless GZIP_KNOWN_FAILURES.size == 2 && GZIP_KNOWN_FAILURES.sort == GZIP_CANCELLATIONS.sort
      raise "Gzip known failures must name exactly the two approved caller-cancellation cases"
    end

    MANIFESTS = { "identity" => MANIFEST, "gzip" => GZIP_MANIFEST }.freeze
    PROFILE_FAILURES = { "identity" => KNOWN_FAILURES, "gzip" => GZIP_KNOWN_FAILURES }.freeze

    CANCELLATION_FAILURE = ["expecting an error but received none",
                            "expecting 0 response messages but instead got 1"].freeze
    SERVER_CONFIG = Regexp.escape("{HTTP_VERSION_1, PROTOCOL_CONNECT, TLS:false}")
    attr_reader :summary

    def initialize(direction:, selected: nil, profile: "identity")
      @direction = direction
      @profile = profile
      all = PROFILES.fetch(profile).fetch(direction)
      raise "Selector must be an exact configured test name" if selected && !all.include?(selected)

      @expected = selected ? [selected] : all
      @selected = selected
      @scheduled = []
      @received = []
      @failures = {}
      @expected_failures = {}
      @counts = {}
    end

    def parse(raw, events, exit_code)
      parse_lines(raw)
      validate_runner!
      validate_events!(events)
      verify(exit_code == (@failures.empty? ? 0 : 1), "Runner exit disagrees with outcomes: #{exit_code}")
      @summary = { runner: "v1.0.6", ruby: RUBY_VERSION, direction: @direction, profile: @profile,
                   configured_discovered: @counts.fetch(:computed),
                   executed: @scheduled.size, passed: @counts.fetch(:passed), failed: @counts.fetch(:failed),
                   expected_failed: @expected_failures.size, could_not_run: @counts.fetch(:could_not_run, 0),
                   unsupported_instructions: events.count do |event|
                     event.fetch("event") == "unsupported_cancellation"
                   end,
                   scheduled: @scheduled.sort, received: @received.sort,
                   dispatched: event_names(events, "dispatch").sort, completed: event_names(events, "completed").sort,
                   outcomes: @scheduled.sort.to_h do |name|
                     outcome = if @expected_failures.key?(name)
                                 "expected_failure"
                               else
                                 @failures.key?(name) ? "failed" : "passed"
                               end
                     [name, outcome]
                   end,
                   failure_assertions: @failures, expected_failure_assertions: @expected_failures,
                   runner_exit: exit_code, accounting_valid: true }
    end

    private

    def verify(condition, message)
      raise "Conformance accounting: #{message}" unless condition
    end

    def count(key, value)
      verify(!@counts.key?(key), "Duplicate #{key}")
      @counts[key] = value.to_i
    end

    def parse_lines(raw)
      current = nil
      raw.each_line(chomp: true) do |line|
        if line.start_with?("\t")
          verify(current, "Unattributed failure assertion")
          (@failures[current] || @expected_failures.fetch(current)) << line.delete_prefix("\t")
          next
        end
        current = nil unless line.empty?
        case line
        when /\ASending request for (".*")\.\.\.\z/
          @scheduled << JSON.parse(Regexp.last_match(1))
        when /\AReceived response for (".*")\.\.\.\z/
          @received << JSON.parse(Regexp.last_match(1))
        when /\AComputed (\d+) test case permutation\(s\) across (\d+) server configuration\(s\)\.\z/
          count(:computed, Regexp.last_match(1))
        when /\AFiltered tests to (\d+) test case permutation\(s\) across (\d+) server configuration\(s\)\.\z/
          count(:filtered, Regexp.last_match(1))
        when /\AFAILED: (.+):\z/
          current = Regexp.last_match(1)
          verify(!@failures.key?(current) && !@expected_failures.key?(current), "Duplicate failure #{current}")
          @failures[current] = []
        when /\AINFO: (.+) failed \(as expected\):\z/
          current = Regexp.last_match(1)
          verify(!@failures.key?(current) && !@expected_failures.key?(current), "Duplicate failure #{current}")
          @expected_failures[current] = []
        when /\ALoaded (\d+) known failing test case pattern\(s\) that match (\d+) test case permutation\(s\)\.\z/
          patterns, matches = Regexp.last_match.captures
          count(:known_patterns, patterns)
          count(:known_matches, matches)
        when /\ATotal cases: (\d+)\z/
          count(:total, Regexp.last_match(1))
        when /\A(\d+) passed, (\d+) failed\z/
          passed, failed = Regexp.last_match.captures
          count(:passed, passed)
          count(:failed, failed)
        when /\AAnother (\d+) could not be run due to client timing out or exiting prematurely\.\z/
          count(:could_not_run, Regexp.last_match(1))
        when %r{\A\(Another (\d+) failed as expected due to being known failures/flakes\.\)\z}
          count(:expected_failed, Regexp.last_match(1))
        when /\AComputed \d+ config case permutations\.\z/,
             /\ALoaded \d+ test suite\(s\), \d+ test case template\(s\)\.\z/,
             /\ARunning \d+ tests with reference-(?:client|server) for server config #1 #{SERVER_CONFIG}\.\.\.\z/, ""
          next
        else
          raise "Conformance accounting: Unrecognized runner output #{line.inspect}"
        end
      end
    end

    def exact_set!(actual, expected, label)
      verify(actual.uniq.size == actual.size, "Duplicate #{label}")
      verify(actual.sort == expected.sort,
             "#{label} set mismatch: missing #{expected - actual}; unexpected #{actual - expected}")
    end

    def validate_runner!
      verify(@counts.fetch(:computed) == PROFILES.fetch(@profile).fetch(@direction).size,
             "Configured case count changed")
      verify(@counts.fetch(:filtered, @counts.fetch(:computed)) == @expected.size, "Filtered count mismatch")
      verify(!@selected || @counts.key?(:filtered), "Missing selector accounting")
      exact_set!(@scheduled, @expected, "scheduled")
      exact_set!(@received, @scheduled, "runner responses")
      configured_failures = PROFILE_FAILURES.fetch(@profile)
      known = @direction == "client" ? configured_failures & @scheduled : []
      exact_set!(@expected_failures.keys, known, "expected failure outcomes")
      manifest_size = @direction == "client" ? configured_failures.size : 0
      verify(@counts.fetch(:known_patterns, 0) == manifest_size &&
             @counts.fetch(:known_matches, 0) == manifest_size, "Known failure manifest accounting changed")
      verify(@counts.fetch(:expected_failed, 0) == known.size, "Expected failure count mismatch")
      verify((@failures.keys - @scheduled).empty?, "Outcome outside scheduled set")
      verify((@failures.values + @expected_failures.values).none?(&:empty?), "Missing failure assertion")
      verify(@counts.fetch(:could_not_run, 0).zero?, "Some cases could not run")
      verify(@counts.fetch(:total) == @scheduled.size, "Total count mismatch")
      verify(@counts.fetch(:failed) == @failures.size, "Failure count mismatch")
      verify(@counts.fetch(:passed) + @failures.size + known.size == @scheduled.size, "Pass count mismatch")
    end

    def event_names(events, kind)
      events.select { |event| event.fetch("event") == kind }.map { |event| event.fetch("name") }
    end

    def validate_events!(events)
      allowed = if @direction == "client"
                  %w[received dispatch wire_response wire_failure completed responded unsupported_cancellation
                     client_eof]
                else
                  %w[dispatch completed server_ready server_stdin_eof server_stopped]
                end
      verify(events.all? do |event|
        allowed.include?(event.fetch("event"))
      end, "Adapter/setup error or unknown telemetry event")
      %w[dispatch completed].each { |kind| exact_set!(event_names(events, kind), @scheduled, kind) }
      if @direction == "client"
        %w[received responded].each { |kind| exact_set!(event_names(events, kind), @scheduled, kind) }
        verify(events.one? { |event| event.fetch("event") == "client_eof" }, "Missing client EOF")
        validate_client_results!(events)
      else
        %w[server_ready server_stdin_eof server_stopped].each do |kind|
          verify(events.one? { |event| event.fetch("event") == kind }, "Missing/duplicate #{kind}")
        end
      end
      validate_wire!(events) if @profile == "gzip"
      validate_order!(events)
    end

    def validate_order!(events)
      sequence = @direction == "client" ? %w[received dispatch completed responded] : %w[dispatch completed]
      @scheduled.each do |name|
        observed = events.filter_map do |event|
          event["event"] if event["name"] == name && sequence.include?(event["event"])
        end
        verify(observed == sequence, "Event ordering #{name}: #{observed}")
      end
    end

    def validate_client_results!(events)
      expected_cancel = (CANCELLATIONS + GZIP_CANCELLATIONS) & @scheduled
      exact_set!(event_names(events, "unsupported_cancellation"), expected_cancel,
                 "unsupported cancellation instructions")
      events.select { |event| event.fetch("event") == "responded" }.each do |event|
        result = event.fetch("result")
        verify(result.fetch("testName") == event.fetch("name") && result.key?("response") && !result.key?("error"),
               "Invalid adapter result")
      end
      expected_cancel.each { |name| validate_cancellation!(events, name) }
    end

    def validate_wire!(events)
      if @direction == "client"
        exact_set!(event_names(events, "wire_response") + event_names(events, "wire_failure"),
                   @scheduled, "wire terminal observations")
        events.select { |event| event["event"] == "dispatch" }.each do |event|
          wire = event.fetch("wire")
          instruction = events.find { |entry| entry["name"] == event["name"] && entry["event"] == "received" }
          compressed = instruction.fetch("compression") == "COMPRESSION_GZIP"
          verify(wire.fetch("encoding") == (compressed ? "gzip" : "identity"), "Request coding changed")
          verify(wire.fetch("gzip_magic") == compressed, "Missing actual request wire evidence")
        end
      else
        events.select { |event| event["event"] == "dispatch" }.each do |event|
          verify(event.fetch("request_encoding") == "gzip", "Reference request coding changed")
        end
      end
      # Ordinary errors may be identity. Only actual Basic success responses
      # prove gzip success on both codecs; never infer coding from totals.
      @scheduled.select { |name| name.start_with?("Basic/") && name.end_with?("unary/success") }.each do |name|
        event = events.find do |entry|
          entry["name"] == name && entry["event"] == (@direction == "client" ? "wire_response" : "completed")
        end
        if @direction == "server"
          request = event.fetch("request_wire")
          verify(request.fetch("bytes").positive? && request.fetch("gzip_magic"), "Missing actual gzip request bytes")
        end
        wire = event.fetch(@direction == "client" ? "wire" : "response_wire")
        verify(wire.fetch("encoding") == "gzip" && wire.fetch("gzip_magic") && wire.fetch("bytes").positive?,
               "Missing actual gzip success response")
      end
    end

    def validate_gzip_cancellation!(records, info)
      dispatch = records.fetch("dispatch")
      completed = records.fetch("completed")
      response = records.fetch("wire_response")
      verify(response.fetch("status") == 200, "Cancellation wire status changed")
      verify(completed.fetch("monotonic") - dispatch.fetch("monotonic") >= 0.2,
             "Cancellation did not complete the requested delay")
      verify(!dispatch.fetch("request_headers").key?("connect-timeout-ms"), "Cancellation wire timeout added")
      headers = info.fetch("requestHeaders").to_h { |header| [header.fetch("name").downcase, header.fetch("value")] }
      verify(headers["content-encoding"] == ["gzip"], "Cancellation peer did not observe gzip")
      verify(headers["content-length"] == [dispatch.fetch("wire").fetch("bytes").to_s],
             "Cancellation peer wire length changed")
      wire = response.fetch("wire")
      verify(%w[gzip identity].include?(wire.fetch("encoding")) && wire.fetch("bytes").positive?,
             "Cancellation response wire missing")
      verify(wire.fetch("gzip_magic") == (wire.fetch("encoding") == "gzip"), "Cancellation response coding changed")
      verify(wire.fetch("content_length").to_i == wire.fetch("bytes"), "Cancellation response length changed")
    end

    def validate_cancellation_echo!(info, name)
      verify(!info.key?("timeoutMs"), "Cancellation sent a wire timeout")
      headers = info.fetch("requestHeaders")
      test_header = headers.select { |header| header.fetch("name").downcase == "x-test-case-name" }
      verify(test_header.size == 1 && test_header.first.fetch("value") == [name], "Cancellation request name changed")
      requests = info.fetch("requests")
      expected = { "@type" => "type.googleapis.com/connectrpc.conformance.v1.UnaryRequest",
                   "responseDefinition" => {
                     "responseHeaders" => [{ "name" => "x-custom-header", "value" => ["foo"] }],
                     "responseData" => "dGVzdCByZXNwb25zZQ==",
                     "responseTrailers" => [{ "name" => "x-custom-trailer", "value" => ["bing"] }],
                     "responseDelayMs" => 200
                   } }
      verify(requests == [expected], "Cancellation request roundtrip changed")
    end

    def validate_cancellation!(events, name)
      records = events.select { |event| event["name"] == name }.to_h { |event| [event.fetch("event"), event] }
      verify(records.fetch("unsupported_cancellation").fetch("instruction") == { "afterCloseSendMs" => 5 },
             "Cancellation timing changed")
      verify(records.fetch("dispatch").fetch("deadline") == false, "Cancellation substituted a deadline")
      verify(records.fetch("completed").fetch("outcome") == "success", "Unrelated cancellation RPC error")
      response = records.fetch("responded").fetch("result").fetch("response")
      verify(!response.key?("error"), "Synthetic cancellation error")
      payloads = response.fetch("payloads")
      verify(payloads.size == 1 && payloads.first.fetch("data") == "dGVzdCByZXNwb25zZQ==",
             "Cancellation payload changed")
      info = payloads.first.fetch("requestInfo")
      validate_cancellation_echo!(info, name)
      validate_gzip_cancellation!(records, info) if @profile == "gzip"
      %w[responseHeaders responseTrailers].zip(%w[x-custom-header x-custom-trailer],
                                               %w[foo bing]).each do |field, key, value|
        entries = response.fetch(field).select { |header| header.fetch("name") == key }
        verify(entries.size == 1 && entries.first.fetch("value") == [value], "Cancellation #{field} changed")
      end
      assertions = @expected_failures[name] || @failures[name]
      verify(assertions == CANCELLATION_FAILURE,
             "Cancellation failure assertions changed: #{assertions}")
    end
  end
end
