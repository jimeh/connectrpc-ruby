# frozen_string_literal: true

require "json"

module Conformance
  # v1.0.6 has no machine-readable/list mode. Account for its --vv protocol
  # strictly, then cross-check independent actual adapter dispatch telemetry.
  class Ledger
    BASELINE = JSON.parse(File.read(File.join(__dir__, "cases.json"))).freeze
    GZIP = JSON.parse(File.read(File.join(__dir__, "cases-gzip.json"))).freeze
    GET_CASES = %w[JSON PROTO].map do |codec|
      "Connect with GET/HTTPVersion:1/Codec:CODEC_#{codec}/TLS:false/success"
    end.freeze
    WITH_GET = BASELINE.transform_values { |names| (names + GET_CASES).sort.freeze }.freeze
    PROFILES = { "identity" => WITH_GET, "gzip" => GZIP }.freeze
    GZIP_CANCELLATIONS = GZIP.fetch("client").select { |name| name.start_with?("Client Cancellation/") }.freeze
    CANCELLATIONS = BASELINE.fetch("client").select { |name| name.start_with?("Client Cancellation/") }.freeze
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
                   expected_failed: 0, could_not_run: @counts.fetch(:could_not_run, 0),
                   scheduled: @scheduled.sort, received: @received.sort,
                   dispatched: event_names(events, "dispatch").sort, completed: event_names(events, "completed").sort,
                   outcomes: @scheduled.sort.to_h do |name|
                     outcome = @failures.key?(name) ? "failed" : "passed"
                     [name, outcome]
                   end,
                   failure_assertions: @failures,
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
          @failures.fetch(current) << line.delete_prefix("\t")
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
          verify(!@failures.key?(current), "Duplicate failure #{current}")
          @failures[current] = []
        when /\ATotal cases: (\d+)\z/
          count(:total, Regexp.last_match(1))
        when /\A(\d+) passed, (\d+) failed\z/
          passed, failed = Regexp.last_match.captures
          count(:passed, passed)
          count(:failed, failed)
        when /\AAnother (\d+) could not be run due to client timing out or exiting prematurely\.\z/
          count(:could_not_run, Regexp.last_match(1))
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
      verify((@failures.keys - @scheduled).empty?, "Outcome outside scheduled set")
      verify(@failures.values.none?(&:empty?), "Missing failure assertion")
      verify(@counts.fetch(:could_not_run, 0).zero?, "Some cases could not run")
      verify(@counts.fetch(:total) == @scheduled.size, "Total count mismatch")
      verify(@counts.fetch(:failed) == @failures.size, "Failure count mismatch")
      verify(@counts.fetch(:passed) + @failures.size == @scheduled.size, "Pass count mismatch")
    end

    def event_names(events, kind)
      events.select { |event| event.fetch("event") == kind }.map { |event| event.fetch("name") }
    end

    def validate_events!(events)
      allowed = if @direction == "client"
                  %w[received dispatch wire_response wire_failure completed responded
                     cancellation_instruction cancellation_signal request_written socket_cleanup client_eof]
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
      validate_get!(events)
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
      exact_set!(event_names(events, "wire_response") + event_names(events, "wire_failure"),
                 @scheduled, "wire terminal observations")
      expected_cancel = (CANCELLATIONS + GZIP_CANCELLATIONS) & @scheduled
      exact_set!(event_names(events, "cancellation_instruction"), expected_cancel,
                 "cancellation instructions")
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

    def validate_get!(events)
      (@scheduled & GET_CASES).each do |name|
        dispatch = events.find { |event| event["name"] == name && event["event"] == "dispatch" }
        verify(dispatch.fetch("http_method") == "GET", "GET case did not dispatch GET")
        query = dispatch.fetch("query")
        controls = query.fetch("controls")
        codec = name.include?("CODEC_JSON") ? "json" : "proto"
        verify(controls.fetch("encoding") == codec && controls.fetch("connect") == "v1", "GET controls changed")
        payload = query.fetch("payload")
        verify(payload.fetch("bytes").positive? && payload.fetch("sha256").match?(/\A[0-9a-f]{64}\z/) &&
               payload.fetch("encoding") == "identity" && !payload.fetch("gzip_magic"), "Missing GET payload evidence")
        if @direction == "client"
          headers = dispatch.fetch("request_headers")
          verify(!headers.keys.intersect?(%w[content-type content-encoding content-length connect-protocol-version]),
                 "GET sent synthetic content headers")
          body = dispatch.fetch("wire")
        else
          verify(dispatch.fetch("content_type").nil? && dispatch.fetch("content_length").nil?,
                 "Reference GET content headers changed")
          completed = events.find { |event| event["name"] == name && event["event"] == "completed" }
          body = completed.fetch("request_wire")
        end
        empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        terminal = events.find do |event|
          event["name"] == name && event["event"] == (@direction == "client" ? "wire_response" : "completed")
        end
        verify(terminal && terminal.fetch("status") == 200, "Missing successful GET wire response")
        verify(body.fetch("bytes").zero? && body.fetch("sha256") == empty_hash,
               "GET HTTP body was not measured empty")
      end
    end

    def validate_cancellation!(events, name)
      records = events.select { |event| event["name"] == name }
      %w[cancellation_instruction cancellation_signal request_written socket_cleanup wire_failure].each do |kind|
        verify(records.one? { |event| event["event"] == kind }, "Missing/duplicate cancellation #{kind}")
      end
      records = records.to_h { |event| [event.fetch("event"), event] }
      verify(records.fetch("cancellation_instruction").fetch("instruction") == { "afterCloseSendMs" => 5 },
             "Cancellation timing changed")
      dispatch = records.fetch("dispatch")
      verify(dispatch.fetch("deadline") == false, "Cancellation substituted a deadline")
      verify(!dispatch.fetch("request_headers").key?("connect-timeout-ms"), "Cancellation wire timeout added")
      written = records.fetch("request_written")
      verify(written.fetch("bytes").positive? && written.fetch("headers_complete") &&
             written.fetch("body").fetch("bytes").positive?, "Cancellation has no actual wire progress")
      verify(written.fetch("body").except("content_length") == dispatch.fetch("wire").except("content_length"),
             "Cancellation actual body changed")
      verify(written.fetch("test_name_header") == name, "Cancellation wire name changed")
      verify(written.fetch("content_length") == written.fetch("body").fetch("bytes").to_s,
             "Cancellation wire length changed")
      verify(written.fetch("timeout_header") == false, "Cancellation sent a wire timeout")
      expected = { "responseDefinition" => {
        "responseHeaders" => [{ "name" => "x-custom-header", "value" => ["foo"] }],
        "responseData" => "dGVzdCByZXNwb25zZQ==",
        "responseTrailers" => [{ "name" => "x-custom-trailer", "value" => ["bing"] }], "responseDelayMs" => 200
      } }
      verify(written.fetch("request") == expected, "Cancellation request changed")
      signal = records.fetch("cancellation_signal")
      verify(signal.fetch("changed") && signal.fetch("reason") == "canceled", "Source cancellation missing")
      verify(written.fetch("written_at") <= signal.fetch("requested_at"), "Cancellation preceded actual request write")
      cleanup = records.fetch("socket_cleanup")
      verify(cleanup.fetch("closed") && cleanup.fetch("interrupted") && cleanup.fetch("retired"),
             "Cancellation did not interrupt and retire its socket")
      completed = records.fetch("completed")
      verify(completed.fetch("outcome") == "error" && completed.fetch("code") == "CODE_CANCELED",
             "Cancellation library result changed")
      response = records.fetch("responded").fetch("result").fetch("response")
      verify(response.fetch("error").fetch("code") == "CODE_CANCELED" && !response.key?("payloads"),
             "Cancellation adapter result changed")
      verify(records.fetch("wire_failure").fetch("code") == "canceled", "Cancellation transport result missing")
    end
  end
end
