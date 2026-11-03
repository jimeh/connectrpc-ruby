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

    def initialize(direction:, selected: nil, profile: "identity", inventory: nil)
      @direction = direction
      @profile = profile
      all = @inventory = (inventory || PROFILES.fetch(profile)).fetch(direction)
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
                   canceled_before_transport: @cancel_stages.select do |_, stage|
                     stage == "before_transport"
                   end.keys.sort,
                   cancellation_stages: @cancel_stages,
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
             runner_configuration_pattern, ""
          next
        else
          raise "Conformance accounting: Unrecognized runner output #{line.inspect}"
        end
      end
    end

    def runner_configuration_pattern
      /\ARunning \d+ tests with reference-(?:client|server) for server config #1 #{SERVER_CONFIG}\.\.\.\z/
    end

    def exact_set!(actual, expected, label)
      verify(actual.uniq.size == actual.size, "Duplicate #{label}")
      verify(actual.sort == expected.sort,
             "#{label} set mismatch: missing #{expected - actual}; unexpected #{actual - expected}")
    end

    def validate_runner!
      verify(@counts.fetch(:computed) == @inventory.size,
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
      @cancel_stages = {}
      allowed = if @direction == "client"
                  %w[received dispatch wire_response wire_failure completed responded
                     cancellation_instruction cancellation_signal cancellation_armed call_admitted
                     call_started call_finished transport_started transport_finished setup_started setup_finished
                     exchange_started exchange_finished
                     request_written socket_cleanup client_eof]
                else
                  %w[dispatch completed server_ready server_stdin_eof server_stopped]
                end
      verify(events.all? do |event|
        allowed.include?(event.fetch("event"))
      end, "Adapter/setup error or unknown telemetry event")
      exact_set!(event_names(events, "completed"), @scheduled, "completed")
      if @direction == "client"
        %w[received responded].each { |kind| exact_set!(event_names(events, kind), @scheduled, kind) }
        verify(events.one? { |event| event.fetch("event") == "client_eof" }, "Missing client EOF")
        validate_client_results!(events)
      else
        exact_set!(event_names(events, "dispatch"), @scheduled, "dispatch")
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
        expected = sequence
        expected = sequence - ["dispatch"] if @direction == "client" && !@dispatched.include?(name)
        verify(observed == expected, "Event ordering #{name}: #{observed}")
        next unless @direction == "client" && @dispatched.include?(name)

        kinds = events.select { |event| event["name"] == name }.map { |event| event["event"] }
        terminal = (kinds & %w[wire_response wire_failure]).first
        verify(kinds.index("dispatch") < kinds.index(terminal) && kinds.index(terminal) < kinds.index("completed"),
               "Wire terminal ordering #{name}")
      end
    end

    def validate_client_results!(events)
      expected_cancel = (CANCELLATIONS + GZIP_CANCELLATIONS) & @scheduled
      exact_set!(event_names(events, "cancellation_instruction"), expected_cancel,
                 "cancellation instructions")
      events.select { |event| event.fetch("event") == "responded" }.each do |event|
        result = event.fetch("result")
        verify(result.fetch("testName") == event.fetch("name") && result.key?("response") && !result.key?("error"),
               "Invalid adapter result")
      end
      expected_cancel.each { |name| validate_cancellation!(events, name) }
      @dispatched = (@scheduled - expected_cancel) + expected_cancel.select do |name|
        finish = events.find { |event| event["event"] == "call_finished" && event["name"] == name }
        finish.fetch("exchange_entries") == 1
      end
      exact_set!(event_names(events, "dispatch"), @dispatched, "dispatch")
      exact_set!(event_names(events, "wire_response") + event_names(events, "wire_failure"),
                 @dispatched, "wire terminal observations")
      cancellation_events = %w[call_admitted cancellation_armed cancellation_signal call_started call_finished
                               transport_started transport_finished setup_started setup_finished
                               exchange_started exchange_finished request_written socket_cleanup]
      cancellation_events.each do |kind|
        names = event_names(events, kind)
        verify((names - expected_cancel).empty?, "Unexplained cancellation #{kind}")
      end
    end

    def validate_wire!(events)
      if @direction == "client"
        exact_set!(event_names(events, "wire_response") + event_names(events, "wire_failure"),
                   @dispatched, "wire terminal observations")
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
      verify(records.map { |event| event.fetch("event") }.uniq.size == records.size,
             "Duplicate cancellation observation #{name}")
      indexed = records.to_h { |event| [event.fetch("event"), event] }
      %w[call_admitted cancellation_armed cancellation_signal call_started call_finished].each do |kind|
        verify(indexed.key?(kind), "Missing cancellation #{kind}")
      end
      verify(indexed.fetch("cancellation_instruction").fetch("instruction") == { "afterCloseSendMs" => 5 },
             "Cancellation timing changed")
      admission = indexed.fetch("call_admitted")
      verify(admission.fetch("timeout_present") == false && indexed.fetch("call_started").fetch("timeout").nil?,
             "Cancellation substituted a deadline")
      expected = { "responseDefinition" => {
        "responseHeaders" => [{ "name" => "x-custom-header", "value" => ["foo"] }],
        "responseData" => "dGVzdCByZXNwb25zZQ==",
        "responseTrailers" => [{ "name" => "x-custom-trailer", "value" => ["bing"] }], "responseDelayMs" => 200
      } }
      verify(admission.fetch("request") == expected, "Cancellation request changed")
      armed = indexed.fetch("cancellation_armed")
      signal = indexed.fetch("cancellation_signal")
      verify((armed.fetch("expires") - armed.fetch("armed_at") - 0.005).abs < 0.00000001,
             "Cancellation timer changed")
      verify(signal.fetch("requested_at") >= armed.fetch("expires"), "Cancellation timer fired early")
      verify(armed.fetch("armed_at") <= indexed.fetch("call_started").fetch("monotonic") &&
             signal.fetch("requested_at") <= indexed.fetch("call_finished").fetch("monotonic"),
             "Cancellation timer outside call lifetime")
      verify(signal.fetch("changed") == true && signal.fetch("reason") == "canceled", "Source cancellation missing")
      finish = indexed.fetch("call_finished")
      verify(finish.fetch("observer_complete") == true, "Incomplete call observer lifetime")
      canceled_terminal!(finish)
      response_observed = indexed.key?("wire_response")
      sequence = %w[received cancellation_instruction call_admitted cancellation_armed call_started]
      %w[transport setup exchange].each_with_index do |phase, index|
        entries = finish.fetch("#{phase}_entries")
        exits = finish.fetch("#{phase}_exits")
        verify([0, 1].include?(entries) && entries == exits, "Incomplete/duplicate #{phase} lifetime")
        if index.positive?
          parent = %w[transport setup exchange][index - 1]
          verify(entries <= finish.fetch("#{parent}_entries"), "Unexplained #{phase} lifetime")
        end
        exact_set!(event_names(records, "#{phase}_started"), entries == 1 ? [name] : [], "#{phase} start")
        exact_set!(event_names(records, "#{phase}_finished"), entries == 1 ? [name] : [], "#{phase} finish")
        sequence << "#{phase}_started" if entries == 1
        next unless entries == 1

        terminal = indexed.fetch("#{phase}_finished")
        if response_observed
          successful_terminal!(terminal)
        else
          phase == "transport" ? canceled_terminal!(terminal) : interrupted_terminal!(terminal)
        end
      end
      setup = finish.fetch("setup_entries") == 1
      exchange = finish.fetch("exchange_entries") == 1
      exact_set!(event_names(records, "socket_cleanup"), setup ? [name] : [], "socket cleanup")
      exact_set!(event_names(records, "request_written"), exchange ? [name] : [], "write observation")
      exact_set!(event_names(records, "wire_failure") + event_names(records, "wire_response"),
                 exchange ? [name] : [], "cancellation wire terminal")
      if setup
        cleanup = indexed.fetch("socket_cleanup")
        verify(cleanup.fetch("retired") == true, "Cancellation did not retire socket ownership")
        closes = cleanup.fetch("close_calls")
        verify(closes.is_a?(Integer) && closes.between?(0, 1), "Invalid socket close calls")
        verify(cleanup.fetch("interrupted") == (closes.zero? ? false : nil), "False socket interruption claim")
        attached = cleanup.fetch("attached")
        verify(attached.is_a?(Integer) && attached.between?(0, 1), "Invalid attached socket count")
        verify(attached.zero? ? cleanup.fetch("closed").nil? : cleanup.fetch("closed") == true,
               "Cancellation socket closure unproven")
      end
      stage = if exchange
                validate_cancellation_write!(indexed, name, expected, signal, response_observed)
              elsif setup
                "during_setup"
              elsif finish.fetch("transport_entries") == 1
                "before_setup"
              else
                "before_transport"
              end
      @cancel_stages[name] = stage
      if exchange
        kind = response_observed ? "wire_response" : "wire_failure"
        sequence.push("dispatch", kind, "request_written", "exchange_finished")
        if response_observed
          validate_cancellation_response!(indexed.fetch(kind))
        else
          wire = indexed.fetch(kind)
          terminal = indexed.fetch("exchange_finished")
          verify(wire.slice("code", "cause", "error_class") == terminal.slice("code", "cause", "error_class"),
                 "Cancellation direct wire result disagrees with exchange")
        end
      end
      sequence.push("socket_cleanup", "setup_finished") if setup
      sequence << "transport_finished" if finish.fetch("transport_entries") == 1
      sequence.push("call_finished", "completed", "responded")
      observed = records.reject { |event| event["event"] == "cancellation_signal" }.map { |event| event.fetch("event") }
      verify(observed == sequence, "Cancellation causal ordering #{name}: #{observed}")
      verify(records.index(signal) < records.index(indexed.fetch("completed")), "Source signal after completion")
      completed = indexed.fetch("completed")
      verify(completed.fetch("outcome") == "error" && completed.fetch("code") == "CODE_CANCELED",
             "Cancellation library result changed")
      response = indexed.fetch("responded").fetch("result").fetch("response")
      verify(response.fetch("error").fetch("code") == "CODE_CANCELED" && !response.key?("payloads"),
             "Cancellation adapter result changed")
    end

    def canceled_terminal!(event)
      verify(event.fetch("code") == "canceled" && event.fetch("cause").nil? &&
             event.fetch("error_class") == "ConnectRPC::Error", "Cancellation library terminal unproven")
    end

    def successful_terminal!(event)
      verify(event.fetch("code").nil? && event.fetch("cause").nil? && event.fetch("error_class").nil?,
             "Transport response did not complete successfully")
    end

    def validate_cancellation_response!(event)
      wire = event.fetch("wire")
      compressed = @profile == "gzip"
      verify(event.fetch("status") == 200 && wire.fetch("bytes").positive? &&
             wire.fetch("sha256").match?(/\A[0-9a-f]{64}\z/) &&
             wire.fetch("encoding") == (compressed ? "gzip" : "identity") && wire.fetch("gzip_magic") == compressed,
             "Incomplete cancellation response wire evidence")
    end

    def interrupted_terminal!(event)
      return canceled_terminal!(event) if event.fetch("error_class") == "ConnectRPC::Error"

      network_error = /\A(?:EOFError|IOError|SocketError|Errno::[A-Z0-9]+|Net::(?:OpenTimeout|ReadTimeout|WriteTimeout|
                          HTTPBadResponse|HTTPHeaderSyntaxError)|OpenSSL::SSL::SSLError)\z/x
      verify(event.fetch("code").nil? && event.fetch("error_class").match?(network_error),
             "Invalid direct I/O terminal")
    end

    # An unfinished write leaves its own bytes unknown, not the facts established
    # by earlier successful writes. Check those facts before selecting any branch.
    def validate_written_facts!(written, records, name, expected)
      prepared = records.fetch("dispatch").fetch("wire")
      body_bytes = written.fetch("body").fetch("bytes")
      complete = written.fetch("headers_complete")
      verify([true, false].include?(complete) && body_bytes <= prepared.fetch("bytes") &&
             (complete || body_bytes.zero?), "Inconsistent written request boundaries")
      if complete || written.fetch("test_name_header")
        verify(written.fetch("test_name_header") == name, "Cancellation wire name changed")
      end
      if complete || written.fetch("content_length")
        verify(written.fetch("content_length") == prepared.fetch("bytes").to_s, "Cancellation wire length changed")
      end
      verify(!complete || written.fetch("timeout_header") == false, "Cancellation sent a wire timeout")
      message = complete && body_bytes == prepared.fetch("bytes") ? expected : nil
      verify(written.fetch("request") == message, "Cancellation actual request changed")
      return if written.fetch("completed_writes").zero?

      time = written.fetch("written_at")
      verify(time.is_a?(Numeric) && time >= records.fetch("exchange_started").fetch("monotonic") &&
             time <= written.fetch("monotonic"), "Write outside observed exchange lifetime")
    end

    def validate_cancellation_write!(records, name, expected, signal, response_observed)
      dispatch = records.fetch("dispatch")
      verify(dispatch.fetch("deadline") == false, "Cancellation substituted a deadline")
      verify(!dispatch.fetch("request_headers").key?("connect-timeout-ms"), "Cancellation wire timeout added")
      verify(dispatch.fetch("request") == expected, "Cancellation encoded request changed")
      instruction = records.fetch("received")
      codec = instruction.fetch("codec") == "CODEC_JSON" ? "json" : "proto"
      encoding = instruction.fetch("compression") == "COMPRESSION_GZIP" ? "gzip" : "identity"
      verify(dispatch.fetch("http_method") == "POST" &&
             dispatch.fetch("request_headers").fetch("content-type") == ["application/#{codec}"] &&
             dispatch.fetch("wire").fetch("encoding") == encoding &&
             dispatch.fetch("wire").fetch("gzip_magic") == (encoding == "gzip"), "Cancellation coding changed")
      written = records.fetch("request_written")
      verify(written.fetch("observer_complete") == true, "Incomplete write observer lifetime")
      attempts = written.fetch("attempts")
      completed = written.fetch("completed_writes")
      verify(attempts.is_a?(Integer) && completed.is_a?(Integer) && attempts >= completed && completed >= 0,
             "Invalid write counts")
      bytes = written.fetch("bytes")
      body = written.fetch("body")
      verify(bytes.is_a?(Integer) && bytes >= 0 && body.fetch("bytes").between?(0, bytes), "Invalid written bytes")
      verify(body == written.fetch("prepared_body_prefix"), "Cancellation written prefix changed")
      verify(written.fetch("timeout_header") != true, "Cancellation sent a wire timeout")
      cleanup = records.fetch("socket_cleanup")
      verify(cleanup.fetch("attached") == 1 && cleanup.fetch("closed"), "Cancellation socket cleanup unproven")
      validate_written_facts!(written, records, name, expected)
      if completed.zero?
        verify(bytes.zero? && body.fetch("bytes").zero? && written.fetch("written_at").nil? &&
               written.fetch("headers_complete") == false && written.fetch("request").nil? &&
               written.fetch("test_name_header").nil? && written.fetch("content_length").nil?,
               "Unexplained written bytes")
      else
        verify(bytes.positive? && written.fetch("written_at").is_a?(Numeric), "Missing completed write bytes")
      end
      verify(!response_observed || (attempts == completed && completed.positive?),
             "Response without complete request write")
      return "write_incomplete" if attempts > completed
      return "before_write" if attempts.zero?

      verify(written.fetch("headers_complete") && body.fetch("bytes").positive?, "Incomplete request write")
      verify(body.except("content_length") == dispatch.fetch("wire").except("content_length"),
             "Cancellation actual body changed")
      return "response_observed" if response_observed

      written.fetch("written_at") <= signal.fetch("requested_at") ? "request_written" : "write_overlapped_signal"
    end
  end
end
