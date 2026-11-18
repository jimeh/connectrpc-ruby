# frozen_string_literal: true

require_relative "ledger"
require_relative "native_lifetimes"

module Conformance
  # The native profiles have separate source-derived inventories. Execution must
  # match each complete named set, including actual runner discovery counts.
  class NativeLedger < Ledger
    include NativeLifetimes

    INVENTORIES = %w[identity gzip].to_h do |profile|
      [profile, JSON.parse(File.read(File.join(__dir__, "cases-native-#{profile}.json")))]
    end.freeze

    def initialize(direction:, selected: nil, profile: "identity", inventory: nil)
      super(direction: direction, selected: selected, profile: profile,
            inventory: inventory || INVENTORIES.fetch(profile))
    end

    # A server that failed to close or never stopped also explains runner
    # failures, so its telemetry is checked before the runner output.
    def parse(raw, events, exit_code)
      validate_native_server_stops!(events) if @direction == "server"
      super
    end

    private

    def runner_configuration_pattern
      Regexp.new('\\ARunning \\d+ tests with reference-(?:client|server) for server config #\\d+ ' \
                 '\\{HTTP_VERSION_2, PROTOCOL_CONNECT, TLS:(?:false|true)(?: \\(with client certs\\))?\\}\\.\\.\\.\\z')
    end

    def validate_events!(events)
      @cancel_stages = {}
      verify(events.none? { |event| event.fetch("event") == "adapter_error" }, "Native adapter error")
      @direction == "client" ? validate_native_client_events!(events) : validate_native_server_events!(events)
    end

    def validate_native_client_events!(events)
      %w[received responded completed native_call_started native_call_finished].each do |kind|
        exact_set!(event_names(events, kind), @scheduled, "native #{kind}")
      end
      verify(events.one? { |event| event.fetch("event") == "client_eof" }, "Native client EOF missing")
      validate_client_lifetimes!(events)
      validate_scope_pairs!(events)
      validate_native_cancellations!(events)
    end

    # The runner kills a server process that has not exited 5s after SIGTERM,
    # before it can report anything more. Name what the telemetry still shows.
    def validate_native_server_stops!(events)
      failure = events.find { |event| event.fetch("event") == "server_close_failed" }
      verify(failure.nil?, failure && "Native server close failed in pid #{failure.fetch('pid')}: " \
                                      "#{failure.fetch('error_class')}: #{failure.fetch('message')}")
      ready = server_pids(events, "server_ready")
      stopped = server_pids(events, "server_stopped")
      verify(!ready.empty?, "Native server never became ready")
      verify(ready.sort == stopped.sort, "Native server lifetime mismatch: #{unpaired_detail(events, ready, stopped)}")
    end

    def server_pids(events, kind)
      events.select { |event| event.fetch("event") == kind }.map { |event| event.fetch("pid") }
    end

    def unpaired_detail(events, ready, stopped)
      unstopped = (ready - stopped).map do |pid|
        last = events.reverse_each.find { |event| event["pid"] == pid }
        "pid #{pid} never stopped; last event #{last.fetch('event')} #{last['name']}".rstrip
      end
      unready = (stopped - ready).map { |pid| "pid #{pid} stopped without becoming ready" }
      (unstopped + unready).join("; ")
    end

    def validate_native_server_events!(events)
      validate_server_lifetimes!(events)
    end
  end
end
