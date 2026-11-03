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

    def validate_native_server_events!(events)
      ready = events.count { |event| event.fetch("event") == "server_ready" }
      stopped = events.count { |event| event.fetch("event") == "server_stopped" }
      verify(ready.positive? && ready == stopped, "Native server lifetime mismatch")
      validate_server_lifetimes!(events)
    end
  end
end
