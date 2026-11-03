# frozen_string_literal: true

require_relative "native_ledger"

module Conformance
  class WebLedger < NativeLedger
    def initialize(host:, selected: nil)
      @host = host
      inventory = JSON.parse(File.read(File.join(__dir__, "cases-web-#{host}.json")))
      super(direction: "server", selected: selected, profile: "web", inventory: inventory)
    end

    private

    def runner_configuration_pattern
      Regexp.new('\\ARunning \\d+ tests with reference-client for server config #\\d+ ' \
                 '\\{HTTP_VERSION_[12], PROTOCOL_GRPC_WEB, TLS:(?:false|true)\\}\\.\\.\\.\\z')
    end

    def validate_native_server_events!(events)
      allowed = %w[server_ready server_stopped native_server_wire_headers native_server_dispatch_enter
                   native_server_dispatch_exit native_server_admitted native_server_processing native_server_finished
                   native_server_closed_lifetime]
      verify(events.all? { |event| allowed.include?(event.fetch("event")) }, "Unknown Web telemetry event")
      super
      unowned = @scheduled.select { |name| name.end_with?("/unexpected-uri") }
      owned = @scheduled - unowned
      %w[native_server_dispatch_enter native_server_dispatch_exit native_server_admitted
         native_server_processing native_server_finished].each do |kind|
        exact_set!(event_names(events, kind), owned, "Web #{kind}")
      end
      exact_set!(event_names(events, "native_server_wire_headers"), @scheduled, "Web actual wire arrivals")
      finished = events.select { |event| event["event"] == "native_server_finished" }
      verify(finished.all? { |event| event["processing"] == "finished" }, "Web processing cleanup incomplete")
    end

    def validate_server_closed!(events)
      return super unless @host == "rack"

      # Portable hosting has no native session or authenticated task catalog.
      verify(events.count { |event| event["event"] == "server_ready" } ==
             events.count { |event| event["event"] == "server_stopped" }, "Portable Web host did not stop")
    end
  end
end
