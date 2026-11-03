# frozen_string_literal: true

require_relative "native_ledger"

module Conformance
  class GRPCLedger < NativeLedger
    INVENTORIES = %w[identity gzip].to_h do |profile|
      [profile, JSON.parse(File.read(File.join(__dir__, "cases-grpc-#{profile}.json")))]
    end.freeze

    def initialize(direction:, selected: nil, profile: "identity")
      super(direction: direction, selected: selected, profile: profile,
            inventory: INVENTORIES.fetch(profile))
    end

    private

    def validate_native_server_events!(events)
      super
      exact_set!(event_names(events, "native_server_wire_headers"), @scheduled, "gRPC native wire invocations")
      dispatched = event_names(events, "native_server_dispatch_enter")
      verify((dispatched - @scheduled).empty?, "Unknown gRPC application dispatch")
      verify(dispatched.uniq.size == dispatched.size, "Duplicate gRPC application dispatch")
    end

    def runner_configuration_pattern
      Regexp.new('\\ARunning \\d+ tests with (?:reference|grpc-reference)-(?:client|server) for server config #\\d+ ' \
                 '\\{HTTP_VERSION_2, PROTOCOL_GRPC, TLS:(?:false|true)(?: \\(with client certs\\))?\\}\\.\\.\\.\\z')
    end
  end
end
