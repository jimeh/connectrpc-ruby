# frozen_string_literal: true

require_relative "native_ledger"

module Conformance
  class NativeHTTP1Ledger < NativeLedger
    INVENTORIES = %w[identity gzip].to_h do |profile|
      [profile, JSON.parse(File.read(File.join(__dir__, "cases-native-http1-client-#{profile}.json")))]
    end.freeze

    def initialize(direction:, selected: nil, profile: "identity")
      raise "Native HTTP1 profiles are client-only" unless direction == "client"

      super(direction: direction, selected: selected, profile: profile, inventory: INVENTORIES.fetch(profile))
    end

    private

    def runner_configuration_pattern
      Regexp.new('\\ARunning \\d+ tests with reference-server for server config #\\d+ ' \
                 '\\{HTTP_VERSION_1, PROTOCOL_CONNECT, TLS:(?:false|true)(?: \\(with client certs\\))?\\}\\.\\.\\.\\z')
    end

    def validate_client_lifetimes!(events)
      closed = events.select { |event| event.fetch("event") == "native_closed_lifetime" }
      exact_set!(closed.map { |event| event.fetch("name") }, @scheduled, "native H1 closed lifetimes")
      closed.each do |event|
        validate_closed!(event)
        validate_client_scope_counts!(event, events)
      end
      responses = events.select { |event| event.fetch("event") == "native_http1_response" }
      verify(!responses.empty?, "No observed native HTTP/1 response")
      responses.each { |event| validate_response!(event) }
    end

    def validate_closed!(event)
      verify(event.fetch("state") == "closed", "Native H1 transport did not close")
      %w[calls leases idle].each { |fact| verify(event.fetch(fact).zero?, "Native H1 leaked #{fact}") }
      %w[tasks_finished operations_retired resources_finished lifetime_children_finished].each do |fact|
        verify(event.fetch(fact), "Native H1 incomplete cleanup: #{fact}")
      end
      verify(event.fetch("raw_closed") != false, "Native H1 raw socket leaked")
    end

    def validate_response!(event)
      verify(@scheduled.include?(event.fetch("name")), "Unscheduled native H1 response")
      verify(event.fetch("version") == "HTTP/1.1", "Native H1 status line missing or contradictory")
      # Pinned generateTestCasePrefix omits TLS for reliesOnTls suites.
      # The declared H1 inventory's only such suite is TLS Client Certs.
      expected_tls = event.fetch("name").include?("TLS:true") ||
                     event.fetch("name").start_with?("TLS Client Certs/")
      verify(event.fetch("tls") == expected_tls, "Native H1 TLS observation contradicts case")
      verify(expected_tls ? [nil, "http/1.1"].include?(event.fetch("alpn")) : event.fetch("alpn").nil?,
             "Native H1 ALPN observation contradicts HTTP/1.1")
    end
  end
end
