# frozen_string_literal: true

require_relative "native_ledger"

module Conformance
  # Separate client inventories for ThreadedTransport: Connect and gRPC unary
  # and server streaming, each in identity and gzip profiles.
  class NativeThreadedLedger < NativeLedger
    PROTOCOLS = { "connect" => "PROTOCOL_CONNECT", "grpc" => "PROTOCOL_GRPC" }.freeze
    INVENTORIES = PROTOCOLS.keys.product(%w[identity gzip]).to_h do |protocol, profile|
      path = File.join(__dir__, "cases-threaded-#{protocol}-#{profile}.json")
      ["#{protocol}-#{profile}", JSON.parse(File.read(path))]
    end.freeze

    def initialize(direction:, selected: nil, profile: "connect-identity")
      raise "Threaded profiles are client-only" unless direction == "client"

      @protocol = PROTOCOLS.fetch(profile.split("-").first)
      super(direction: direction, selected: selected, profile: profile, inventory: INVENTORIES.fetch(profile))
    end

    private

    def runner_configuration_pattern
      Regexp.new('\\ARunning \\d+ tests with (?:reference|grpc-reference)-server for server config #\\d+ ' \
                 "\\{HTTP_VERSION_2, #{@protocol}, TLS:(?:false|true)(?: \\(with client certs\\))?\\}\\.\\.\\.\\z")
    end

    # Both owners close: close returned true, the front's admission and
    # mailboxes are empty, its owner thread exited, and the inner Transport
    # retired every operation.
    def validate_client_lifetimes!(events)
      super
      events.select { |event| event.fetch("event") == "native_closed_lifetime" }.each do |event|
        verify(event["close_outcome"] == true, "Threaded close did not succeed")
        verify(event.fetch("front_state") == "closed", "Threaded front did not close")
        verify(event.fetch("admitted").zero? && event.fetch("mailboxes").zero?, "Threaded admission leaked")
        verify(event.fetch("owner_exited"), "Threaded owner thread did not exit")
        verify(event.fetch("raw_closed") != false, "Threaded raw socket leaked")
      end
      validate_threads!(events)
    end

    # Library and transport scopes run on the adapter's plain caller thread;
    # dispatch, operations and cleanup run on the transport's owner thread.
    def validate_threads!(events)
      events.select { |event| event.fetch("event") == "native_scope_enter" }.each do |event|
        expected = %w[library transport].include?(event.fetch("phase")) ? "caller" : "owner"
        verify(event.fetch("thread") == expected, "Threaded #{event.fetch('phase')} scope ran on the wrong thread")
      end
    end
  end
end
