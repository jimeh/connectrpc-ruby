# frozen_string_literal: true

require_relative "native_ledger"

module Conformance
  # Server-only H1 execution has its own discovered inventory and runner mode.
  class HTTP1Ledger < NativeLedger
    INVENTORIES = %w[identity gzip].to_h do |profile|
      [profile, JSON.parse(File.read(File.join(__dir__, "cases-http1-#{profile}.json")))]
    end.freeze

    def initialize(direction:, selected: nil, profile: "identity")
      raise "HTTP1 conformance is Falcon server-only" unless direction == "server"

      super(direction: direction, selected: selected, profile: profile, inventory: INVENTORIES.fetch(profile))
    end

    private

    def runner_configuration_pattern
      Regexp.new('\\ARunning \\d+ tests with reference-client for server config #\\d+ ' \
                 '\\{HTTP_VERSION_1, PROTOCOL_CONNECT, TLS:(?:false|true)(?: \\(with client certs\\))?\\}\\.\\.\\.\\z')
    end
  end
end
