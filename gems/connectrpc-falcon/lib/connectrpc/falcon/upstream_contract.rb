# frozen_string_literal: true

module ConnectRPC
  module Falcon
    # Endpoint replaces build_endpoint and calls the protected tcp_endpoint.
    # Checked once at require with connectrpc-async's upstream contract.
    module UpstreamContract
      HOOKS = [
        ["async-http", "Async::HTTP::Endpoint", :build_endpoint, :public, "(_ = ?)", "Async::HTTP::Endpoint"],
        ["async-http", "Async::HTTP::Endpoint", :tcp_endpoint, :protected, "()", "Async::HTTP::Endpoint"]
      ].freeze
      TESTED = { "async-http" => "0.105.0" }.freeze

      def self.verify!
        ConnectRPC::Async.const_get(:Internal).const_get(:UpstreamContract)
                         .verify!(package: "connectrpc-falcon", version: VERSION, hooks: HOOKS, tested: TESTED,
                                  finalization: false)
      end
    end
    private_constant :UpstreamContract
  end
end
