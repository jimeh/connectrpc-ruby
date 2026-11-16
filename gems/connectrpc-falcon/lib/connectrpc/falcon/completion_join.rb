# frozen_string_literal: true

module ConnectRPC
  module Falcon
    # Falcon and connectrpc-async joins on one task share a single observation.
    CompletionJoin = ConnectRPC::Async.const_get(:Internal).const_get(:CompletionJoin)
    private_constant :CompletionJoin
  end
end
