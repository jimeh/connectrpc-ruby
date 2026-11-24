# frozen_string_literal: true

module ConnectRPC
  module Falcon
    # Falcon joins tasks with connectrpc-async's settled-task join.
    CompletionJoin = ConnectRPC::Async.const_get(:Internal).const_get(:CompletionJoin)
    # Falcon teardown reports its failures as connectrpc-async's does.
    Teardown = ConnectRPC::Async.const_get(:Internal).const_get(:Teardown)
    private_constant :CompletionJoin, :Teardown
  end
end
