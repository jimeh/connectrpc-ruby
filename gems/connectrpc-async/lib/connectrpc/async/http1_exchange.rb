# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      class HTTP1Exchange
        def initialize(transport, operation)
          @transport = transport
          @operation = operation
        end

        def response_head(timeout: nil) = invoke(:response_head, timeout: timeout)
        def read(length, timeout: nil) = invoke(:read, length, timeout: timeout)
        def write(bytes) = invoke(:write, bytes)
        def close_send = invoke(:close_send)
        def finish(timeout: nil) = invoke(:finish, timeout: timeout)
        def abort = invoke(:abort)

        private

        def invoke(method, *, **)
          @transport.send(method, @operation, *, **)
        end
      end
    end
  end
end
