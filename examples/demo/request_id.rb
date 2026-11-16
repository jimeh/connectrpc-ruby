# frozen_string_literal: true

require "connectrpc"

module DemoExample
  # A server interceptor that echoes the client's demo-request-id request
  # metadata as a demo-request-id response header. Interceptors wrap every call
  # of a server with one piece of policy, here before the handler runs. The
  # same object covers every RPC shape: server_unary wraps unary calls and
  # server_stream wraps the three streaming shapes. Each must yield to run the
  # handler; raising ConnectRPC::Error instead would reject the call.
  class RequestID
    def server_unary(call, &) = echo(call, &)
    def server_stream(call, &) = echo(call, &)

    private

    def echo(call)
      # A call without the header, such as a plain curl request, passes through unchanged.
      id = call.context.request_metadata["demo-request-id"]&.first
      # Response headers written before yield go out with the handler's own.
      call.context.response_metadata.add("demo-request-id", id) if id
      yield
    end
  end

  # Concurrent calls share interceptor objects, so the list is built once.
  INTERCEPTORS = [RequestID.new].freeze
end
