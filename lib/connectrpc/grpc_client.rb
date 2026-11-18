# frozen_string_literal: true

module ConnectRPC
  class Client
    private

    def client_stream_strategy(lifecycle = nil)
      return Streaming::ConnectStrategy.new unless @protocol == :grpc

      GRPCProtocol::Strategy.new(codec: @status_codec, wire_limit: @grpc_status_wire_limit, limit: @grpc_status_limit,
                                 on_error: ->(error) { lifecycle&.record_failure(error) })
    end

    # The chain runs inside the scope's deadline and arbiter and outside the
    # exchange; its innermost continuation is today's single-request scope.
    def intercept_grpc_unary(chain, method, request, metadata, scope, state, deadline, lifecycle, injection, &) # rubocop:disable Metrics/ParameterLists, Metrics/MethodLength
      metadata = Metadata.new(metadata)
      lifecycle.request_metadata = Instrumentation.carrier(metadata) if lifecycle
      call = ClientUnaryCall.new(method: method, protocol: :grpc, request: request, request_metadata: metadata,
                                 deadline: deadline, cancellation: state.view)
      scope.intercept(chain, call) do
        dispatched = dispatch_metadata(call, lifecycle, injection)
        state.check!
        outgoing = stream_request(method, dispatched, deadline, state.view, lifecycle)
        open_client_stream(method, call.request, outgoing, scope, state, lifecycle, &)
        scope.result
      end
    end

    def grpc_stream_request(url, metadata, deadline, cancellation, lifecycle)
      headers = metadata.to_h.merge("content-type" => [GRPCProtocol::CONTENT_TYPE], "te" => ["trailers"],
                                    "grpc-accept-encoding" => [@accept_encoding.join(",")])
      headers["grpc-encoding"] = ["gzip"] if @encoding == :gzip
      headers["grpc-timeout"] = [GRPCProtocol.timeout(deadline)] if deadline
      StreamRequest.new(url: url, http_method: "POST", headers: default_user_agent(headers, GRPCProtocol::USER_AGENT),
                        deadline: deadline, cancellation: cancellation, lifecycle: lifecycle)
    end
  end
end
