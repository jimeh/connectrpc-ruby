# frozen_string_literal: true

module ConnectRPC
  class Client
    private

    def new_client_stream_session(method, state, exchange, lifecycle)
      Streaming::Session.new(binding: @binding, method: method, codec: @codec, state: state,
                             input: exchange, output: exchange, request_limit: @request_limit,
                             response_limit: @response_limit, request_wire_limit: @request_wire_limit,
                             response_wire_limit: @response_wire_limit, request_encoding: @encoding,
                             response_encoding: :identity, accept_encoding: @accept_encoding,
                             terminal_wire_limit: @terminal_wire_limit, terminal_limit: @terminal_limit,
                             lifecycle: lifecycle, strategy: client_stream_strategy(lifecycle))
    end

    # The stream chain runs like the unary one. Its innermost continuation
    # opens the stream; perform_stream_scope applies the call's wrappers.
    def intercept_stream(chain, method, request, metadata, scope, state, deadline, lifecycle, injection, &) # rubocop:disable Metrics/ParameterLists
      metadata = Metadata.new(metadata)
      lifecycle.request_metadata = Instrumentation.carrier(metadata) if lifecycle
      call = new_client_stream_call(method, request, metadata, deadline, state)
      scope.intercept(chain, call) do
        dispatched = dispatch_metadata(call, lifecycle, injection)
        state.check!
        outgoing = stream_request(method, dispatched, deadline, state.view, lifecycle)
        open_client_stream(method, nil, outgoing, scope, state, lifecycle, call, &)
        scope.result
      end
    end

    def new_client_stream_call(method, request, metadata, deadline, state)
      if method.is_a?(ServerStreamMethod)
        return ClientStreamCall::ServerStreaming.new(method: method, protocol: @protocol, request: request,
                                                     request_metadata: metadata, deadline: deadline,
                                                     cancellation: state.view)
      end

      ClientStreamCall.new(method: method, protocol: @protocol, request_metadata: metadata, deadline: deadline,
                           cancellation: state.view)
    end

    def with_stream_session(session, scope) # rubocop:disable Metrics/MethodLength
      yield
    rescue StandardError => e
      scope.failure = e
      scope.delivered_error = e if session.remote_completed?(e)
      recovered = session.recover_send_failure if session.send_failure?(e)
      if recovered
        scope.delivered_error = recovered if session.remote_completed?(recovered)
        raise recovered, cause: nil
      end

      raise
    ensure
      begin
        scope.cleanup(session)
      ensure
        # A kill or throw during abandonment still closes the session.
        session.close
      end
    end

    # Wrappers apply to the raw endpoint before the server-streaming request
    # is sent and before the application block runs.
    def perform_stream_scope(method, request, session, scope, call = nil)
      endpoint = stream_endpoint(method, session)
      endpoint = scope.wrap(call, endpoint) if call
      request = call.request if call.is_a?(ClientStreamCall::ServerStreaming)
      prepare_server_stream(method, request, session)
      yield_stream_endpoint(session, scope) { yield endpoint }
      scope.abandoned = session.abandoned?
      scope.result = finish_stream_scope(method, session)
      scope.completed = !scope.abandoned
    end

    def prepare_server_stream(method, request, session)
      return unless method.is_a?(ServerStreamMethod) || method.is_a?(Method)

      session.send_message(request)
      session.close_send
    end

    def yield_stream_endpoint(session, scope)
      yield
    rescue Exception => e # rubocop:disable Lint/RescueException -- Preserve fatal user exceptions through transport cleanup.
      scope.record_user_error(e) unless session.owned_error?(e)
      raise
    end

    def stream_endpoint(method, session)
      case method
      when ServerStreamMethod then ResponseReader.new(session)
      when ClientStreamMethod then RequestWriter.new(session)
      else ClientDuplex.new(session)
      end
    end

    def finish_stream_scope(method, session)
      # Unary and client-streaming results are frozen snapshots.
      if method.is_a?(ClientStreamMethod) || method.is_a?(Method)
        return Interceptors.response!(client_stream_response(session))
      end
      return if session.abandoned?
      return session.receive if session.consumed?

      raise StreamNotConsumedError, "Response stream was not consumed"
    end

    def client_stream_response(session)
      raise StreamNotConsumedError, "Client streaming was abandoned" if session.abandoned?

      session.close_send
      message = session.receive
      raise Error.new(:unimplemented, "Expected exactly one response"), cause: nil if message.nil?

      if @protocol == :grpc
        # Surplus messages are counted, not decoded; the trailers decide.
        session.receive(skip_data: true) until session.consumed?
      else
        session.receive
      end
      Response.new(message: message, metadata: session.headers, trailers: session.trailers || Metadata.new)
    end
  end
end
