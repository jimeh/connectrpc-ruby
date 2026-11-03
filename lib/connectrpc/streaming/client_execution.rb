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
                             lifecycle: lifecycle)
    end

    def with_stream_session(session, scope)
      yield
    rescue StandardError => e
      scope.failure = e
      recovered = session.recover_send_failure if session.send_failure?(e)
      raise recovered, cause: nil if recovered

      raise
    ensure
      scope.cleanup(session)
      session.close
    end

    def perform_stream_scope(method, request, session, scope)
      prepare_server_stream(method, request, session)
      yield_stream_endpoint(session, scope) { yield stream_endpoint(method, session) }
      scope.abandoned = session.abandoned?
      scope.result = finish_stream_scope(method, session)
      scope.completed = !scope.abandoned
    end

    def prepare_server_stream(method, request, session)
      return unless method.is_a?(ServerStreamMethod)

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
      return client_stream_response(session) if method.is_a?(ClientStreamMethod)
      return if session.abandoned?
      return session.receive if session.consumed?

      raise StreamNotConsumedError, "Response stream was not consumed"
    end

    def client_stream_response(session)
      raise StreamNotConsumedError, "Client streaming was abandoned" if session.abandoned?

      session.close_send
      message = session.receive
      raise Error.new(:unimplemented, "Expected exactly one response"), cause: nil if message.nil?

      session.receive
      Response.new(message: message, metadata: session.headers, trailers: session.trailers || Metadata.new)
    end
  end
end
