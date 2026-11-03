# frozen_string_literal: true

module ConnectRPC
  module Streaming
    # Mutable state is local to one original processing task, never the Server.
    class ServerState
      attr_reader :headers, :codec, :publish, :lifecycle, :cancellation
      attr_accessor :context, :session, :encoding, :error

      def initialize(headers:, codec:, publish:, lifecycle:, cancellation:)
        @headers = headers
        @codec = codec
        @publish = publish
        @lifecycle = lifecycle
        @cancellation = cancellation
        @encoding = :identity
      end
    end
  end

  class Server
    private

    def execute_stream_handler(state, path, input, output)
      state.encoding = stream_encoding(state.headers)
      context = state.context = streaming_context(state)
      declaration = streaming_declaration(path)
      session = state.session = new_stream_session(state, declaration, input, output)
      invoke_stream_handler(@handlers.fetch(path), declaration, session, context)
      context.check!
    end

    def stream_encoding(headers)
      normalized = Streaming.coding_headers(headers)
      validate_headers!(normalized)
      request_encoding = Protocol.content_encoding(normalized) #: encoding
      Protocol.response_encoding(normalized, request_encoding, @encoding)
    end

    def streaming_declaration(path)
      declaration = @methods[path]
      return declaration if declaration && !declaration.is_a?(Method) && @handlers[path]

      raise Error.new(:unimplemented, "Unknown streaming method"), cause: nil
    end

    def new_stream_session(state, declaration, input, output)
      context = state.context #: StreamingContext
      coding = Protocol.content_encoding(Streaming.coding_headers(state.headers)) #: encoding
      Streaming::Session.new(binding: @binding, method: declaration, codec: state.codec, state: context,
                             input: input, output: output, request_limit: @request_limit,
                             response_limit: @response_limit, request_wire_limit: @request_wire_limit,
                             response_wire_limit: @response_wire_limit,
                             request_encoding: coding,
                             response_encoding: state.encoding, terminal_wire_limit: @terminal_wire_limit,
                             terminal_limit: @terminal_limit, server: true, context: state.context,
                             lifecycle: state.lifecycle)
    end

    def invoke_stream_handler(handler, declaration, session, context) # rubocop:disable Metrics/MethodLength
      context.check!
      case declaration
      when ServerStreamMethod
        ss = handler #: _ServerStreamHandler[untyped, untyped]
        request = single_stream_request(session)
        context.check!
        ss.call(request, ResponseWriter.new(session, context), context)
      when ClientStreamMethod
        cs = handler #: _ClientStreamHandler[untyped, untyped]
        session.send_message(cs.call(RequestReader.new(session), context))
      when BidiStreamMethod
        invoke_bidi_stream(handler, session, context)
      end
    end

    def invoke_bidi_stream(handler, session, context)
      bidi = handler #: _BidiStreamHandler[untyped, untyped]
      bidi.call(ServerDuplex.new(session, context), context)
    end

    def single_stream_request(session)
      request = session.receive
      return request if !request.nil? && session.receive.nil?

      raise Error.new(:unimplemented, "Expected exactly one request"), cause: nil
    end
  end
end
