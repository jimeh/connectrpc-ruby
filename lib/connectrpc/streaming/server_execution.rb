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
      invoke_stream_handler(@handlers.fetch(path), declaration, session, context, :connect)
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

    def invoke_stream_handler(handler, declaration, session, context, protocol)
      context.check!
      # Server streaming reads and checks its single request before the chain.
      if declaration.is_a?(ServerStreamMethod)
        request = single_stream_request(session)
        context.check!
      end
      serve_stream(handler, declaration, session, context, request, protocol)
    end

    # Every stream handler call site runs the stream chain here. Core sends a
    # client-streaming result after the outermost interceptor returns it.
    def serve_stream(handler, declaration, session, context, request, protocol) # rubocop:disable Metrics/ParameterLists
      chain = @stream_interceptors
      result = if chain
                 intercept_stream_handler(chain, handler, declaration, session, context, request, protocol)
               else
                 endpoint = server_stream_endpoint(declaration, session, context)
                 call_stream_handler(handler, declaration, endpoint, context, request)
               end
      session.send_message(result) if declaration.is_a?(ClientStreamMethod)
    end

    def intercept_stream_handler(chain, handler, declaration, session, context, request, protocol) # rubocop:disable Metrics/ParameterLists, Metrics/MethodLength
      call = if declaration.is_a?(ServerStreamMethod)
               ServerStreamCall::ServerStreaming.new(method: declaration, protocol: protocol, request: request,
                                                     context: context)
             else
               ServerStreamCall.new(method: declaration, protocol: protocol, context: context)
             end
      chain.run(call, context) do
        call.__send__(:dispatch!)
        endpoint = call.__send__(:wrap, server_stream_endpoint(declaration, session, context))
        request = call.request if call.is_a?(ServerStreamCall::ServerStreaming)
        call_stream_handler(handler, declaration, endpoint, context, request)
      end
    end

    def server_stream_endpoint(declaration, session, context)
      case declaration
      when ServerStreamMethod then ResponseWriter.new(session, context)
      when ClientStreamMethod then RequestReader.new(session)
      else ServerDuplex.new(session, context)
      end
    end

    def call_stream_handler(handler, declaration, endpoint, context, request) # rubocop:disable Metrics/MethodLength
      case declaration
      when ServerStreamMethod
        ss = handler #: _ServerStreamHandler[untyped, untyped]
        ss.call(request, endpoint, context)
      when ClientStreamMethod
        cs = handler #: _ClientStreamHandler[untyped, untyped]
        cs.call(endpoint, context)
      else
        bidi = handler #: _BidiStreamHandler[untyped, untyped]
        bidi.call(endpoint, context)
      end
    end

    def single_stream_request(session)
      request = session.receive
      return request if !request.nil? && session.request_complete?

      raise Error.new(:unimplemented, "Expected exactly one request"), cause: nil
    end
  end
end
