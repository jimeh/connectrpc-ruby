# frozen_string_literal: true

module ConnectRPC
  module GRPCProtocol
    class ServerState
      attr_reader :headers, :codec, :publish, :lifecycle, :cancellation
      attr_accessor :context, :session, :encoding, :error, :published

      def initialize(headers:, codec:, publish:, lifecycle:, cancellation:)
        @headers = headers
        @codec = codec
        @publish = publish
        @lifecycle = lifecycle
        @cancellation = cancellation
        @encoding = :identity
        @published = false
      end
    end
  end

  class Server # rubocop:disable Metrics/ClassLength
    # Native entry point; portable adapters must never opt into HTTP/2 execution.
    def call_grpc(path:, http_method:, headers:, input:, output:, publish:, lifecycle: nil, cancellation: nil, # rubocop:disable Metrics/CyclomaticComplexity, Metrics/MethodLength
                  http2: false)
      # @type var owned: bool?
      owned = nil
      return bare(415, {}, cancellation: cancellation) unless http2 && @protocol.include?(:grpc)
      return bare(405, { "allow" => ["POST"] }, cancellation: cancellation) unless http_method == "POST"

      values = Protocol.headers(headers)
      unless GRPCProtocol.media?(Protocol.single(values, "content-type"))
        return bare(415, { "accept-post" => [GRPCProtocol::CONTENT_TYPE] }, cancellation: cancellation)
      end

      lifecycle, owned = begin_native_processing(path, values, lifecycle, http_method)
      work = lambda do
        process_grpc(path, values, input: input, output: output, publish: publish,
                                   lifecycle: lifecycle, cancellation: cancellation)
      end
      lifecycle ? lifecycle.activate(&work) : work.call
      nil
    ensure
      finish_native_processing(lifecycle, owned) if owned || lifecycle
    end

    private

    def process_grpc(path, headers, input:, output:, publish:, lifecycle:, cancellation:)
      # @type var state: GRPCProtocol::ServerState?
      active = state = GRPCProtocol::ServerState.new(headers: headers, codec: :proto, publish: publish,
                                                     lifecycle: lifecycle, cancellation: cancellation)
      capture_stream_error(active) { execute_grpc_handler(active, path, input, output) }
      render_grpc_terminal(active, output)
    ensure
      state&.session&.close
      state&.context&.close
    end

    def execute_grpc_handler(state, path, input, output) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      GRPCProtocol.check_fields!(state.headers)
      request_encoding = GRPCProtocol.encoding(state.headers, request: true, enabled: @encoding)
      state.encoding = GRPCProtocol.response_encoding(state.headers, @encoding)
      declaration = @methods[path]
      raise Error.new(:unimplemented, "Unknown method"), cause: nil unless declaration
      unless declaration.is_a?(Method) || @handlers[path]
        raise Error.new(:unimplemented, "Unknown streaming method"), cause: nil
      end

      context = state.context = grpc_context(state, declaration)
      session = state.session = Streaming::Session.new(
        binding: @binding, method: declaration, codec: :proto, state: context, input: input, output: output,
        request_limit: @request_limit, response_limit: @response_limit,
        request_wire_limit: @request_wire_limit, response_wire_limit: @response_wire_limit,
        request_encoding: request_encoding, response_encoding: state.encoding,
        terminal_wire_limit: @grpc_status_wire_limit, terminal_limit: @grpc_status_limit,
        server: true, context: context, lifecycle: state.lifecycle, publish: -> { publish_grpc_head(state) },
        strategy: GRPCProtocol::Strategy.new(codec: @status_codec, wire_limit: @grpc_status_wire_limit,
                                             limit: @grpc_status_limit)
      )
      if declaration.is_a?(Method)
        request = single_stream_request(session)
        context.check!
        session.send_message(@invoker.call(declaration, request, context))
      else
        streaming = context #: StreamingContext
        invoke_stream_handler(@handlers.fetch(path), declaration, session, streaming)
      end
      context.check!
    end

    def grpc_context(state, declaration = nil, headers: state.headers)
      metadata = Protocol.metadata(headers)
      state.lifecycle.request_metadata = Instrumentation.carrier(metadata) if state.lifecycle
      options = { request_metadata: metadata, deadline: GRPCProtocol.request_deadline(headers),
                  cancellation: state.cancellation, lifecycle: state.lifecycle }
      return Context.new(**options) if declaration.nil? || declaration.is_a?(Method)

      StreamingContext.new(publish: ->(snapshot) { publish_grpc_snapshot(state, snapshot) }, **options)
    end

    def publish_grpc_snapshot(state, metadata)
      state.publish.call(grpc_head(metadata, state.encoding))
      state.published = true
      nil
    end

    def publish_grpc_head(state)
      return if state.published

      context = state.context #: Context
      if context.is_a?(StreamingContext)
        context.send(:publish_headers, check: false)
      else
        context.response_metadata.freeze
        publish_grpc_snapshot(state, context.response_metadata)
      end
      raise(state.error || Error.new(:internal, "gRPC headers were not published")) unless state.published
    end

    def grpc_head(metadata, encoding)
      fields = metadata.to_h.merge("content-type" => [GRPCProtocol::CONTENT_TYPE], "grpc-accept-encoding" => [
                                     @encoding == :gzip ? "gzip,identity" : "identity"
                                   ])
      fields["grpc-encoding"] = ["gzip"] if encoding == :gzip
      GRPCProtocol.check_fields!(fields.merge(":status" => ["200"]))
      GRPCHead.new(status: 200, headers: fields, end_stream: false)
    end

    def grpc_terminal_fields(state, context)
      merge_stream_error_metadata(state.error, context)
      codec = @status_codec #: _GRPCStatusCodec
      GRPCProtocol.terminal(
        state.error, stream_trailers(state.error, context),
        codec: codec,
        wire_limit: @grpc_status_wire_limit, limit: @grpc_status_limit,
        on_error: lambda { |error|
          state.lifecycle&.record_failure(error)
        }
      )
    end

    def render_grpc_terminal(state, output)
      context = state.context ||= grpc_context(state, headers: {})
      fields, effective = grpc_terminal_fields(state, context)
      observe_stream_result(state, effective) if state.error
      publish_grpc_head(state)
      output.finish(headers: fields)
      context.commit! unless state.error
      observe_stream_result(state, effective) unless state.error
    end
  end
end
