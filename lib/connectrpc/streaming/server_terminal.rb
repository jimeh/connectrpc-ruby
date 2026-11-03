# frozen_string_literal: true

module ConnectRPC
  class Server
    private

    def render_stream_terminal(state, output)
      context = state.context ||= streaming_context(state, headers: {})
      merge_stream_error_metadata(state.error, context)
      bytes, effective = stream_terminal_bytes(state, context)
      handler_error = state.error
      # A peer reset may reject terminal delivery. Retain the handler failure
      # independently of that later delivery error.
      observe_stream_result(state, effective) if handler_error
      context.send(:publish_headers, check: false)
      emit_stream_terminal(bytes, output)
      context.commit! unless handler_error
      observe_stream_result(state, effective) unless handler_error
    end

    def stream_terminal_bytes(state, context)
      trailers = stream_trailers(state.error, context)
      Streaming.terminal_bytes(state.error, trailers, wire_limit: @terminal_wire_limit, limit: @terminal_limit)
    end

    def merge_stream_error_metadata(error, context)
      return unless error && !context.response_metadata.frozen?

      error.metadata.each do |key, values|
        context.response_metadata[key] = (context.response_metadata[key] || []) + values
      end
    end

    def stream_trailers(error, context)
      trailers = Metadata.new(context.response_trailers)
      error&.trailers&.each { |key, values| trailers[key] = (trailers[key] || []) + values }
      context.response_trailers.freeze
      trailers
    end

    def emit_stream_terminal(bytes, output)
      framing = Streaming::Framing.new(input: nil, output: output, request: false,
                                       limit: @response_limit, wire_limit: @response_wire_limit,
                                       terminal_wire_limit: @terminal_wire_limit, terminal_limit: @terminal_limit)
      framing.emit(bytes, terminal: true)
    end

    def observe_stream_result(state, effective)
      context = state.context #: StreamingContext
      state.lifecycle&.processing_finished(code: effective ? effective.code : "OK", error: state.error,
                                           normalized_error: effective, http_status: 200,
                                           response_metadata: Instrumentation.carrier(context.response_metadata))
    end

    def streaming_context(state, headers: state.headers)
      metadata = Protocol.metadata(headers)
      state.lifecycle.request_metadata = Instrumentation.carrier(metadata) if state.lifecycle
      leading = ->(snapshot) { state.publish.call(stream_head(snapshot, state.codec, state.encoding)) }
      StreamingContext.new(publish: leading, request_metadata: metadata, deadline: request_deadline(headers),
                           cancellation: state.cancellation, lifecycle: state.lifecycle)
    end

    def stream_head(metadata, codec, encoding)
      values = metadata.to_h.merge("content-type" => [Streaming::CONTENT_TYPES.fetch(codec)])
      values["connect-content-encoding"] = ["gzip"] if encoding == :gzip
      values["connect-accept-encoding"] = ["gzip, identity"] if @encoding == :gzip
      StreamHead.new(status: 200, headers: values)
    end
  end
end
