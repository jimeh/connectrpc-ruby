# frozen_string_literal: true

module ConnectRPC
  class Server
    private

    def render_stream_terminal(state, output)
      context = state.context ||= streaming_context(state, headers: {})
      fail_unpublished_head(state, "Connect") if context.send(:head_failed?)
      bytes, effective, trailers = stream_terminal_bytes(state, context)
      handler_error = state.error
      # A peer reset may reject terminal delivery. Retain the handler failure
      # independently of that later delivery error.
      observe_stream_result(state, effective, trailers) if handler_error
      publish_stream_head(context, handler_error)
      emit_stream_terminal(bytes, output)
      context.commit! unless handler_error
      observe_stream_result(state, effective, trailers) unless handler_error
    end

    # A failed head publication is not retried and a stream never finishes
    # without a head. Stop before serializing the terminal, so a terminal that
    # exceeds its limits cannot replace the publication failure.
    def fail_unpublished_head(state, protocol)
      error = state.error
      observe_stream_result(state, error) if error
      raise(error || Error.new(:internal, "#{protocol} headers were not published"))
    end

    # Error-path observation runs first and can attempt the first publication,
    # so check again after publishing; a failed attempt is never retried.
    def publish_stream_head(context, error)
      context.send(:publish_headers, check: false)
      return unless context.send(:head_failed?)

      raise(error || Error.new(:internal, "Connect headers were not published"))
    end

    def grpc_head_failed?(state, context)
      return context.send(:head_failed?) if context.is_a?(StreamingContext)

      state.head_attempted && !state.published
    end

    def stream_terminal_bytes(state, context)
      trailers = stream_trailers(state.error, context)
      bytes, effective = Streaming.terminal_bytes(state.error, trailers, wire_limit: @terminal_wire_limit,
                                                                         limit: @terminal_limit)
      [bytes, effective, emitted_trailers(trailers, state.error, effective)]
    end

    # Every replacement terminal (byte limit or status codec failure) carries
    # a new error and none of the application trailers.
    def emitted_trailers(trailers, error, effective)
      effective.equal?(error) ? trailers : Metadata.new
    end

    # gRPC and gRPC-Web write one field line per value, so a name without
    # values is not on their wire. Connect's JSON terminal keeps it.
    def emitted_fields(trailers, error, effective)
      emitted_trailers(trailers, error, effective).to_h.reject { |_, values| values.empty? }
    end

    # Error metadata joins a head that is still unpublished. Once the head is
    # committed (frozen), it follows the response trailers in the terminal,
    # ahead of the error's own trailers, as connect-go places it.
    def stream_trailers(error, context)
      trailers = Metadata.new(context.response_trailers)
      if error
        head = context.response_metadata
        append_metadata(head.frozen? ? trailers : head, error.metadata)
        append_metadata(trailers, error.trailers)
      end
      context.response_trailers.freeze
      trailers
    end

    def append_metadata(target, source)
      source.each { |key, values| target[key] = (target[key] || []) + values }
    end

    def emit_stream_terminal(bytes, output)
      framing = Streaming::Framing.new(input: nil, output: output, request: false,
                                       limit: @response_limit, wire_limit: @response_wire_limit,
                                       terminal_wire_limit: @terminal_wire_limit, terminal_limit: @terminal_limit)
      framing.emit(bytes, terminal: true)
    end

    # Trailers are those the rendered terminal carries; none when no terminal is serialized.
    def observe_stream_result(state, effective, trailers = Metadata.new)
      context = state.context #: StreamingContext
      state.lifecycle&.processing_finished(code: effective ? effective.code : "OK", error: state.error,
                                           normalized_error: effective, http_status: 200,
                                           response_metadata: Instrumentation.carrier(context.response_metadata),
                                           response_trailers: Instrumentation.carrier(trailers))
    end

    def streaming_context(state, headers: state.headers)
      leading = ->(snapshot) { state.publish.call(stream_head(snapshot, state.codec, state.encoding)) }
      StreamingContext.new(publish: leading, request_metadata: Protocol.metadata(headers),
                           deadline: request_deadline(headers),
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
