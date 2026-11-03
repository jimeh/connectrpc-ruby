# frozen_string_literal: true

module ConnectRPC
  class Server
    # Explicit native-host SPI. Portable adapters retain the unary entry point.
    def call_stream(path:, http_method:, headers:, input:, output:, publish:, lifecycle: nil, cancellation: nil, # rubocop:disable Metrics/MethodLength
                    http2: false)
      unless http2
        return call(path: path, http_method: http_method, headers: headers, input: input,
                    lifecycle: lifecycle, cancellation: cancellation)
      end

      return bare(415, {}, cancellation: cancellation) unless @protocol.include?(:connect)

      values = Protocol.headers(headers)
      codec = Streaming.codec(Protocol.single(values, "content-type"))
      rejection = stream_preflight(http_method, codec, cancellation)
      return rejection if rejection || !codec

      execute_native_stream(path, values, codec, http_method: http_method,
                                                 input: input, output: output, publish: publish,
                                                 lifecycle: lifecycle, cancellation: cancellation)
    end

    private

    def stream_preflight(http_method, codec, cancellation)
      return bare(405, { "allow" => ["POST"] }, cancellation: cancellation) unless http_method == "POST"
      return if codec

      bare(415, { "accept-post" => [Streaming::CONTENT_TYPES.values.join(", ")] }, cancellation: cancellation)
    end

    def execute_native_stream(path, headers, codec, http_method:, input:, output:, publish:, lifecycle:, cancellation:)
      # @type var owned: bool?
      lifecycle, owned = begin_native_processing(path, headers, lifecycle, http_method)
      work = lambda {
        process_stream(path, headers, codec, input: input, output: output, publish: publish,
                                             lifecycle: lifecycle, cancellation: cancellation)
      }
      lifecycle ? lifecycle.activate(&work) : work.call
      nil
    ensure
      finish_native_processing(lifecycle, owned)
    end

    def begin_native_processing(path, headers, lifecycle, http_method)
      owned = lifecycle.nil?
      lifecycle ||= start_call(path: path, headers: headers, http_method: http_method)
      lifecycle&.begin_processing(self, path)
      [lifecycle, owned]
    end

    def finish_native_processing(lifecycle, owned)
      return unless lifecycle

      lifecycle.processing_finished(code: nil, error: $ERROR_INFO) unless lifecycle.outcome
      lifecycle.finish(delivery: :unobserved) if owned
    end

    def process_stream(path, headers, codec, input:, output:, publish:, lifecycle:, cancellation:)
      # @type var state: Streaming::ServerState?
      active = state = Streaming::ServerState.new(headers: headers, codec: codec, publish: publish,
                                                  lifecycle: lifecycle, cancellation: cancellation)
      capture_stream_error(active) { execute_stream_handler(active, path, input, output) }
      render_stream_terminal(active, output)
    ensure
      state&.session&.close
      state&.context&.close
    end

    def capture_stream_error(state)
      yield
    rescue StandardError => e
      state.context&.record_failure(e)
      state.lifecycle&.record_failure(e)
      safe = e.is_a?(Error) ? e : Error.new(:internal, "Internal error")
      state.error = terminal_error(safe, state.context, state.cancellation)
    end
  end
end
