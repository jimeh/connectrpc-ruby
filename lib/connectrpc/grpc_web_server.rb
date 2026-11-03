# frozen_string_literal: true

module ConnectRPC
  module GRPCWeb
    class ServerState
      attr_reader :headers, :publish, :lifecycle, :cancellation, :representation, :path
      attr_accessor :context, :session, :error, :published

      def initialize(path:, headers:, representation:, publish:, lifecycle:, cancellation:)
        @path = path
        @headers = headers
        @representation = representation
        @publish = publish
        @lifecycle = lifecycle
        @cancellation = cancellation
        @published = false
      end
    end
  end

  class Server # rubocop:disable Metrics/ClassLength
    # HTTP-version-neutral publication. Hosts stage finish's terminal until the
    # original application task has completed all outer cleanup successfully.
    def call_grpc_web(path:, http_method:, headers:, input:, output:, publish:, capabilities: [:unary], # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
                      lifecycle: nil, cancellation: nil)
      # @type var owned: bool?
      owned = nil
      GRPCWeb.capabilities!(capabilities)
      values = Protocol.headers(headers)
      early = cors_response(path: path, http_method: http_method, headers: values)
      return early if early

      lifecycle, owned = begin_native_processing(path, values, lifecycle, http_method)
      work = lambda do
        rejection = web_preflight(http_method, values, cancellation)
        if rejection
          lifecycle&.processing_finished(code: nil, http_status: rejection.status)
          next cors_response(path: path, http_method: http_method, headers: values, response: rejection) || rejection
        end

        request = GRPCWeb.media(Protocol.single(values, "content-type")) #: GRPCWeb::representation
        representation = GRPCWeb.representation(values, request) #: GRPCWeb::representation
        state = GRPCWeb::ServerState.new(path: path, headers: values, representation: representation,
                                         publish: publish, lifecycle: lifecycle, cancellation: cancellation)
        process_grpc_web(state, input, output, request, capabilities)
        nil
      end
      lifecycle ? lifecycle.activate(&work) : work.call
    rescue Exception => e # rubocop:disable Lint/RescueException -- discard staged status and preserve the original failure
      lifecycle&.record_failure(e)
      begin
        output.abort unless owned.nil?
      rescue StandardError => cleanup_error
        lifecycle&.record_failure(cleanup_error)
      end
      raise
    ensure
      finish_native_processing(lifecycle, owned) unless owned.nil?
    end

    private

    def web_preflight(http_method, headers, cancellation)
      fields = { "vary" => ["Accept"] }
      return bare(415, fields, cancellation: cancellation) unless @protocol.include?(:grpc_web)
      return bare(405, fields.merge("allow" => ["POST"]), cancellation: cancellation) unless http_method == "POST"

      representation = GRPCWeb.media(Protocol.single(headers, "content-type"))
      unless representation
        return bare(415, fields.merge("accept-post" => [GRPCWeb::CONTENT_TYPES.values.join(", ")]),
                    cancellation: cancellation)
      end
      return if GRPCWeb.representation(headers, representation)

      bare(406, fields, cancellation: cancellation)
    end

    def call_grpc_web_unary(path, http_method, headers, input, lifecycle, cancellation) # rubocop:disable Metrics/MethodLength, Metrics/ParameterLists
      # Separately padded writes can expand every raw byte by four. The bounded
      # aggregate includes one data prefix and the separately staged terminal.
      output = GRPCWeb::Aggregate.new(4 * (5 + @response_wire_limit + 5 + @grpc_status_wire_limit))
      head = nil #: StreamHead?
      response = call_grpc_web(path: path, http_method: http_method, headers: headers,
                               input: GRPCWeb::PortableInput.new(input), output: output,
                               lifecycle: lifecycle, cancellation: cancellation,
                               publish: ->(published) { head = published })
      return response if response

      output.complete
      published = head #: StreamHead
      TransportResponse.new(status: published.status, headers: published.headers, body: output.body)
    rescue Exception # rubocop:disable Lint/RescueException -- discard staged status, preserve arbitrary cleanup failure
      output&.abort
      raise
    end

    def process_grpc_web(state, input, output, request, capabilities)
      prepared = GRPCWeb::Output.new(output, state.representation)
      capture_stream_error(state) { execute_grpc_web(state, input, prepared, request, capabilities) }
      render_grpc_web_terminal(state, prepared)
    ensure
      state.session&.close
      state.context&.close
    end

    def execute_grpc_web(state, input, output, representation, capabilities) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
      GRPCProtocol.check_fields!(state.headers)
      declaration = @methods[state.path]
      allowed = if declaration.is_a?(Method)
                  capabilities.include?(:unary)
                else
                  declaration.is_a?(ServerStreamMethod) && capabilities.include?(:server_stream) &&
                    @handlers[state.path]
                end
      raise Error.new(:unimplemented, "Unsupported Web method"), cause: nil unless allowed

      GRPCProtocol.encoding(state.headers, request: true, enabled: :identity)
      unless Protocol.identity_encoding?(state.headers)
        raise Error.new(:unimplemented, "Web requires identity HTTP encoding"), cause: nil
      end

      context = state.context = web_context(state, declaration)
      incoming = GRPCWeb::Input.new(input, representation: representation, wire_limit: @request_wire_limit,
                                           headers: state.headers)
      session = state.session = Streaming::Session.new(
        binding: @binding, method: declaration, codec: :proto, state: context, input: incoming, output: output,
        request_limit: @request_limit, response_limit: @response_limit,
        request_wire_limit: @request_wire_limit, response_wire_limit: @response_wire_limit,
        request_encoding: :identity, response_encoding: :identity,
        terminal_wire_limit: @grpc_status_wire_limit, terminal_limit: @grpc_status_limit,
        server: true, context: context, lifecycle: state.lifecycle,
        publish: -> { publish_web_head(state) }, strategy: GRPCWeb::Strategy.new
      )
      message = session.receive
      if message.nil? || incoming.read(1, timeout: context.deadline&.remaining)
        raise Error.new(:unimplemented, "Expected exactly one request"), cause: nil
      end

      context.check!
      if declaration.is_a?(Method)
        session.send_message(@invoker.call(declaration, message, context))
      else
        handler = @handlers.fetch(state.path) #: _ServerStreamHandler[untyped, untyped]
        streaming = context #: StreamingContext
        handler.call(message, ResponseWriter.new(session, streaming), streaming)
      end
      context.check!
    end

    def web_context(state, declaration = nil, headers: state.headers)
      metadata = Protocol.metadata(headers)
      state.lifecycle.request_metadata = Instrumentation.carrier(metadata) if state.lifecycle
      options = { request_metadata: metadata, deadline: GRPCProtocol.request_deadline(headers),
                  cancellation: state.cancellation, lifecycle: state.lifecycle }
      return Context.new(**options) unless declaration.is_a?(ServerStreamMethod)

      StreamingContext.new(publish: ->(snapshot) { publish_web_snapshot(state, snapshot) }, **options)
    end

    def publish_web_head(state)
      return if state.published

      context = state.context #: Context
      if context.is_a?(StreamingContext)
        context.send(:publish_headers, check: false)
      else
        context.response_metadata.freeze
        publish_web_snapshot(state, context.response_metadata)
      end
    end

    def publish_web_snapshot(state, metadata) # rubocop:disable Metrics/AbcSize
      fields = metadata.to_h.merge("content-type" => [GRPCWeb::CONTENT_TYPES.fetch(state.representation)],
                                   "grpc-accept-encoding" => ["identity"])
      GRPCWeb.vary(fields, "Accept")
      response = TransportResponse.new(status: 200, headers: fields, body: "")
      response = cors_response(path: state.path, http_method: "POST", headers: state.headers,
                               response: response) || response
      GRPCProtocol.check_fields!(response.headers.merge(":status" => [response.status.to_s]))
      state.publish.call(StreamHead.new(status: response.status, headers: response.headers))
      state.published = true
      nil
    end

    def render_grpc_web_terminal(state, output) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      context = state.context ||= web_context(state, headers: {})
      merge_stream_error_metadata(state.error, context)
      codec = @status_codec #: _GRPCStatusCodec
      bytes, effective = GRPCWeb.terminal(
        state.error, stream_trailers(state.error, context),
        codec: codec,
        wire_limit: @grpc_status_wire_limit, limit: @grpc_status_limit,
        on_error: lambda { |error|
          state.lifecycle&.record_failure(error)
        }
      )
      observe_stream_result(state, effective) if state.error
      publish_web_head(state)
      output.finish(terminal: bytes)
      context.commit! unless state.error
      observe_stream_result(state, effective) unless state.error
    end
  end
end
