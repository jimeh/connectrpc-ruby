# frozen_string_literal: true

module ConnectRPC
  class Client
    def server_stream(method, request, metadata: {}, timeout: 30, deadline: nil, cancellation: nil, &block)
      validate_stream_call!(method, ServerStreamMethod, deadline, cancellation, block)
      stream_call(method, request, metadata, [timeout, deadline], cancellation, &block)
    end

    def client_stream(method, metadata: {}, timeout: 30, deadline: nil, cancellation: nil, &block)
      validate_stream_call!(method, ClientStreamMethod, deadline, cancellation, block)
      stream_call(method, nil, metadata, [timeout, deadline], cancellation, &block)
    end

    def bidi_stream(method, metadata: {}, timeout: 30, deadline: nil, cancellation: nil, &block)
      validate_stream_call!(method, BidiStreamMethod, deadline, cancellation, block)
      stream_call(method, nil, metadata, [timeout, deadline], cancellation, &block)
    end

    private

    def validate_stream_call!(method, type, deadline, cancellation, block)
      raise ArgumentError, "Wrong streaming declaration" unless method.is_a?(type)
      raise ArgumentError, "A streaming block is required" unless block

      validate_call_options!(deadline, cancellation)
      return if stream_capability?(method)

      raise ArgumentError, "Transport does not support #{method.cardinality}"
    end

    def stream_capability?(method)
      if @protocol == :grpc
        candidate = @transport #: (_Transport | _StreamingTransport | _GRPCTransport) & _GRPCCapabilityProbe
        return false unless candidate.respond_to?(:grpc_capabilities) && candidate.respond_to?(:open_grpc_stream)

        grpc = @transport #: _GRPCTransport
        return grpc.grpc_capabilities.include?(method.cardinality)
      end
      capability = @transport #: _StreamingTransport
      streaming_transport? && capability.streaming_capabilities.include?(method.cardinality)
    end

    def streaming_transport?
      # Runtime reflection is confined here; unary structural callers need only call.
      transport = @transport #: (_Transport | _StreamingTransport) & _StreamCapabilityProbe
      transport.respond_to?(:streaming_capabilities) && transport.respond_to?(:open_stream)
    end

    def stream_call(method, request, metadata, limit, cancellation, &)
      injection = {} #: headers
      lifecycle = @instrumentation&.start(owner: self, role: :client, method: method, carrier: injection,
                                          protocol: @protocol, **request_facts(method))
      lifecycle&.begin_processing(self)
      work = lambda do
        run_stream(method, request, metadata, limit: limit, cancellation: cancellation,
                                              lifecycle: lifecycle, injection: injection, &)
      end
      lifecycle ? lifecycle.activate(&work) : work.call
    end

    def run_stream(method, request, metadata, limit:, cancellation:, lifecycle:, injection:, &) # rubocop:disable Metrics/MethodLength
      scope = Streaming::ClientScope.new(lifecycle)
      chain = method.is_a?(Method) ? @unary_interceptors : @stream_interceptors
      scope.run(limit, cancellation) do |state, deadline|
        if chain && method.is_a?(Method)
          intercept_grpc_unary(chain, method, request, metadata, scope, state, deadline, lifecycle, injection, &)
        elsif chain
          stream = method #: stream_declaration
          intercept_stream(chain, stream, request, metadata, scope, state, deadline, lifecycle, injection, &)
        else
          outgoing = stream_request(method, stream_metadata(metadata, injection, lifecycle), deadline, state.view,
                                    lifecycle)
          open_client_stream(method, request, outgoing, scope, state, lifecycle, &)
        end
      end
    end

    def open_client_stream(method, request, outgoing, scope, state, lifecycle, call = nil, &) # rubocop:disable Metrics/ParameterLists, Metrics/MethodLength
      transport = @transport #: _StreamingTransport
      work = lambda do |exchange|
        session = scope.session = new_client_stream_session(method, state, exchange, lifecycle)
        with_stream_session(session, scope) { perform_stream_scope(method, request, session, scope, call, &) }
      end
      if @protocol == :grpc
        grpc = @transport #: _GRPCTransport
        grpc.open_grpc_stream(outgoing, &work)
      else
        transport.open_stream(outgoing, &work)
      end
    end

    def stream_metadata(metadata, injection, lifecycle)
      metadata = Metadata.new(metadata)
      metadata = injected_metadata(metadata, injection) if lifecycle
      lifecycle.request_metadata = Instrumentation.carrier(metadata) if lifecycle
      metadata
    end

    def stream_request(method, metadata, deadline, cancellation, lifecycle)
      url = @base_url.dup
      url.path = request_path(method)
      return grpc_stream_request(url, metadata, deadline, cancellation, lifecycle) if @protocol == :grpc

      headers = metadata.to_h.merge("content-type" => [Streaming::CONTENT_TYPES.fetch(@codec)],
                                    "connect-protocol-version" => ["1"],
                                    "connect-accept-encoding" => [@accept_encoding.join(", ")])
      headers["connect-content-encoding"] = ["gzip"] if @encoding == :gzip
      headers["connect-timeout-ms"] = [deadline.timeout_ms] if deadline
      StreamRequest.new(url: url, http_method: "POST", headers: default_user_agent(headers, Protocol::USER_AGENT),
                        deadline: deadline, cancellation: cancellation, lifecycle: lifecycle)
    end
  end
end
