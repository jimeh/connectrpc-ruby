# frozen_string_literal: true

module ConnectRPC
  class Client
    def server_stream(method, request, metadata: {}, timeout: 30, cancellation: nil, &block)
      validate_stream_call!(method, ServerStreamMethod, cancellation, block)
      stream_call(method, request, metadata, timeout, cancellation, &block)
    end

    def client_stream(method, metadata: {}, timeout: 30, cancellation: nil, &block)
      validate_stream_call!(method, ClientStreamMethod, cancellation, block)
      stream_call(method, nil, metadata, timeout, cancellation, &block)
    end

    def bidi_stream(method, metadata: {}, timeout: 30, cancellation: nil, &block)
      validate_stream_call!(method, BidiStreamMethod, cancellation, block)
      stream_call(method, nil, metadata, timeout, cancellation, &block)
    end

    private

    def validate_stream_call!(method, type, cancellation, block)
      raise ArgumentError, "Wrong streaming declaration" unless method.is_a?(type)
      raise ArgumentError, "A streaming block is required" unless block
      unless cancellation.nil? || cancellation.is_a?(CancellationToken)
        raise ArgumentError, "cancellation must be a CancellationToken or nil"
      end
      return if stream_capability?(method)

      raise ArgumentError, "Transport does not support #{method.cardinality}"
    end

    def stream_capability?(method)
      capability = @transport #: _StreamingTransport
      @transport.respond_to?(:streaming_capabilities) && @transport.respond_to?(:open_stream) &&
        capability.streaming_capabilities.include?(method.cardinality)
    end

    def stream_call(method, request, metadata, timeout, cancellation, &)
      injection = {} #: headers
      lifecycle = @instrumentation&.start(owner: self, role: :client, method: method, carrier: injection,
                                          server_address: @base_url.hostname.freeze, server_port: @base_url.port)
      lifecycle&.begin_processing(self)
      work = lambda do
        run_stream(method, request, metadata, timeout: timeout, cancellation: cancellation,
                                              lifecycle: lifecycle, injection: injection, &)
      end
      lifecycle ? lifecycle.activate(&work) : work.call
    end

    def run_stream(method, request, metadata, timeout:, cancellation:, lifecycle:, injection:, &)
      scope = Streaming::ClientScope.new(lifecycle)
      scope.run(timeout, cancellation) do |state, deadline|
        outgoing = stream_request(method, stream_metadata(metadata, injection, lifecycle), deadline, state, lifecycle)
        transport = @transport #: _StreamingTransport
        transport.open_stream(outgoing) do |exchange|
          session = new_client_stream_session(method, state, exchange, lifecycle)
          with_stream_session(session, scope) { perform_stream_scope(method, request, session, scope, &) }
        end
      end
    end

    def stream_metadata(metadata, injection, lifecycle)
      metadata = Metadata.new(metadata)
      metadata = injected_metadata(metadata, injection) if lifecycle
      lifecycle.request_metadata = Instrumentation.carrier(metadata) if lifecycle
      metadata
    end

    def stream_request(method, metadata, deadline, state, lifecycle)
      url = @base_url.dup
      base_path = @base_url.path #: String
      url.path = base_path.delete_suffix("/") + method.path
      headers = metadata.to_h.merge("content-type" => [Streaming::CONTENT_TYPES.fetch(@codec)],
                                    "connect-protocol-version" => ["1"],
                                    "connect-accept-encoding" => [@accept_encoding.join(", ")])
      headers["connect-content-encoding"] = ["gzip"] if @encoding == :gzip
      headers["connect-timeout-ms"] = [deadline.timeout_ms] if deadline
      StreamRequest.new(url: url, http_method: "POST", headers: headers, deadline: deadline,
                        cancellation: state, lifecycle: lifecycle)
    end
  end
end
