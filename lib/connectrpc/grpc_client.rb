# frozen_string_literal: true

module ConnectRPC
  class Client
    private

    def client_stream_strategy(lifecycle = nil)
      return Streaming::ConnectStrategy.new unless @protocol == :grpc

      GRPCProtocol::Strategy.new(codec: @status_codec, wire_limit: @grpc_status_wire_limit, limit: @grpc_status_limit,
                                 on_error: ->(error) { lifecycle&.record_failure(error) })
    end

    def grpc_stream_request(url, metadata, deadline, state, lifecycle)
      headers = metadata.to_h.merge("content-type" => [GRPCProtocol::CONTENT_TYPE], "te" => ["trailers"],
                                    "grpc-accept-encoding" => [@accept_encoding.join(",")])
      headers["grpc-encoding"] = ["gzip"] if @encoding == :gzip
      headers["grpc-timeout"] = [GRPCProtocol.timeout(deadline)] if deadline
      StreamRequest.new(url: url, http_method: "POST", headers: headers, deadline: deadline,
                        cancellation: state, lifecycle: lifecycle)
    end
  end
end
