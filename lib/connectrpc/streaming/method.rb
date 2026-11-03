# frozen_string_literal: true

module ConnectRPC
  # Streaming declarations share validation, but are never unary Methods.
  class StreamDeclaration
    attr_reader :service, :name, :request_type, :response_type, :path, :idempotency

    def initialize(service:, name:, request_type:, response_type:, idempotency: :unknown)
      declaration = Method.new(service: service, name: name, request_type: request_type,
                               response_type: response_type, idempotency: idempotency)
      @service = declaration.service
      @name = declaration.name
      @request_type = declaration.request_type
      @response_type = declaration.response_type
      @path = declaration.path
      @idempotency = declaration.idempotency
      freeze
    end
  end
  private_constant :StreamDeclaration

  class ServerStreamMethod < StreamDeclaration
    def cardinality = :server_stream
  end

  class ClientStreamMethod < StreamDeclaration
    def cardinality = :client_stream
  end

  class BidiStreamMethod < StreamDeclaration
    def cardinality = :bidi_stream
  end
end
