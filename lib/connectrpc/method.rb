# frozen_string_literal: true

module ConnectRPC
  class Method
    IDENTIFIER = /\A[A-Za-z_][A-Za-z_0-9]*\z/
    QUALIFIED_NAME = /\A[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)*\z/

    attr_reader :service, :name, :request_type, :response_type, :path, :idempotency

    def cardinality = :unary

    def self.validate_name!(value, qualified: false)
      pattern = qualified ? QUALIFIED_NAME : IDENTIFIER
      raise ArgumentError, "Invalid protobuf name" unless value.is_a?(String) && pattern.match?(value)
    end

    # Validate and assign the immutable declaration together.
    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
    def initialize(service:, name:, request_type:, response_type:, cardinality: :unary, idempotency: :unknown)
      raise ArgumentError, "Only unary methods are supported" unless cardinality == :unary

      unless %i[unknown no_side_effects idempotent].include?(idempotency)
        raise ArgumentError, "Invalid method idempotency"
      end

      self.class.validate_name!(service, qualified: true)
      self.class.validate_name!(name)
      raise ArgumentError, "Message types are required" if request_type.nil? || response_type.nil?

      @idempotency = idempotency
      @service = service.dup.freeze
      @name = name.dup.freeze
      @request_type = request_type
      @response_type = response_type
      @path = "/#{service}/#{name}".freeze
      freeze
    end
  end

  # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

  class Service
    attr_reader :name, :methods

    def initialize(name:, methods:)
      Method.validate_name!(name, qualified: true)
      validate_methods!(name, methods)

      @name = name.dup.freeze
      @methods = methods.dup.freeze
      freeze
    end

    private

    def validate_methods!(name, methods)
      unless methods.is_a?(Array) && methods.all? { |method| service_method?(method, name) }
        raise ArgumentError, "Methods must belong to this service"
      end
      raise ArgumentError, "Duplicate methods" unless methods.map(&:name).uniq.size == methods.size
    end

    def service_method?(method, name)
      [Method, ServerStreamMethod, ClientStreamMethod, BidiStreamMethod].any? { |type| method.is_a?(type) } &&
        method.service == name
    end
  end
end
