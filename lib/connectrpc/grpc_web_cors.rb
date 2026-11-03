# frozen_string_literal: true

require "uri"

module ConnectRPC
  # One immutable policy per mount. Authentication, CSRF and outer middleware
  # remain application-owned.
  class GRPCWebCORS
    BASE_REQUEST = %w[content-type x-grpc-web x-user-agent grpc-timeout accept].freeze
    BASE_EXPOSE = %w[grpc-status grpc-message grpc-status-details-bin].freeze
    TOKEN = /\A[!#$%&'*+.^_`|~0-9A-Za-z-]+\z/
    private_constant :BASE_REQUEST, :BASE_EXPOSE, :TOKEN

    attr_reader :origins, :credentials, :request_headers, :expose_headers

    def initialize(origins:, credentials: false, request_headers: [], expose_headers: []) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
      unless [true, false].include?(credentials) && origins.is_a?(Array) && origins.size <= 32 &&
             origins.all? { |origin| valid_origin?(origin) } && origins.uniq.size == origins.size
        raise ArgumentError, "CORS requires distinct exact HTTP(S) origins and boolean credentials"
      end

      @origins = origins.map { |origin| origin.dup.freeze }.freeze
      @credentials = credentials
      @request_headers = (BASE_REQUEST + names(request_headers)).uniq.freeze
      @expose_headers = (BASE_EXPOSE + names(expose_headers)).uniq.freeze
      freeze
    end

    private

    def valid_origin?(origin) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
      return false unless origin.is_a?(String) && origin.ascii_only? && origin.bytesize <= 2048
      return false unless %r{\Ahttps?://[^/?#\s]+\z}.match?(origin)

      uri = URI.parse(origin)
      host = uri.host
      port = ":#{uri.port}" unless uri.port == uri.default_port
      !!(uri.is_a?(URI::HTTP) && host && !uri.userinfo && uri.to_s == origin &&
        host == host.downcase && origin == "#{uri.scheme}://#{host}#{port}")
    rescue URI::InvalidURIError
      false
    end

    def names(values) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
      unless values.is_a?(Array) && values.size <= 32 && values.all? do |value|
               value.is_a?(String) && value.bytesize <= 128 && TOKEN.match?(value)
             end
        raise ArgumentError, "CORS header lists require at most 32 HTTP tokens of at most 128 bytes"
      end

      normalized = values.map { |value| value.downcase.freeze }
      raise ArgumentError, "CORS header names must be distinct" unless normalized.uniq.size == normalized.size

      normalized
    end
  end

  class Server
    def cors_response(path:, http_method:, headers:, response: nil) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
      policy = @cors
      return response unless policy && @protocol.include?(:grpc_web) && handles?(path)

      return response if http_method != "OPTIONS" && request_protocol(headers, http_method) != :grpc_web

      values = Protocol.headers(headers)
      preflight = http_method == "OPTIONS" &&
                  (values.key?("origin") || values.key?("access-control-request-method"))
      return response unless preflight || GRPCWeb.intent?(values)

      origin = Protocol.single(values, "origin")
      allowed = origin && policy.origins.include?(origin)
      if preflight
        evaluated = cors_preflight(values, policy, origin, allowed)
        return evaluated unless response && evaluated.status == 204

        decorated = merge_cors_fields(response.headers, evaluated.headers.except("vary"))
        GRPCWeb.vary(decorated, *evaluated.headers.fetch("vary"))
        return TransportResponse.new(status: response.status, headers: decorated, body: response.body)
      end

      if values.key?("origin") && !allowed
        return TransportResponse.new(status: 403, headers: { "vary" => %w[Origin Accept] }, body: "")
      end
      return unless response

      # @type var fields: headers
      fields = allowed && origin ? cors_fields(policy, origin) : {}
      merged = merge_cors_fields(response.headers, fields)
      GRPCWeb.vary(merged, "Origin", "Accept")
      TransportResponse.new(status: response.status, headers: merged, body: response.body)
    end

    private

    def cors_fields(policy, origin)
      fields = { "access-control-allow-origin" => [origin],
                 "access-control-expose-headers" => [policy.expose_headers.join(", ")] }
      fields["access-control-allow-credentials"] = ["true"] if policy.credentials
      fields
    end

    def merge_cors_fields(headers, fields)
      normalized = Protocol.headers(headers)
      if normalized.any? { |key, values| key.start_with?("access-control-") && fields[key] != values }
        raise Error.new(:internal, "Conflicting application CORS policy"), cause: nil
      end

      normalized.merge(fields)
    end

    def cors_preflight(headers, policy, origin, allowed) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
      controls = headers.select { |key, _| key == "origin" || key.start_with?("access-control-request-") }
      if GRPCProtocol.field_size(controls) > 8192
        return TransportResponse.new(status: 403, body: "", headers: {
                                       "vary" => %w[Origin Access-Control-Request-Method Access-Control-Request-Headers]
                                     })
      end
      requested = headers.fetch("access-control-request-headers", []).flat_map do |value|
        value.split(",", -1).map(&:strip)
      end
      valid = GRPCProtocol.field_size(controls) <= 8192 && requested.size <= 32 &&
              requested.all? { |value| /\A[!#$%&'*+.^_`|~0-9A-Za-z-]+\z/.match?(value) }
      permitted = valid && allowed && Protocol.single(headers, "access-control-request-method") == "POST" &&
                  requested.all? { |value| policy.request_headers.include?(value.downcase) }
      fields = { "vary" => %w[Origin Access-Control-Request-Method Access-Control-Request-Headers] }
      if permitted && origin
        fields.merge!(cors_fields(policy, origin))
        fields["access-control-allow-methods"] = ["POST"]
        fields["access-control-allow-headers"] = [policy.request_headers.join(", ")]
        fields["access-control-max-age"] = ["0"]
      end
      TransportResponse.new(status: permitted ? 204 : 403, headers: fields, body: "")
    end
  end
end
