# frozen_string_literal: true

require "connectrpc"

begin
  gem "rack", ">= 3.2.7", "< 4"
  require "rack"
rescue LoadError => e
  raise LoadError, "ConnectRPC Rack integration requires rack >= 3.2.7, < 4: #{e.message}"
end

module ConnectRPC
  class Rack
    def initialize(app = nil, server:)
      @app = app || ->(_env) { [404, { "content-type" => "text/plain" }, ["Not found"]] }
      @server = server
    end

    def call(env) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
      path = env.fetch("PATH_INFO", "")
      return @app.call(env) unless @server.handles?(path)

      headers = request_headers(env)
      early = @server.cors_response(path: path, http_method: env.fetch("REQUEST_METHOD"), headers: headers)
      return [early.status, early.headers, [early.body]] if early

      lifecycle = @server.start_call(path: path, headers: headers, http_method: env.fetch("REQUEST_METHOD"))
      response = @server.call(path: path, http_method: env.fetch("REQUEST_METHOD"), headers: headers,
                              input: env["rack.input"], query: env.fetch("QUERY_STRING", ""), lifecycle: lifecycle)
      body = [response.body]
      if lifecycle
        callbacks = env["rack.response_finished"]
        if callbacks
          callbacks << lambda do |_env, status, _headers, error|
            lifecycle.finish(delivery: error ? :aborted : :response_finished, error: error, http_status: status)
          end
        else
          body = Body.new(body, lifecycle)
        end
      end
      [response.status, response.headers, body]
    rescue Exception => e # rubocop:disable Lint/RescueException -- observe cancellation, then preserve it
      lifecycle&.finish(delivery: :aborted, error: e)
      raise
    end

    class Body
      def initialize(body, lifecycle)
        @body = body
        @lifecycle = lifecycle
      end

      def each(&) = @body.each(&)

      def close
        @lifecycle.finish(delivery: :body_closed)
      end

      def discard(error)
        @lifecycle.finish(delivery: :aborted, error: error)
      end
    end
    private_constant :Body

    private

    # Rails has not accepted ownership until its final header/body merge returns.
    # Only our body owns disposal here; response_finished remains host-owned.
    def transfer_response(env)
      result = call(env)
      status, headers, body = result
      response = TransportResponse.new(status: status, headers: headers, body: "")
      decorated = @server.cors_response(path: env.fetch("PATH_INFO"), http_method: env.fetch("REQUEST_METHOD"),
                                        headers: request_headers(env), response: response)
      yield result, !decorated.equal?(response)
    rescue Exception => e # rubocop:disable Lint/RescueException -- preserve the original transfer failure
      body.discard(e) if body.is_a?(Body)
      raise
    end

    def request_headers(env)
      env.each_with_object({}) do |(key, value), result|
        if key.start_with?("HTTP_")
          result[key.delete_prefix("HTTP_").tr("_", "-").downcase] = [value]
        elsif %w[CONTENT_TYPE CONTENT_LENGTH].include?(key)
          result[key.tr("_", "-").downcase] = [value]
        end
      end
    end
  end
end
