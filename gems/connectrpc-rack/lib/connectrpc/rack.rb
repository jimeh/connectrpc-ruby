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

    def call(env) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      path = env.fetch("PATH_INFO", "")
      return @app.call(env) unless @server.handles?(path)

      headers = request_headers(env)
      lifecycle = @server.start_call(path: path, headers: headers)
      response = @server.call(path: path, http_method: env.fetch("REQUEST_METHOD"), headers: headers,
                              input: env["rack.input"], lifecycle: lifecycle)
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
    end
    private_constant :Body

    private

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
