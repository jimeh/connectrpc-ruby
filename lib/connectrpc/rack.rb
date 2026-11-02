# frozen_string_literal: true

require_relative "../connectrpc"

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

    def call(env)
      path = env.fetch("PATH_INFO", "")
      return @app.call(env) unless @server.handles?(path)

      response = @server.call(path: path, http_method: env.fetch("REQUEST_METHOD"), headers: request_headers(env),
                              input: env["rack.input"])
      [response.status, response.headers, [response.body]]
    end

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
