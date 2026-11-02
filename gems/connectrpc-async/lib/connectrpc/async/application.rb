# frozen_string_literal: true

module ConnectRPC
  module Async
    # Native unary translation. Generic HTTP middleware belongs on fallback.
    class Application
      def initialize(server:, fallback: nil)
        raise ArgumentError, "server must be a ConnectRPC::Server" unless server.is_a?(ConnectRPC::Server)
        raise ArgumentError, "fallback must be callable" unless fallback.nil? || fallback.respond_to?(:call)

        @server = server
        @fallback = fallback
      end

      def call(request)
        path = request.path.split("?", 2).first
        unless @server.handles?(path)
          return @fallback ? @fallback.call(request) : ::Protocol::HTTP::Response[404, {}, nil]
        end

        response = @server.call(path: path, http_method: request.method, headers: Internal.headers(request),
                                input: Internal::Input.new(request.body))
        body = response.body.empty? ? nil : [response.body]
        ::Protocol::HTTP::Response[response.status, Internal.fields(response.headers), body]
      end
    end
  end
end
