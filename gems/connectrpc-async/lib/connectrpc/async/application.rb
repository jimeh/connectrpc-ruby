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

      def start_call(request)
        path = request.path&.split("?", 2)&.first
        @server.start_call(path: path, headers: Internal.headers(request)) if path && @server.handles?(path)
      end

      def call(request, lifecycle: nil)
        stream = request.stream if request.respond_to?(:stream)
        stream = nil unless stream.is_a?(Internal::RequestStream)
        path, query = request.path.split("?", 2)
        unless @server.handles?(path)
          return @fallback ? @fallback.call(request) : ::Protocol::HTTP::Response[404, {}, nil]
        end

        response = @server.call(path: path, http_method: request.method, headers: Internal.headers(request),
                                input: Internal::Input.new(request.body), query: query || "", lifecycle: lifecycle,
                                cancellation: stream&.processing_cancellation)
        body = response.body.empty? ? nil : [response.body]
        ::Protocol::HTTP::Response[response.status, Internal.fields(response.headers), body]
      ensure
        stream&.detach_cancellation
      end
    end
  end
end
