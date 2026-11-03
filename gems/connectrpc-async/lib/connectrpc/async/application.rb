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

      def call_with_publication(request, publication:, lifecycle: nil)
        path, = request.path.split("?", 2)
        headers = Internal.headers(request)
        grpc = GRPCProtocol.intent?(headers)
        unless @server.handles?(path) && (grpc || Streaming.codec(Protocol.single(headers, "content-type")))
          return call(request, lifecycle: lifecycle)
        end

        body = Internal::OutputBody.new
        trailers = ::Protocol::HTTP::Headers.new if grpc
        output = grpc ? Internal::GRPCServerOutput.new(body, trailers) : Internal::ServerOutput.new(body)
        begin
          input_type = grpc ? Internal::GRPCInput : Internal::Input
          input = input_type.new(request.body, resource: request.stream.connection.resource)
          entrypoint = grpc ? :call_grpc : :call_stream
          response = @server.public_send(
            entrypoint, path: path, http_method: request.method, headers: headers,
                        input: input,
                        output: output, http2: true,
                        lifecycle: lifecycle, cancellation: request.stream.processing_cancellation,
                        publish: lambda { |head|
                          fields = ::Protocol::HTTP::Headers[Internal.fields(head.headers)]
                          fields.trailer!
                          output.headers = fields if grpc
                          publication.publish(::Protocol::HTTP::Response[head.status, fields, body])
                        }
          )
          if response
            return ::Protocol::HTTP::Response[response.status, Internal.fields(response.headers),
                                              response.body.empty? ? nil : [response.body]]
          end
        ensure
          request.stream.detach_cancellation
        end
        body.close_write
        nil
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
