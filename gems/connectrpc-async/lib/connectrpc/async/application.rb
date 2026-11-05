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
        return if path && @server.cors_response(path: path, http_method: request.method,
                                                headers: Internal.headers(request))

        return unless path && @server.handles?(path)

        @server.start_call(path: path, headers: Internal.headers(request), http_method: request.method)
      end

      def publication?(request)
        path = request.path&.split("?", 2)&.first
        return false unless path

        return false unless @server.handles?(path)

        headers = Internal.headers(request)
        protocol = @server.send(:request_protocol, headers, request.method)
        protocol == :grpc_web ||
          (protocol != :grpc && @server.instance_variable_get(:@methods)[path].is_a?(BidiStreamMethod)) ||
          (request.method == "POST" && protocol == :connect &&
            !Streaming.codec(Protocol.single(headers, "content-type")).nil?) ||
          http1_streaming_route?(request)
      end

      def http1_streaming_route?(request)
        return false if request.respond_to?(:stream)

        path = request.path&.split("?", 2)&.first
        return false unless path && @server.handles?(path)

        declaration = @server.instance_variable_get(:@methods)[path]
        headers = Internal.headers(request)
        protocol = @server.send(:request_protocol, headers, request.method)
        return false if protocol == :grpc_web

        (declaration && !declaration.is_a?(Method)) ||
          (request.method == "POST" && protocol == :connect &&
            !Streaming.codec(Protocol.single(headers, "content-type")).nil?)
      end

      private :publication?, :http1_streaming_route?

      def admission_response(request)
        path = request.path&.split("?", 2)&.first
        response = TransportResponse.new(status: 503, headers: {}, body: "")
        if path
          response = @server.cors_response(path: path, http_method: request.method,
                                           headers: Internal.headers(request), response: response) || response
        end
        ::Protocol::HTTP::Response[response.status, Internal.fields(response.headers), nil]
      end

      def call_with_publication(request, publication:, lifecycle: nil, resource: nil)
        path, = request.path.split("?", 2)
        headers = Internal.headers(request)
        # This source-coupled host shares core's selection before choosing output.
        protocol = @server.send(:request_protocol, headers, request.method)
        h1_connect = !request.respond_to?(:stream) && http1_streaming_route?(request)
        return call(request, lifecycle: lifecycle) if !h1_connect && request.method == "GET" && protocol == :connect

        web = protocol == :grpc_web
        grpc = protocol == :grpc
        stream = request.stream if request.respond_to?(:stream)
        resource ||= stream&.connection&.resource
        if web && !stream && resource.respond_to?(:web_input)
          h1_input = resource.web_input(request.body, headers: headers)
        end
        early = @server.cors_response(path: path, http_method: request.method, headers: headers)
        h1_input&.close if early
        return ::Protocol::HTTP::Response[early.status, Internal.fields(early.headers), nil] if early
        if h1_connect && @server.instance_variable_get(:@methods)[path].is_a?(BidiStreamMethod)
          return ::Protocol::HTTP::Response[505, {}, nil]
        end
        return call(request, lifecycle: lifecycle) if grpc && !stream
        if h1_connect && request.method == "POST" && request.version != "HTTP/1.1" &&
           Streaming.codec(Protocol.single(headers, "content-type"))
          return ::Protocol::HTTP::Response[505, {}, nil]
        end

        unless @server.handles?(path) && (web || grpc || Streaming.codec(Protocol.single(headers, "content-type")))
          return call(request, lifecycle: lifecycle)
        end

        body = web ? Internal::GRPCWebBody.new(publication) : Internal::OutputBody.new
        trailers = ::Protocol::HTTP::Headers.new if grpc
        output = if web
                   Internal::GRPCWebServerOutput.new(body)
                 elsif grpc
                   Internal::GRPCServerOutput.new(body, trailers)
                 else
                   Internal::ServerOutput.new(body)
                 end
        begin
          input_type = grpc ? Internal::GRPCInput : Internal::Input
          input = input_type.new(request.body, resource: resource)
          resource.input_retirement.input = input if h1_connect && resource&.input_retirement
          entrypoint = if web
                         :call_grpc_web
                       else
                         (grpc ? :call_grpc : :call_stream)
                       end
          options = if web
                      { capabilities: %i[unary server_stream] }
                    elsif stream
                      { http2: true }
                    else
                      { capabilities: %i[server_stream client_stream] }
                    end
          response = @server.public_send(
            entrypoint, path: path, http_method: request.method, headers: headers,
                        input: input,
                        output: output, **options,
                        lifecycle: lifecycle, cancellation: stream&.processing_cancellation,
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
          stream&.detach_cancellation
          # HTTP/1 Finishable retains a reading request until explicit close,
          # even after EOF. The original processor owns this cleanup too.
          if web && !stream
            h1_input ? h1_input.close : request.body&.close
          end
        end
        body.close_write
        nil
      ensure
        resource.input_retirement&.processor_finished if resource.respond_to?(:input_retirement)
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
        Internal.close_consumed_http1(request)
      end
    end
  end
end
