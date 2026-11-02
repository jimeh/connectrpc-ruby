# frozen_string_literal: true

module ConnectRPC
  module Falcon
    class RackGuard
      class Unsupported < StandardError; end
      private_constant :Unsupported

      class Request < SimpleDelegator
        def initialize(request, reject)
          super(request)
          @reject = reject
        end

        def hijack? = false
        def hijack! = @reject.call
      end
      private_constant :Request

      def initialize(app)
        raise ArgumentError, "rack_app must be callable" unless app.respond_to?(:call)

        @app = app
      end

      def call(env)
        attempted = false
        reject = lambda do
          attempted = true
          raise Unsupported, "Socket ownership transfer is unsupported"
        end
        env["rack.hijack?"] = false
        env["rack.hijack"] = reject
        env["protocol.http.request"] = Request.new(env["protocol.http.request"], reject)
        response = @app.call(env)
        status, headers, body = response
        if attempted || status == 101 || headers.keys.any? do |key|
             %w[rack.hijack rack.protocol upgrade].include?(key.downcase)
           end
          body.close if body.respond_to?(:close)
          return rejected
        end
        response
      rescue Unsupported
        rejected
      end

      private

      def rejected
        [501, { "content-type" => "text/plain" }, ["Socket ownership transfer is unsupported"]]
      end
    end
    private_constant :RackGuard

    class ListenerApplication
      def initialize(application, scheme)
        @application = application
        @scheme = scheme
      end

      def call(request)
        request.scheme ||= @scheme
        if request.method == "CONNECT" || request.protocol || request.headers["upgrade"]
          return ::Protocol::HTTP::Response[501, {}, nil]
        end

        response = @application.call(request)
        if response.nil? || response.status == 101 || response.protocol
          response&.close
          return ::Protocol::HTTP::Response[501, {}, nil]
        end
        response.body = CloseOnce.new(response.body) if response.body
        response
      end
    end
    private_constant :ListenerApplication

    # Upstream write ensure and cancellation can both close a body. Mark before
    # entering callbacks so a blocked rack.response_finished cannot reenter.
    class CloseOnce < ::Protocol::HTTP::Body::Wrapper
      def close(error = nil)
        return if @closed

        @closed = true
        super
      end
    end
    private_constant :CloseOnce
  end
end
