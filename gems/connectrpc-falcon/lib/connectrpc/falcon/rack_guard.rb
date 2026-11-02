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
        [status, headers, completing_body(env, status, headers, body)]
      rescue Unsupported
        rejected
      end

      private

      def completing_body(env, status, headers, body)
        callbacks = env["rack.response_finished"]
        return body unless callbacks&.any?

        # Reuse upstream conversion and callback policy, with separate cleanup
        # ownership so cancellation inside a callback cannot skip body close.
        env["rack.response_finished"] = []
        original = if body.is_a?(::Protocol::HTTP::Body::Readable)
                     CloseOnce.new(body)
                   elsif body
                     RackBody.new(body)
                   end
        converted = ::Protocol::Rack::Body.wrap(env, status, headers.dup, original,
                                                env["protocol.http.request"].body)
        callback = ::Protocol::Rack::Body.completion_callback(callbacks, env, status, headers)
        CompletionBody.new(converted || ::Protocol::HTTP::Body::Buffered.wrap([]), callback, original)
      end

      def rejected
        [501, { "content-type" => "text/plain" }, ["Socket ownership transfer is unsupported"]]
      end
    end
    private_constant :RackGuard

    class RackBody < SimpleDelegator
      def close(*)
        return if @closed

        @closed = true
        __getobj__.close if __getobj__.respond_to?(:close)
      end
    end
    private_constant :RackBody

    class CompletionBody < ::Protocol::HTTP::Body::Wrapper
      def initialize(body, callback, original)
        super(body)
        @callback = callback
        @original = original
      end

      def rewindable? = false
      def rewind = false

      def close(error = nil)
        return if @closed

        @closed = true
        begin
          @callback.call(error)
        ensure
          begin
            super
          ensure
            @original&.close(error)
          end
        end
      end
    end
    private_constant :CompletionBody

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
