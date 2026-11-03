# frozen_string_literal: true

require "English"

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
        original = guarded_body(body)
        if attempted || status == 101 || headers.keys.any? do |key|
             %w[rack.hijack rack.protocol upgrade].include?(key.downcase)
           end
          return rejected
        end

        response = [status, headers, completing_body(env, status, headers, original)]
        handed_off = true
        response
      rescue Unsupported
        rejected
      ensure
        original&.close($ERROR_INFO) unless handed_off
      end

      private

      def guarded_body(body)
        if body.is_a?(::Protocol::HTTP::Body::Readable)
          CloseOnce.new(body)
        elsif body
          RackBody.new(body)
        end
      end

      def completing_body(env, status, headers, original)
        # Original-body ownership is needed even without callbacks: upstream
        # array conversion retains only the array's contents.
        callbacks = env["rack.response_finished"] || []
        callback = ::Protocol::Rack::Body.completion_callback(callbacks, env, status, headers)
        env["rack.response_finished"] = []
        CompletionBody.new(callback, original) do
          ::Protocol::Rack::Body.wrap(env, status, headers.dup, original, env["protocol.http.request"].body)
        end
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
      def initialize(callback, original)
        super(::Protocol::HTTP::Body::Buffered.wrap([]))
        @callback = callback
        @original = original
        begin
          @body = yield || @body
          converted = true
        ensure
          close($ERROR_INFO) unless converted
        end
      end

      def rewindable? = false
      def rewind = false

      # The Rack adapter closes a rejected tuple without passing its exception.
      def close(error = $ERROR_INFO)
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

      def call(request, lifecycle: nil, publication: nil)
        request.scheme ||= @scheme
        if request.method == "CONNECT" || request.protocol || request.headers["upgrade"]
          lifecycle&.host_rejected!
          return ::Protocol::HTTP::Response[501, {}, nil]
        end

        response = if publication
                     publication.on_publish { |head| head.body = CloseOnce.new(head.body) if head&.body }
                     @application.call_with_publication(request, lifecycle: lifecycle, publication: publication)
                   else
                     @application.call(request, lifecycle: lifecycle)
                   end
        return if publication && response.nil?

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
