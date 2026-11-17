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

        # A 101 interim response would switch protocols before the returned
        # status is checked. Other interim responses, such as 100 and 103, pass.
        def send_interim_response(status, headers)
          @reject.call if status == 101
          __getobj__.interim_response&.call(status, headers)
        end

        def write_interim_response(status, headers = nil) = send_interim_response(status, headers)
        def interim_response = ->(status, headers) { send_interim_response(status, headers) }
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
      rescue Exception => e # rubocop:disable Lint/RescueException -- the original body learns every escape
        error = e
        raise
      ensure
        original&.close(error) unless handed_off
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
        @callback = callback
        @original = original
        begin
          body = yield
          converted = true
        rescue Exception => e # rubocop:disable Lint/RescueException -- callbacks learn every conversion escape
          error = e
          raise
        ensure
          complete(error) unless converted
        end
        super(body || ::Protocol::HTTP::Body::Buffered.wrap([]))
      end

      def rewindable? = false
      def rewind = false

      # The Rack adapter closes a rejected tuple from inside its own rescue
      # clause without passing that exception, so only an argless close reads
      # it. Other argless closes, such as no-content conversion, run outside a
      # rescue clause on a host-owned fiber.
      def close(error = $ERROR_INFO)
        return if @closed

        @closed = true
        complete(error) { super }
      end

      private

      # Release order: completion callback, wrapped body (when built), original.
      def complete(error)
        @callback.call(error)
      ensure
        begin
          yield if block_given?
        ensure
          @original&.close(error)
        end
      end
    end
    private_constant :CompletionBody

    class ListenerApplication
      def initialize(application, scheme)
        @application = application
        @scheme = scheme
      end

      def publication?(request) = @application.send(:publication?, request)
      def http1_streaming_route?(request) = @application.send(:http1_streaming_route?, request)
      private :publication?, :http1_streaming_route?
      def admission_response(request) = @application.admission_response(request)

      def call(request, lifecycle: nil, publication: nil, resource: nil)
        request.scheme ||= @scheme
        if switch_requested?(request)
          lifecycle&.host_rejected!
          return ::Protocol::HTTP::Response[501, {}, nil]
        end

        response = if publication
                     publication.on_publish { |head| head.body = CloseOnce.new(head.body) if head&.body }
                     @application.call_with_publication(request, lifecycle: lifecycle, publication: publication,
                                                                 resource: resource)
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

      private

      # An HTTP/1 Upgrade header (async-http moves it to request.protocol) is
      # only an offer, which this listener ignores. CONNECT, HTTP/2 extended
      # CONNECT's :protocol and an HTTP/2 upgrade field demand a tunnel.
      def switch_requested?(request)
        return true if request.method == ::Protocol::HTTP::Methods::CONNECT
        return false if request.version&.start_with?("HTTP/1")

        !!(request.protocol || request.headers["upgrade"])
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
