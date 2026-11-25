# frozen_string_literal: true

require "English"

module ConnectRPC
  module Falcon
    # An application tried to take over the socket or switch protocols.
    class Unsupported < StandardError; end
    private_constant :Unsupported

    class RackGuard
      class Request < SimpleDelegator
        def initialize(request, reject)
          super(request)
          @reject = reject
        end

        def hijack? = false
        def hijack! = @reject.call

        # A 101 interim response would switch protocols before the returned
        # status is checked. Other interim responses, such as 100 and 103, pass.
        def send_interim_response(status, headers = nil)
          @reject.call if Integer(status, exception: false) == 101
          # Upstream's HTTP/1 writer cannot take nil headers.
          __getobj__.interim_response&.call(status, headers || {})
        end

        def write_interim_response(status, headers = nil) = send_interim_response(status, headers)
        def interim_response = ->(status, headers = nil) { send_interim_response(status, headers) }
      end
      private_constant :Request

      def initialize(app, identities: nil)
        raise ArgumentError, "rack_app must be callable" unless app.respond_to?(:call)

        @app = app
        @identities = identities
        @produced = {}.compare_by_identity
      end

      # Runs on the session's handler task. The request task runs the app, its
      # rack.response_finished callbacks and its body's completion on one
      # fiber, which Rails' executor needs under fiber isolation. It is a
      # non-transient child of this handler, so cancellation, joins and
      # retirement capture reach it. This handler returns as soon as the
      # request task has an outcome, so the transport can write and close the
      # body while the request task waits for that close.
      def call(env)
        handler = ::Async::Task.current
        outcome = ::Async::Promise.new
        identity = @identities&.capture
        handler.async(finished: false) do
          if @identities
            @identities.scope(identity) { serve(env, outcome) }
          else
            serve(env, outcome)
          end
        end
        raised, value = outcome.wait
        raise value if raised

        @produced[handler] = value[2]
        value
      end

      # Called by the server on the handler task once the fallback call has
      # returned or failed. An exception that leaves the server after this
      # guard produced a response, such as a failing response middleware,
      # abandons the body before any transport owns it, so it is closed here
      # with that error. A response lost later, inside the session's dispatch,
      # stays parked until retirement cancels the request task, which then
      # exits without completion, as before this request task existed.
      def settle(error)
        body = @produced.delete(::Async::Task.current)
        body&.close(error) if error
      end

      private

      # Runs on the request task and always resolves the outcome. Escapes are
      # handed to the handler as tagged outcomes, so Async never reports them
      # as task failures here.
      def serve(env, outcome)
        begin
          response = respond(env)
          completion = response[2].completion
          # Park before the handler can see the body, so every close finds it.
          completion.park
          result = [false, response]
        rescue Exception => e # rubocop:disable Lint/RescueException -- the handler re-raises every escape
          result = [true, e]
          begin
            # protocol-rack's error handler would run these on the handler task,
            # where a callback's own escape would replace the app's.
            fail_callbacks(env, e) if e.is_a?(StandardError)
          rescue Exception => escape # rubocop:disable Lint/RescueException -- handed to the handler as above
            result = [true, escape]
          end
        ensure
          outcome.resolve(result || [true, $ERROR_INFO])
        end
        completion&.serve
      end

      def respond(env)
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
          return rejected(env)
        end

        response = [status, headers, completing_body(env, status, headers, original)]
        handed_off = true
        response
      rescue Unsupported
        rejected(env)
      rescue Exception => e # rubocop:disable Lint/RescueException -- the original body learns every escape
        error = e
        raise
      ensure
        original&.close(error) unless handed_off
      end

      # Matches protocol-rack's error handler: reverse order, nil status and
      # headers, each callback's StandardError logged. The adapter then finds
      # no callbacks left to run.
      def fail_callbacks(env, error)
        callbacks = env["rack.response_finished"]
        return if callbacks.nil? || callbacks.empty?

        env["rack.response_finished"] = []
        ::Protocol::Rack::Body.completion_callback(callbacks, env, nil, nil).call(error)
      end

      def guarded_body(body)
        if body.is_a?(::Protocol::HTTP::Body::Readable)
          CloseOnce.new(body)
        elsif body
          RackBody.new(body)
        end
      end

      def completing_body(env, status, headers, original, content = original)
        # Original-body ownership is needed even without callbacks: upstream
        # array conversion retains only the array's contents.
        callbacks = env["rack.response_finished"] || []
        callback = ::Protocol::Rack::Body.completion_callback(callbacks, env, status, headers)
        env["rack.response_finished"] = []
        CompletionBody.new(Completion.new(callback, original)) do
          ::Protocol::Rack::Body.wrap(env, status, headers.dup, content, env["protocol.http.request"].body)
        end
      end

      # The rejected original closes first; callbacks run once this reply's
      # body closes, through the same request-task completion.
      def rejected(env)
        headers = { "content-type" => "text/plain" }
        [501, headers, completing_body(env, 501, headers, nil, ["Socket ownership transfer is unsupported"])]
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
      attr_reader :completion

      def initialize(completion)
        begin
          body = yield
          converted = true
        rescue Exception => e # rubocop:disable Lint/RescueException -- callbacks learn every conversion escape
          error = e
          raise
        ensure
          completion.close(error) unless converted
        end
        super(body || ::Protocol::HTTP::Body::Buffered.wrap([]))
        completion.body = @body
        @completion = completion
      end

      def rewindable? = false
      def rewind = false

      # The Rack adapter closes a rejected tuple from inside its own rescue
      # clause without passing that exception, so only an argless close reads
      # it. Other argless closes, such as no-content conversion, run outside a
      # rescue clause on a host-owned fiber. The default is read here, on the
      # closing fiber, and passed on explicitly.
      def close(error = $ERROR_INFO)
        @completion.close(error)
      end
    end
    private_constant :CompletionBody

    # One response's completion, shared by its CompletionBody and any shallow
    # copy, such as a cache hit's, so it runs exactly once.
    #
    # States: inline (not parked, a close completes on the closing fiber),
    # parked (the request task waits for a close), requested (a closer handed
    # the request task its error), running (claimed), done, and gone (the
    # request task exited without claiming).
    #
    # Cancellation fallback: a request task cancelled or failed while parked
    # never closes the body or runs callbacks itself, because a writer may
    # still be writing the body and closing it would truncate a response that
    # could then look complete. It marks itself gone and exits; the later
    # close, from whichever fiber closes the body, then completes inline with
    # that close's error.
    class Completion
      attr_writer :body

      def initialize(callback, original)
        @callback = callback
        @original = original
        @state = :inline
      end

      # On the request task, before the handler task can see the response.
      def park
        @owner = ::Async::Task.current
        @wake = ::Async::Promise.new
        @done = ::Async::Promise.new
        @state = :parked
      end

      # On the request task: waits for a close, then completes here. A closer
      # waiting on this task is always woken, whatever ends it.
      def serve
        @wake.wait
        @state = :running
        run(@error)
      rescue ::Async::Cancel
        raise
      rescue Exception => e # rubocop:disable Lint/RescueException -- the waiting closer raises the cleanup escape
        @escape = e if @state == :running
      ensure
        settle
      end

      def close(error)
        case @state
        when :inline, :gone then run_inline(error)
        when :parked then @owner.current? ? run_inline(error) : hand_off(error)
        end
      end

      private

      # Waits for the request task to complete. An interrupted closer stops
      # waiting and re-raises; a request task that has claimed the run
      # finishes on its own under retirement capture. Before the request task
      # claims, the run falls to whichever side remains.
      def hand_off(error)
        @error = error
        @state = :requested
        @wake.resolve(true)
        begin
          @done.wait
        rescue Exception => e # rubocop:disable Lint/RescueException -- the interruption is re-raised below
          interruption = e
        end
        if interruption
          begin
            if @state == :gone
              run_inline(error)
            elsif @state == :requested
              @abandoned = true
            end
          ensure
            raise interruption
          end
        end
        return run_inline(error) if @state == :gone

        raise @escape if @escape
      end

      def settle
        case @state
        when :running then @state = :done
        when :parked then @state = :gone
        when :requested
          if @abandoned
            # The closer stopped waiting before this task claimed the run.
            begin
              run_inline(@error)
            rescue Exception # rubocop:disable Lint/RescueException -- no closer remains to receive it
              nil
            end
          else
            @state = :gone
          end
        end
      ensure
        @done&.resolve(nil)
      end

      def run_inline(error)
        @state = :running
        run(error)
      ensure
        @state = :done
      end

      # Release order: completion callback, wrapped body (when built), original.
      def run(error)
        @callback.call(error)
      ensure
        begin
          @body&.close(error)
        ensure
          @original&.close(error)
        end
      end
    end
    private_constant :Completion

    # Scopes the session's authenticated client leaf to the exact Rack
    # fallback request task, as the session scopes it to its handler task:
    # never to that task's children, and only on the session's thread and
    # reactor. Each read builds a fresh certificate from the captured DER.
    class RequestIdentities
      def initialize(&source)
        @source = source
        @entries = {}.compare_by_identity
      end

      # On the handler task, before the request task starts.
      def capture = @source.call&.to_der

      def scope(der)
        return yield unless der

        task = ::Async::Task.current
        @entries[task] = [der, Thread.current, Fiber.scheduler]
        yield
      ensure
        @entries.delete(task) if task
      end

      def peer_certificate
        der, thread, scheduler = @entries[::Async::Task.current?]
        return unless der && Thread.current.equal?(thread) && Fiber.scheduler.equal?(scheduler)

        OpenSSL::X509::Certificate.new(der.dup)
      end
    end
    private_constant :RequestIdentities

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
