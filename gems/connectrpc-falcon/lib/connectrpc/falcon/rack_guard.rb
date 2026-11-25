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
        raised, value = wait_outcome(outcome) do
          handler.async(finished: false) do
            if @identities
              @identities.scope(identity) { serve(env, outcome) }
            else
              serve(env, outcome)
            end
          end
        end
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
        abandon(body, error) if body && error
      end

      private

      # Starts the request task, then waits for its outcome. The outcome is
      # resolved once, by whichever side reaches it first. Starting the task
      # runs it at once and queues this handler, so an interruption can reach
      # the handler before it waits; the start is therefore inside the same
      # rescue. A handler interrupted after the request task produced a
      # response owns that response's body, which no transport will close. It
      # closes the body with the interruption, so completion still runs once
      # on the request task, then raises the interruption. A handler
      # interrupted earlier resolves the outcome as abandoned with its
      # interruption, and the request task completes any response it produces
      # afterwards.
      def wait_outcome(outcome)
        yield
        outcome.wait
      rescue Exception => e # rubocop:disable Lint/RescueException -- re-raised once the response is accounted for
        if outcome.resolved?
          raised, value = outcome.value
          abandon(value[2], e) unless raised
        else
          outcome.resolve([:abandoned, e])
        end
        raise
      end

      # Closes an abandoned body with the error that abandoned it, on the
      # handler or, for a response produced after the handler left, on the
      # request task. That error, not the close's own escape, is what leaves
      # the handler, so the escape is logged as protocol-rack logs a failing
      # callback.
      def abandon(body, error)
        body.close(error)
      rescue Exception => e # rubocop:disable Lint/RescueException -- the abandoning error propagates instead
        ::Console.error(self, "Error occurred while closing an abandoned Rack response:", e)
      end

      # Runs on the request task and always resolves the outcome, unless the
      # handler abandoned it first. Escapes are handed to the handler as
      # tagged outcomes, so Async never reports them as task failures here.
      def serve(env, outcome)
        begin
          response = respond(env)
          # Nothing yields between this check and the park and resolve below,
          # so either the handler takes the response or this task completes
          # it, here and with the handler's interruption.
          abandoned = outcome.value[1] if outcome.resolved?
          unless abandoned
            completion = response[2].completion
            # Park before the handler can see the body, so every close finds it.
            completion.park
          end
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
        abandoned ? abandon(response[2], abandoned) : completion&.serve
      end

      def respond(env)
        accepted(env) || rejected(env)
      end

      # Returns the guarded response, or nil for a rejected one once its
      # original has closed. A rejected original whose close raises leaves the
      # callbacks in env for the app-error path, where protocol-rack's error
      # handler ran them.
      def accepted(env)
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
        switching = headers.keys.any? { |key| %w[rack.hijack rack.protocol upgrade].include?(key.downcase) }
        return if attempted || status == 101 || switching

        response = [status, headers, completing_body(env, status, headers, original)]
        handed_off = true
        response
      rescue Unsupported
        nil
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

      def completing_body(env, status, headers, original, content = original, callback_headers: headers)
        # Original-body ownership is needed even without callbacks: upstream
        # array conversion retains only the array's contents.
        callbacks = env["rack.response_finished"] || []
        callback = ::Protocol::Rack::Body.completion_callback(callbacks, env, status, callback_headers)
        env["rack.response_finished"] = []
        CompletionBody.new(Completion.new(callback, original)) do
          ::Protocol::Rack::Body.wrap(env, status, headers.dup, content, env["protocol.http.request"].body)
        end
      end

      # The rejected original closes first; callbacks run once this reply's
      # body closes, through the same request-task completion. They receive
      # the headers as protocol-rack converted this reply's for them.
      def rejected(env)
        headers = { "content-type" => "text/plain" }
        [501, headers, completing_body(env, 501, headers, nil, ["Socket ownership transfer is unsupported"],
                                       callback_headers: ::Protocol::HTTP::Headers[headers])]
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
    # copy, such as a cache hit's, so it runs at most once.
    #
    # States: inline (not parked, a close completes on the closing fiber),
    # parked (the request task waits for a close), requested (a closer handed
    # the request task its error), running (claimed), done, and gone (the
    # request task exited without claiming).
    #
    # Closer interruptions: before the request task existed, a closer ran the
    # completion itself, so the closer's cancellation and a callback's
    # deferral met on one task. A closer waiting on the request task keeps
    # that across the two: it relays each interruption with the force it had,
    # takes on any deferral the request task makes of an unforced one, and
    # returns or raises only once the request task has settled.
    #
    # Cancellation fallback: a request task cancelled or failed while parked
    # never closes the body or runs callbacks itself, because a writer may
    # still be writing the body and closing it would truncate a response that
    # could then look complete. It marks itself gone and exits; the later
    # close, from whichever fiber closes the body, then completes inline with
    # that close's error. The callbacks then run on that fiber, where
    # peer_certificate is nil unless it is the session's handler task.
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
      # waiting on this task is always woken, whatever ends it. The claimed
      # run's escapes go to that closer, except this task's own cancellation:
      # a cancellation reaches the closer only when a close raised it. Only a
      # cancellation is re-raised here, which Async counts as this task's
      # cancellation rather than a failure.
      def serve
        @wake.wait
        @state = :running
        run(@error)
      rescue Exception => e # rubocop:disable Lint/RescueException -- the waiting closer raises the cleanup escape
        cancel = e.is_a?(::Async::Cancel)
        @escape = e if @state == :running && (!cancel || e.equal?(@close_escape))
        raise if cancel
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

      # Waits until the request task has settled. It then runs the completion
      # here if the request task exited without claiming it, and raises the
      # closer's first interruption, or else the claimed run's escape.
      #
      # Limitation: over HTTP/2 a peer reset during the response's HEADERS
      # flush makes the stream close the body under its connection's frame
      # guard. A completion callback that then writes a frame to that same
      # connection waits for the guard this closer holds, until retirement
      # forces the closer and that force reaches the request task.
      def hand_off(error)
        @error = error
        @state = :requested
        @wake.resolve(true)
        interruption = await_settled
        begin
          run_inline(error) if @state == :gone
        ensure
          raise interruption if interruption
        end
        raise @escape if @escape
      end

      # Returns the closer's first interruption once the request task has
      # settled. Each interruption is relayed inside the rescued region, so
      # one that arrives while a relay runs the request task is relayed too.
      def await_settled(interruption = nil)
        unrelayed = nil
        until @done.resolved?
          begin
            if unrelayed
              cause = unrelayed
              unrelayed = nil
              deferred = relay(cause)
              return mirror(deferred, interruption) if deferred
            else
              @done.wait
            end
          rescue Exception => e # rubocop:disable Lint/RescueException -- relayed, then raised once settled
            interruption ||= e
            unrelayed = e
          end
        end
        interruption
      end

      # Delivers an interruption to the request task with the force it had
      # here. One that reached this closer through its own pending deferred
      # cancellation was forced, so it is delivered even if the request task
      # defers it. Any other is delivered once, for the request task to defer
      # or not. Returns the interruption when the request task deferred it
      # unforced.
      def relay(interruption)
        forced = ::Async::Task.current.cancel_deferred?
        @owner.cancel(cause: interruption)
        return unless @owner.cancel_deferred?
        return interruption unless forced

        @owner.cancel(cause: interruption)
        nil
      end

      # Mirrors the request task's deferral here, with the relayed cancellation
      # pending, until the request task settles. A host that forces deferring
      # tasks, such as retirement, therefore forces this closer, and relay
      # passes that force on. A host that cancels only once, such as an HTTP/2
      # peer reset, lets the callback finish its deferred section; each task's
      # cancellation then fires as it leaves its deferral. This closer's own
      # fires here and gives way to its first interruption.
      def mirror(cause, interruption)
        closer = ::Async::Task.current
        closer.defer_cancel do
          closer.cancel(cause: cause)
          interruption = await_settled(interruption)
        end
        interruption
      rescue ::Async::Cancel => e
        raise unless e.cause.equal?(cause)

        interruption
      end

      def settle
        case @state
        when :running then @state = :done
        when :parked, :requested then @state = :gone
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
          release(@body, error)
        ensure
          release(@original, error)
        end
      end

      # Remembers the escape a close raised itself, as distinct from one that
      # only passed through the release.
      def release(body, error)
        body&.close(error)
      rescue Exception => e # rubocop:disable Lint/RescueException -- recorded and re-raised unchanged
        @close_escape = e
        raise
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
