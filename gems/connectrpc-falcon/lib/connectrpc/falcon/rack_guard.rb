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
        raised, value = wait_outcome(outcome)
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

      # A handler interrupted after the request task produced a response owns
      # that response's body, which no transport will close. It closes the
      # body with the interruption, so completion still runs once on the
      # request task, then raises the interruption.
      def wait_outcome(outcome)
        outcome.wait
      rescue Exception => e # rubocop:disable Lint/RescueException -- re-raised once the produced body is closed
        raised, value = outcome.value if outcome.resolved?
        abandon(value[2], e) if outcome.resolved? && !raised
        raise
      end

      # Closes an abandoned body with the error that abandoned it. That error,
      # not the close's own escape, still leaves the handler, so the escape is
      # logged as protocol-rack logs a failing callback.
      def abandon(body, error)
        body.close(error)
      rescue Exception => e # rubocop:disable Lint/RescueException -- the abandoning error propagates instead
        ::Console.error(self, "Error occurred while closing an abandoned Rack response:", e)
      end

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
    # Cancellation fallback: a request task cancelled or failed while parked
    # never closes the body or runs callbacks itself, because a writer may
    # still be writing the body and closing it would truncate a response that
    # could then look complete. It marks itself gone and exits; the later
    # close, from whichever fiber closes the body, then completes inline with
    # that close's error. The callbacks then run on that fiber, where
    # peer_certificate is nil.
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

      # Waits until the request task has settled. Before the request task
      # existed the closer ran the completion itself, so an interruption of
      # the closer interrupted the running callback and its close returned
      # only once the nested ensures had closed the bodies. To keep that, each
      # interruption of this closer is forwarded to the request task, where a
      # second one also reaches a cancellation the task deferred, and the
      # first is raised once the task has settled. A request task that exits
      # without claiming the run leaves it to this closer.
      #
      # Limitation: over HTTP/2 a peer reset during the response's HEADERS
      # flush makes the stream close the body under its connection's frame
      # guard. A completion callback that then writes a frame to that same
      # connection waits for the guard this closer holds, until retirement's
      # cancellation of the closer is forwarded here.
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
      # settled.
      def await_settled
        interruption = nil
        forwards = 0
        until @done.resolved?
          begin
            if forwards.positive?
              forwards -= 1
              @owner.cancel
            else
              @done.wait
            end
          rescue Exception => e # rubocop:disable Lint/RescueException -- forwarded, then raised once settled
            interruption ||= e
            forwards += 1
          end
        end
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
