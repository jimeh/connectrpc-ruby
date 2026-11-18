# frozen_string_literal: true

module ConnectRPC
  module Async
    class ServerSession
      include Internal::Affinity

      # Resource errors back off like Go net/http's Server.Serve; an aborted
      # handshake or an interrupted call retries after one scheduler turn.
      ACCEPT_RESOURCE_ERRORS = [Errno::EMFILE, Errno::ENFILE, Errno::ENOBUFS, Errno::ENOMEM].freeze
      ACCEPT_IMMEDIATE_ERRORS = [Errno::ECONNABORTED, Errno::EINTR].freeze
      ACCEPT_INITIAL_DELAY = 0.005
      ACCEPT_MAX_DELAY = 1.0
      private_constant :ACCEPT_RESOURCE_ERRORS, :ACCEPT_IMMEDIATE_ERRORS, :ACCEPT_INITIAL_DELAY, :ACCEPT_MAX_DELAY
      # The default ceiling of dynamic receive windows. grpc-go uses 16 MiB;
      # hosts stop at 1 MiB so unread request data stays bounded in memory.
      MAX_RECEIVE_WINDOW = 1_048_576
      private_constant :MAX_RECEIVE_WINDOW

      def initialize(application:, tls: nil, tls_provider: nil, max_connections: 1024, max_requests: 128,
                     handshake_timeout: 10, idle_timeout: 60, refresh_interval: 60, refresh_jitter: 0.1,
                     refresh_timeout: 10, on_tls_error: nil, alpn_protocols: ["h2"], http1: nil, stop_accepting: nil,
                     on_request_rejected: nil, lifecycle_factory: nil, keepalive: ServerKeepalive.new,
                     max_concurrent_streams: 100, request_body_timeout: 60,
                     response_body_timeout: 60, frame_write_timeout: 60,
                     stream_receive_window: 65_536,
                     connection_receive_window: 16_777_216, dynamic_receive_windows: true,
                     max_receive_window: MAX_RECEIVE_WINDOW)
        initialize_affinity
        raise ArgumentError, "application must be callable" unless application.respond_to?(:call)
        raise ArgumentError, "stop_accepting must be callable" if stop_accepting && !stop_accepting.respond_to?(:call)
        if on_request_rejected && !on_request_rejected.respond_to?(:call)
          raise ArgumentError, "on_request_rejected must be callable"
        end
        unless keepalive.nil? || keepalive.is_a?(ServerKeepalive)
          raise ArgumentError, "keepalive must be a ServerKeepalive or nil"
        end

        @lifecycle_factory = lifecycle_factory
        @application = application
        @http1 = http1
        @stop_accepting = stop_accepting
        @on_request_rejected = on_request_rejected
        @selector = ::Async::HTTP::Protocol::HTTP.new if http1
        @max_connections = Internal.positive_integer(max_connections, "max_connections")
        @max_requests = Internal.positive_integer(max_requests, "max_requests")
        unless max_concurrent_streams.is_a?(Integer) && max_concurrent_streams.between?(1, 0xffff_ffff)
          raise ArgumentError, "max_concurrent_streams must be an integer from 1 to 4294967295"
        end

        # One connection can never have more streams admitted than the host,
        # so a client queues locally instead of sending streams to refuse.
        @max_concurrent_streams = [max_concurrent_streams, @max_requests].min
        @stream_receive_window = Internal.receive_window(stream_receive_window, "stream_receive_window")
        @connection_receive_window = Internal.receive_window(connection_receive_window, "connection_receive_window")
        @receive_window_limit = Internal.receive_window_limit(dynamic_receive_windows, max_receive_window)
        unless request_body_timeout.nil?
          @request_body_timeout = Internal.seconds(request_body_timeout, "request_body_timeout")
        end
        unless response_body_timeout.nil?
          @response_body_timeout = Internal.seconds(response_body_timeout, "response_body_timeout")
        end
        unless frame_write_timeout.nil?
          @frame_write_timeout = Internal.seconds(frame_write_timeout, "frame_write_timeout")
        end
        @handshake_timeout = Internal.seconds(handshake_timeout, "handshake_timeout")
        @idle_timeout = Internal.seconds(idle_timeout, "idle_timeout") unless idle_timeout.nil?
        @keepalive = keepalive
        # One clock and random source for every connection's upkeep timers.
        @clock = Internal::MonotonicClock
        @random = ::Random
        @tls = TLSConfiguration.new(tls: tls, tls_provider: tls_provider, refresh_interval: refresh_interval,
                                    refresh_jitter: refresh_jitter, refresh_timeout: refresh_timeout,
                                    on_error: on_tls_error, alpn_protocols: alpn_protocols)
        @retirement_floor = 0
        # Host admission slots by HTTP/2 stream or HTTP/1 request: :reserved
        # while a stream's HEADERS took a slot its dispatch has not reached,
        # false once admitted, and true once its processor completed.
        @admitted = {}
        @h1_wire = {}
        @identities = {}
        install_owner_guard
      end

      def inspect = "#<#{self.class} state=#{@state} tls_generation=#{tls_generation.inspect}>"

      def start
        preflight_start
        return self if @state == :running

        @tls.start
        @state = :running
        self
      end

      def reload_tls(certificate_chain:, private_key:, client_ca:)
        check_running_tls!
        @tls.reload(certificate_chain: certificate_chain, private_key: private_key, client_ca: client_ca)
      end

      def tls_generation = @tls.generation
      def tls_status = @tls.status
      def refresh_tls = @tls.refresh

      def close(grace: 5)
        check_affinity!
        @tls.send(:check_recursion!)
        Internal.seconds(grace, "grace", zero: true)
        return @close_outcome if @state == :closed

        return nil if @cleanup&.current? || @closer&.current?

        owned = resource_owned_task?
        launch = @state != :closing
        if launch
          @state = :closing
          @closer = ::Async::Task.new(@owner, finished: false) do
            begin
              close_owned(Internal.now + grace)
            ensure
              abort_owned
            end
            @close_outcome
          end
        end
        if owned
          @closer.run if launch
          return @state == :closed ? @close_outcome : nil
        end

        waiter = @closer || @cleanup
        Internal.join_complete(waiter, failure: true, on_interrupt: method(:abort_owned)) { @closer.run if launch }
        @close_outcome
      end

      def abort
        check_affinity!
        @tls.send(:check_recursion!)
        abort_owned
        self
      end

      def drain_tls(grace: 5)
        check_running_tls!
        Internal.seconds(grace, "grace", zero: true)
        cutoff = tls_generation
        @retirement_floor = [@retirement_floor, cutoff].max
        targets = @resources.reject { |resource| resource.send(:retirement_completed?) }
                            .select { |resource| resource.generation < cutoff }
        targets.each { |resource| resource.draining = true }
        drain_resources(targets, Internal.now + grace, reason: "tls_rotation")
      end

      def peer_certificate
        return unless Thread.current.equal?(@thread) && Fiber.scheduler.equal?(@reactor)

        der = @identities[::Async::Task.current?]
        OpenSSL::X509::Certificate.new(der.dup) if der
      end

      # Accepted work must outlive cancellation of a listener's accept loop.
      def schedule(&)
        check_affinity!
        @lifetime.async(finished: false, &)
      end

      # Hosting SPI. Capture must happen in the accepting task before it
      # schedules any handshake work. The returned handle owns this one peer.
      # Listeners that take raw sockets through `accept_raw` never reach the
      # over-capacity close; it remains a fallback for other callers.
      def accept(raw)
        check_affinity!
        unless @state == :running && capacity?
          Internal.raw_close(raw)
          return
        end
        capture = @tls.send(:capture, raw)
        resource = Internal::Resource.new(progress: @progress, generation: capture.generation, snapshot: capture)
        resource.send(:retirement_parent=, @lifetime)
        resource.raw = raw
        @resources << resource
        Accepted.new(self, resource, @progress)
      rescue LifecycleError
        raise
      rescue StandardError
        Internal.raw_close(raw)
        nil
      end

      class Accepted
        def initialize(session, resource, progress)
          @session = session
          @resource = resource
          @progress = progress
        end

        def serve
          @session.send(:serve, @resource) { |resource| @resource = resource }
        end

        def close
          @resource.abort unless @resource.finishing?
        end

        def wait_closed
          @progress.wait until @resource.send(:retirement_completed?)
        end

        def timeout=(value)
          @resource.raw.timeout = value if @resource.raw.respond_to?(:timeout=)
        end

        def remote_address = @resource.raw.remote_address
        def inspect = "#<#{self.class} accepted connection>"
      end
      private_constant :Accepted

      private

      # Nonyielding calling-task validation, shared with Host before startup
      # cleanup can retire anything. A running session stays idempotent.
      def preflight_start
        check_affinity!
        return if @state == :running
        raise LifecycleError, "Server session cannot start after close" unless @state == :new

        @tls.send(:check_recursion!)
      end

      def capacity?
        @resources.reject! { |resource| resource.send(:retirement_completed?) }
        @resources.size < @max_connections
      end

      # Resources signal the shared progress condition when they change and
      # once their retirement completes, which frees their slot.
      def wait_for_capacity
        @progress.wait until capacity?
      end

      # Hosting SPI step shared by the Host and Falcon listeners. It takes a
      # peer from `listener` only while a slot is free, so peers above
      # `max_connections` wait in the kernel backlog, where a sibling listener
      # or worker process can take them. The capacity check, accept_nonblock
      # and the caller's following synchronous `accept(raw)` do not yield, so
      # concurrent listeners never exceed the limit. Resource errors back off
      # through `backoff`, which receives the delay in seconds. Other errors
      # propagate to the listener.
      def accept_raw(listener, backoff)
        delay = nil
        loop do
          wait_for_capacity
          listener.wait_readable
          next unless capacity?

          pair = listener.accept_nonblock(exception: false)
          return pair unless pair == :wait_readable
        rescue *ACCEPT_RESOURCE_ERRORS => e
          delay = delay ? [delay * 2, ACCEPT_MAX_DELAY].min : ACCEPT_INITIAL_DELAY
          begin
            warn "ConnectRPC accept error: #{e.class}; retrying in #{delay}s"
          rescue IOError, SystemCallError
            nil # A broken diagnostic stream must not end the listener.
          end
          backoff.call(delay)
        rescue *ACCEPT_IMMEDIATE_ERRORS
          # Cross a scheduling boundary so a sustained stream of these
          # cannot starve established connections or close.
          ::Async::Task.current.yield
        end
      end

      def check_running_tls!
        check_affinity!
        raise LifecycleError, "A running TLS host is required" unless @state == :running && tls_generation
      end

      def serve(resource, &handoff)
        return if resource.finished?

        resource.task = ::Async::Task.current
        expires = Internal.now + @handshake_timeout
        # Setup hands an HTTP/1 replacement to this serve, the accepted handle
        # and the session's mapping in one step, so a cancellation of this
        # wait aborts whichever resource is mapped.
        setup = resource.track(::Async::Task.new(::Async::Task.current, finished: false) do
          setup_connection(resource) do |replacement|
            resource = replacement
            handoff&.call(replacement)
          end
        end)
        setup.run
        Internal.wait(setup, expires)
        # A session abort or drain that started during setup ends serving here.
        # Setup leaves the original resource mapped, without a connection,
        # when it refused an HTTP/1 replacement.
        return unless resource.connection && serving?(resource)

        resource.ready = true
        resource.changed
        if resource.connection.is_a?(Internal::ServerConnection)
          keep_up(resource)
        elsif @idle_timeout
          idle = @lifetime.async(finished: false) { watch_idle(resource) }
        end
        resource.connection.start_connection if resource.connection.respond_to?(:start_connection)
        resource.connection.each { |request| dispatch(resource, request) }
      rescue StandardError
        # Socket/setup/protocol failures terminate only this owned connection.
        nil
      ensure
        Internal.cancel(idle)
        resource.abort unless resource.finishing?
      end

      def setup_connection(resource)
        socket = resource.snapshot.handshake { |socket| resource.socket = socket }
        resource.socket = socket
        stream = ::IO::Stream(socket)
        if @http1 && ((!resource.snapshot.tls? &&
                       @selector.protocol_for(stream) == ::Async::HTTP::Protocol::HTTP1) ||
                      (resource.snapshot.tls? && socket.alpn_protocol != "h2"))
          return resource unless serving?(resource)

          replacement = @http1.call(stream, progress: @progress, generation: resource.generation)
          # Session abort maps nothing new; it may have started while the
          # callable ran.
          return resource unless serving?(resource)

          replacement.send(:retirement_parent=, @lifetime)
          replacement.raw = resource.raw
          replacement.socket = socket
          replacement.snapshot = resource.snapshot
          replacement.task = resource.task
          @resources[@resources.index(resource)] = replacement
          yield replacement
          return replacement
        end
        if resource.snapshot.tls? && socket.alpn_protocol != "h2"
          raise OpenSSL::SSL::SSLError, "HTTP/2 ALPN is required"
        end

        connection = Internal::ServerConnection.new(stream)
        resource.connection = connection
        connection.resource = resource
        connection.stream_limit = @max_concurrent_streams
        connection.admission = self
        connection.receive_windows(stream: @stream_receive_window, connection: @connection_receive_window,
                                   limit: @receive_window_limit)
        connection.read_connection_preface(Internal.settings(@max_requests,
                                                             max_concurrent_streams: @max_concurrent_streams,
                                                             stream_window: @stream_receive_window))
        connection.grow_connection_window
        resource
      end

      def serving?(resource)
        !@aborting && !resource.finished? && !resource.draining &&
          !(tls_generation && resource.generation < @retirement_floor)
      end

      # Every HTTP/2 connection has upkeep for its control frames, refusals and
      # graceful drains. Idle expiry drains it; keepalive adds PINGs, PING
      # enforcement and the age limit.
      def keep_up(resource)
        upkeep = Internal::ServerUpkeep.new(resource, parent: @lifetime, clock: @clock, random: @random,
                                                      keepalive: @keepalive, idle_timeout: @idle_timeout,
                                                      write_timeout: @frame_write_timeout)
        # A drain that starts while the upkeep's tasks start keeps its frames
        # queued for the writer. The reader starts later.
        resource.upkeep = upkeep
        upkeep.start
      end

      # HTTP/1 resources are aborted once idle.
      def watch_idle(resource)
        loop do
          if resource.empty?
            remaining = @idle_timeout - (Internal.now - resource.activity_at)
            if remaining <= 0
              resource.abort
              break
            end
            begin
              ::Async::Task.current.with_timeout([remaining, Internal::MAX_WAIT].min) { resource.progress.wait }
            rescue ::Async::TimeoutError
              next
            end
          else
            resource.progress.wait
          end
          break if resource.finished?
        end
      end

      def dispatch(resource, request)
        if resource.respond_to?(:prepare_stream_input) && @application.respond_to?(:http1_streaming_route?, true) &&
           @application.send(:http1_streaming_route?, request)
          retirement = resource.prepare_stream_input(request.body)
        end
        lifecycle = @lifecycle_factory&.call(request)
        if lifecycle
          exchange = Internal::Exchange.new(lifecycle)
          if request.respond_to?(:stream)
            request.stream.exchange = exchange
          else
            resource.exchange = exchange
          end
        end
        prune_admissions
        http2 = request.respond_to?(:stream)
        key = http2 ? request.stream : request
        # The stream closed while the lifecycle factory ran, as when the peer
        # resets it: pruning released any reservation, and it is neither
        # admitted nor refused. Its worker finishes the exchange as not
        # delivered.
        return if http2 && key.closed?

        # A stream reserved while its HEADERS were read already holds its
        # slot. An HTTP/2 stream on a draining connection was opened before
        # its cutoff, so only full admission or an abort refuses it, with
        # REFUSED_STREAM. Aborting starts no more application work.
        reserved = http2 && @admitted[key] == :reserved
        if (!reserved && @admitted.size >= @max_requests) || @aborting || resource.finished? ||
           (!http2 && (@state != :running || resource.draining))
          @admitted.delete(key) if reserved
          exchange&.decision = :admission_rejected
          @on_request_rejected&.call
          return Internal::ServerConnection::REFUSAL if http2

          exchange&.http_status = 503
          retirement&.processor_finished
          retirement&.head_resolved(503)
          return @application.admission_response(request) if @application.respond_to?(:admission_response)

          return ::Protocol::HTTP::Response[503, {}, nil]
        end

        @admitted[key] = false
        begin
          if http2
            bound_input(request)
            bound_output(request)
          end
          retirement&.on_close do
            complete_admission(key, wire: true)
            resource.changed
          end
          publishes = request.respond_to?(:stream) ||
                      (@application.respond_to?(:publication?, true) && @application.send(:publication?, request))
          publication = Internal::Publication.new if publishes
          handler = ::Async::Task.new(@lifetime, finished: false) do |task|
            @identities[task] = resource.snapshot.authenticated_leaf
            work = lambda do
              if publication && @application.respond_to?(:call_with_publication)
                @application.call_with_publication(request, lifecycle: lifecycle, publication: publication,
                                                            resource: resource)
              elsif lifecycle
                @application.call(request, lifecycle: lifecycle)
              else
                @application.call(request)
              end
            end
            application_work = work
            work = -> { lifecycle.send(:with_processing_completion, &application_work) } if lifecycle && publication
            result = lifecycle ? lifecycle.activate(&work) : work.call
            publication.publish(result) if publication && !publication.published?
            result
          rescue Exception => e # rubocop:disable Lint/RescueException -- cancellation also completes publication
            failure = e
            raise unless publication && e.is_a?(StandardError)

            lifecycle&.record_failure(e)
            publication.complete(e)
            nil
          ensure
            complete_processor(publication, task, key, resource, failure)
          end
          resource.track(handler)
          handler.run
          response = publication ? publication.response : handler.wait
          retirement&.head_resolved(response&.status)
          exchange.http_status = response&.status if exchange
          if !request.respond_to?(:stream) && response
            response.body = Internal::AdmissionBody.new(response.body) do
              # Connect retirement owns completion through post-delivery hygiene.
              complete_admission(key, wire: true) unless retirement
              resource.changed
            end
            deferred = true
            # An early Web head can stop an HTTP/1 client's unfinished upload.
            # Keep input cleanup in the original processor before serialization;
            # consumed unary input still permits incremental server streams.
            # Connect retirement owns input hygiene after response delivery.
            if publication && !retirement && GRPCWeb.intent?(Internal.headers(request)) &&
               request.body && !request.body.empty?
              begin
                publication.wait
              rescue Exception => e # rubocop:disable Lint/RescueException -- cancellation must close deferred ownership too
                begin
                  response.body.close(e)
                ensure
                  raise e
                end
              end
            end
          end
          response
        ensure
          complete_admission(key, processor: true) unless handler
          complete_admission(key, wire: true) unless deferred || retirement
          resource.changed
        end
      end

      # Client-streaming and bidi routes may idle between messages; every other
      # admitted HTTP/2 request body must make progress while its handler waits.
      def bound_input(request)
        return unless @request_body_timeout
        return if @application.respond_to?(:input_stream_route?, true) &&
                  @application.send(:input_stream_route?, request)

        request.stream.bound_input(@clock, @request_body_timeout)
      end

      # Server-streaming and bidi responses may wait for credit while their
      # consumer applies backpressure; every other admitted HTTP/2 response
      # must receive flow-control credit within the bound.
      def bound_output(request)
        return unless @response_body_timeout
        return if @application.respond_to?(:output_stream_route?, true) &&
                  @application.send(:output_stream_route?, request)

        request.stream.bound_output(@clock, @response_body_timeout)
      end

      def complete_processor(publication, task, key, resource, error)
        @identities.delete(task)
        complete_admission(key, processor: true)
        resource.changed
        publication&.complete(error)
      end

      # Called by the connection reader while a new stream's HEADERS are
      # read, without yielding. Takes a host admission slot for the stream
      # unless none is free, so a burst of HEADERS read together cannot
      # overcommit before any of them dispatches.
      def reserve_admission(stream)
        prune_admissions
        return false if @aborting || stream.connection.resource&.finished? || @admitted.size >= @max_requests

        @admitted[stream] = :reserved
      end

      # Observes a stream refused at HEADERS once, like a refusal at
      # dispatch, but in a task of its own: the reader never waits for or
      # fails on it, and retiring the connection does not drop it. The
      # refusal is counted first, so a failing or cancelled factory cannot
      # skip it. Delivery completes once the reset is written, whatever ran
      # before. The lifecycle never activates.
      def refuse_admission(request, written)
        return if @lifetime.finished?

        @lifetime.async(finished: false) do
          observe_safely { @on_request_rejected&.call }
          lifecycle = observe_safely { @lifecycle_factory&.call(request) }
          if lifecycle
            exchange = Internal::Exchange.new(lifecycle)
            exchange.decision = :admission_rejected
          end
          written.wait
        ensure
          exchange&.finish(written.value == true)
        end
      end

      def observe_safely
        yield
      rescue StandardError
        nil
      end

      # Capacity is only consulted here, so pruning is as prompt as a close
      # hook. A reservation ends with its stream or connection; an admitted
      # stream ends once its processor completed and its stream closed.
      def prune_admissions
        @admitted.delete_if do |key, state|
          next false unless key.respond_to?(:closed?)

          state == :reserved ? key.closed? || key.connection.retiring? : state == true && key.closed?
        end
      end

      def complete_admission(key, processor: false, wire: false)
        # An absent admission is terminal. Late or duplicate events cannot
        # recreate either side of the processor/wire join.
        return unless @admitted.key?(key)

        @admitted[key] = true if processor
        if key.respond_to?(:closed?)
          @admitted.delete(key) if @admitted[key] == true && (@aborting || key.closed?)
        else
          @h1_wire[key] = true if wire
          if @admitted[key] == true && @h1_wire[key]
            @admitted.delete(key)
            @h1_wire.delete(key)
          end
        end
      end

      # HTTP/2 connections start a two-phase GOAWAY drain with `reason` as
      # debug data. HTTP/1 connections drain without one.
      def drain_resources(resources, expires, reason:)
        tasks = resources.map do |resource|
          @lifetime.async(finished: false) do
            if resource.is_a?(Internal::Resource)
              resource.drain(@lifetime, expires, reason: reason)
            else
              resource.drain(@lifetime, expires)
            end
          end
        end
        tasks.map(&:wait).all?
      end

      def stop_accepting
        @stop_accepting&.call
      end

      def close_owned(expires)
        stop_accepting
        @tls.close
        @resources.each { |resource| resource.draining = true }
        drained = drain_resources(@resources, expires, reason: "graceful_stop")
        @admitted.clear
        @h1_wire.clear
        decide_close_outcome(drained)
      end

      def install_owner_guard
        @lifetime = @owner.async do
          ::Async::Condition.new.wait
        ensure
          abort_owned unless @state == :closed
        end
      end

      def resource_owned_task?
        @resources.any? { |resource| resource.send(:owns_task?, ::Async::Task.current) }
      end

      def decide_close_outcome(outcome)
        return if @close_decided

        @close_decided = true
        @close_outcome = outcome
      end

      def abort_owned
        return if @cleanup&.current?

        owned = resource_owned_task?
        launch = !@cleanup
        if launch
          @aborting = true
          @state = :closing unless @state == :closed
          decide_close_outcome(false)
          @cleanup = ::Async::Task.new(@lifetime, transient: true, finished: false) { retire_session }
        end
        if owned
          @cleanup.run if launch
        else
          Internal.join_complete(@cleanup, failure: true) { @cleanup.run if launch }
        end
      end

      def retire_session
        # Every accepted peer fails before this cleanup can yield. A raw
        # shutdown never yields; listener cleanup and each resource's own
        # retirement can. Only a resource's retirement closes its descriptor,
        # once its tasks have left it, so a failure or interruption never skips
        # a later step. Listeners prune @resources while a step yields, so each
        # pass walks its own array.
        resources = @resources.dup
        resources.each { |resource| resource.send(:fail_raw) if resource.respond_to?(:fail_raw, true) }
        failures = [Internal::Teardown.capture { stop_accepting }]
        # Setup maps no HTTP/1 replacement once this abort has started, so
        # these are every resource the session holds.
        failures.concat((resources | @resources).map { |resource| Internal::Teardown.capture { resource.abort } })
        # Every resource has run its retirement, and a retirement that failed
        # has still failed and closed its raw socket. Nothing can retry it, so
        # the session closes whatever failed, and then reports the first
        # interruption, otherwise the first failure, as in CompletionJoin.
        @admitted.clear
        @h1_wire.clear
        failures << Internal::Teardown.capture { @tls.close }
        finish_closed(@close_outcome)
        failure = Internal::Teardown.first(failures)
        # Cancelling the guard can stop this cleanup too, once its own
        # cancellation made it non-transient. That stop never replaces the
        # outcome above.
        stopped = Internal::Teardown.capture { Internal.cancel(@lifetime) }
        failure ||= stopped
        raise failure if failure
      end
    end
  end
end
