# frozen_string_literal: true

module ConnectRPC
  module Async
    class ServerSession
      include Internal::Affinity

      def initialize(application:, tls: nil, tls_provider: nil, max_connections: 128, max_requests: 128,
                     handshake_timeout: 10, idle_timeout: 60, refresh_interval: 60, refresh_jitter: 0.1,
                     refresh_timeout: 10, on_tls_error: nil, alpn_protocols: ["h2"], http1: nil, stop_accepting: nil,
                     on_request_rejected: nil, lifecycle_factory: nil)
        initialize_affinity
        raise ArgumentError, "application must be callable" unless application.respond_to?(:call)
        raise ArgumentError, "stop_accepting must be callable" if stop_accepting && !stop_accepting.respond_to?(:call)
        if on_request_rejected && !on_request_rejected.respond_to?(:call)
          raise ArgumentError, "on_request_rejected must be callable"
        end

        @lifecycle_factory = lifecycle_factory
        @application = application
        @http1 = http1
        @stop_accepting = stop_accepting
        @on_request_rejected = on_request_rejected
        @selector = ::Async::HTTP::Protocol::HTTP.new if http1
        @max_connections = Internal.positive_integer(max_connections, "max_connections")
        @max_requests = Internal.positive_integer(max_requests, "max_requests")
        @handshake_timeout = Internal.seconds(handshake_timeout, "handshake_timeout")
        @idle_timeout = Internal.seconds(idle_timeout, "idle_timeout") unless idle_timeout.nil?
        @tls = TLSConfiguration.new(tls: tls, tls_provider: tls_provider, refresh_interval: refresh_interval,
                                    refresh_jitter: refresh_jitter, refresh_timeout: refresh_timeout,
                                    on_error: on_tls_error, alpn_protocols: alpn_protocols)
        @retirement_floor = 0
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
        drain_resources(targets, Internal.now + grace)
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
      def accept(raw)
        check_affinity!
        @resources.reject! { |resource| resource.send(:retirement_completed?) }
        unless @state == :running && @resources.size < @max_connections
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

      def check_running_tls!
        check_affinity!
        raise LifecycleError, "A running TLS host is required" unless @state == :running && tls_generation
      end

      def serve(resource)
        return if resource.finished?

        resource.task = ::Async::Task.current
        expires = Internal.now + @handshake_timeout
        setup = resource.track(::Async::Task.new(::Async::Task.current, finished: false) { setup_connection(resource) })
        setup.run
        resource = Internal.wait(setup, expires)
        yield resource if block_given?
        return if resource.draining || (tls_generation && resource.generation < @retirement_floor)

        resource.ready = true
        resource.changed
        resource.connection.start_connection if resource.connection.respond_to?(:start_connection)
        idle = @lifetime.async(finished: false) { watch_idle(resource) } if @idle_timeout
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
          return resource if resource.draining || (tls_generation && resource.generation < @retirement_floor)

          replacement = @http1.call(stream, progress: @progress, generation: resource.generation)
          replacement.send(:retirement_parent=, @lifetime)
          replacement.raw = resource.raw
          replacement.socket = socket
          replacement.snapshot = resource.snapshot
          replacement.task = resource.task
          @resources[@resources.index(resource)] = replacement
          return replacement
        end
        if resource.snapshot.tls? && socket.alpn_protocol != "h2"
          raise OpenSSL::SSL::SSLError, "HTTP/2 ALPN is required"
        end

        connection = Internal::ServerConnection.new(stream)
        resource.connection = connection
        connection.resource = resource
        connection.read_connection_preface(Internal.settings(@max_requests))
        resource
      end

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
        @admitted.delete_if { |stream, completed| completed && stream.respond_to?(:closed?) && stream.closed? }
        key = request.respond_to?(:stream) ? request.stream : request
        if @state != :running || resource.draining || @admitted.size >= @max_requests
          if exchange
            exchange.decision = :admission_rejected
            exchange.http_status = 503
          end
          @on_request_rejected&.call
          retirement&.processor_finished
          retirement&.head_resolved(503)
          return @application.admission_response(request) if @application.respond_to?(:admission_response)

          return ::Protocol::HTTP::Response[503, {}, nil]
        end

        @admitted[key] = false
        begin
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
          rescue StandardError => e
            raise unless publication

            lifecycle&.record_failure(e)
            publication.complete(e)
            nil
          ensure
            complete_processor(publication, task, key, resource)
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

      def complete_processor(publication, task, key, resource)
        @identities.delete(task)
        complete_admission(key, processor: true)
        resource.changed
        publication&.complete($ERROR_INFO)
      end

      def complete_admission(key, processor: false, wire: false)
        # An absent admission is terminal. Late or duplicate events cannot
        # recreate either side of the processor/wire join.
        return unless @admitted.key?(key)

        @admitted[key] = true if processor
        if key.respond_to?(:closed?)
          @admitted.delete(key) if @admitted[key] && (@aborting || key.closed?)
        else
          @h1_wire[key] = true if wire
          if @admitted[key] && @h1_wire[key]
            @admitted.delete(key)
            @h1_wire.delete(key)
          end
        end
      end

      def drain_resources(resources, expires)
        tasks = resources.map do |resource|
          @lifetime.async(finished: false) { resource.drain(@lifetime, expires) }
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
        drained = drain_resources(@resources, expires)
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
        # Listener cleanup can yield. Fail every peer before crossing it.
        @resources.each { |resource| Internal.raw_close(resource.raw) }
        stop_accepting
        @resources.each(&:abort)
        @admitted.clear
        @h1_wire.clear
        @tls.close
        finish_closed(@close_outcome)
        Internal.cancel(@lifetime)
      end
    end
  end
end
