# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # A server advertises its per-connection stream limit, which can be
      # lower than its host-wide request admission.
      def self.settings(max_requests, max_concurrent_streams: max_requests, stream_window: 65_536)
        {
          ::Protocol::HTTP2::Settings::ENABLE_PUSH => 0,
          ::Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS => max_concurrent_streams,
          ::Protocol::HTTP2::Settings::MAXIMUM_FRAME_SIZE => 16_384,
          ::Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE => stream_window
        }
      end

      # From the RFC 9113 initial window, 65,535, to the largest it allows.
      # The stream window is also each stream's retained ingress bound.
      # Connection credit returns on receipt, so the connection window bounds
      # bytes in flight; retained bytes stay within the stream windows.
      def self.receive_window(value, name)
        unless value.is_a?(Integer) && value.between?(65_535, ::Protocol::HTTP2::MAXIMUM_ALLOWED_WINDOW_SIZE)
          raise ArgumentError, "#{name} must be an integer from 65535 to 2147483647"
        end

        value
      end

      # Raised before a writer enters the guard of a connection that can no
      # longer start streams. Nothing has been written for that writer.
      class UnusableConnection < IOError; end

      # Keepalive's view of a connection: frame reads, PING ACKs and the work
      # a dead peer would leave stuck. Shared by client and server connections.
      module Liveness
        # Tasks waiting to acquire the frame guard, including unsent dispatchers.
        def guard_waiting? = !@frame_guard.waiting.empty?

        # Stream output tasks, retained past their stream's removal: a reset
        # stream's output can still be inside a deferred frame write.
        def output_started(task)
          outputs = @outputs ||= {}.compare_by_identity
          outputs.delete_if { |output, _| output.finished? }
          outputs[task] = true
        end

        def outputs_pending? = !!@outputs&.each_key&.any? { |output| !output.finished? }

        # Timestamped before the frame is applied: a blocked synchronous reply,
        # such as a PING ACK, cannot hide that the frame arrived.
        def read_frame
          super do |frame|
            @resource&.frame_read
            yield frame if block_given?
          end
        end

        def receive_ping(frame)
          @resource&.ping_acknowledged(frame.unpack) if frame.acknowledgement?
          super
        end
      end

      # Receive windows of a client or server connection.
      module ReceiveWindows
        # Set before the preface. Streams use the stream window before the
        # peer acknowledges SETTINGS: accepting more than the peer may yet
        # send is safe, and a peer may spend the larger window before its ACK.
        def receive_windows(stream:, connection:)
          @stream_receive_window = stream
          @connection_receive_window = connection
        end

        attr_reader :stream_receive_window

        # Grows the connection window from the RFC 9113 initial 65,535 with
        # one WINDOW_UPDATE on stream 0, as net/http2 does, instead of waiting
        # for the first DATA. Credit is reserved before the write, as for
        # stream updates.
        def grow_connection_window
          target = @connection_receive_window
          return unless target

          local_window.desired = target
          increment = target - local_window.available
          return unless increment.positive?

          local_window.expand(increment)
          frame = ::Protocol::HTTP2::WindowUpdateFrame.new(0)
          frame.pack(increment)
          write_frame(frame)
        end

        # protocol-http2 retargets connection replenishment to the stream
        # window when the peer acknowledges SETTINGS.
        def update_local_settings(changes)
          super
          local_window.desired = @connection_receive_window if @connection_receive_window
        end
      end

      # Version-coupled wrappers around the selected stack, not a frame codec.
      module Connection # rubocop:disable Metrics/ModuleLength
        include Liveness
        include ReceiveWindows

        attr_accessor :resource, :defer_close
        attr_reader :writers, :reader_task

        def initialize(...)
          super
          # Every frame write reaches this guard through #synchronize.
          @frame_guard = ::Async::Semaphore.new(1)
          @writers = {}
          @writer = nil
        end

        # A dispatcher has allocated and written nothing while it waits, so it
        # is not a retirement writer until it holds the guard. It then rechecks
        # before registering, without yielding, and returns to acquisition.
        def synchronize(dispatch: false)
          task = ::Async::Task.current
          return yield if @writer.equal?(task)

          raise UnusableConnection, "HTTP/2 connection closed" if @resource&.finished?

          @writers[task] = true unless dispatch
          begin
            @frame_guard.acquire do
              if dispatch
                raise UnusableConnection, "HTTP/2 connection closed" if @resource&.finished?

                @writers[task] = true
              end
              @writer = task
              begin
                yield
              ensure
                @writer = nil
              end
            end
          ensure
            @writers.delete(task)
          end
        end

        # Acquire while cancellable. Once a frame starts, encode/flush/state
        # must finish together; a second cancellation is reserved for raw abort.
        def write_frame(frame)
          synchronize { ::Async::Task.current.defer_cancel { timed_write { super } } }
        end

        def write_frames(&)
          synchronize { ::Async::Task.current.defer_cancel { timed_write { super(&) } } }
        end

        # Mirror async-http 0.105.0 and Async 2.46's eager Task#async, publishing
        # the actual transient reader before EOF/error can reenter retirement.
        def read_in_background(parent: ::Async::Task.current)
          raise "Connection is closed!" if closed?
          raise ::Async::Task::FinishedError if parent.finished?

          reader = @reader_task = ::Async::Task.new(parent, transient: true) do |task|
            @reader = task
            task.annotate("#{version} reading data for #{self.class}.")
            begin
              until closed?
                consume_window
                read_frame
              end
            rescue StandardError => e
              # Upstream forwards the original reader failure through close.
            ensure
              if @reader
                @reader = nil
                close(e)
              end
            end
          end
          reader.run
          reader
        end

        def consume_local_window(frame)
          if frame.length > local_window.available
            raise ::Protocol::HTTP2::FlowControlError, "DATA exceeds connection receive window"
          end

          super
        end

        def delete(id)
          super
        ensure
          @resource&.changed
        end

        # The reader closes with its failure, or with nil once a GOAWAY closed
        # the connection. A drain may keep the resource after the peer left.
        def close(error = nil)
          close_requests
          if @defer_close
            @resource&.changed
          elsif @resource && @reader_task&.current? && @resource.outlives_peer?(error)
            @resource.peer_departed!
          elsif @resource
            @resource.abort(error)
          else
            super
          end
        end

        def finish_close(error = nil)
          close_requests
          # Skip our override while retaining upstream queue/reader/framer cleanup.
          super_method = method(:close).super_method
          super_method.call(error)
        end

        # Retirement closes raw TCP, and abort marks the resource finished,
        # before close! marks this connection closed. Session retirement closes
        # every raw socket before it aborts each resource.
        def retiring? = closed? || !!@resource&.finished? || !!@resource&.raw&.closed?

        private

        # Only server connections have a request queue.
        def close_requests = nil

        # Server upkeep times each outermost frame write, from holding the
        # guard to its flush, so its watchdog never counts guard waits or
        # application cleanup that runs under the guard.
        def timed_write
          upkeep = @resource&.upkeep
          return yield if @timing || !upkeep

          @timing = true
          upkeep.write_started
          begin
            yield
          ensure
            upkeep.write_finished
            @timing = false
          end
        end
      end

      class GRPCHeaderError < ::Protocol::HTTP2::HeaderError
        def initialize(message, code)
          super(message)
          @code = code
        end
      end

      # A server response body waited longer than response_body_timeout for
      # flow-control credit.
      class ResponseBodyTimeout < IOError; end

      # Publish ownership before the pinned output's eager first body read.
      class Output < ::Async::HTTP::Protocol::HTTP2::Output
        def start(parent: ::Async::Task.current)
          raise "Task already started!" if @task

          work = method(@body.stream? ? :stream : :passthrough)
          task = @task = ::Async::Task.new(parent, &work)
          yield task
          task.run
          task
        end

        # Bounds each wait for flow-control credit to `seconds` on `clock`.
        # Only credit ends a wait, so a wakeup that leaves the window empty
        # does not restart it. On expiry the stream is reset with CANCEL
        # and the write raises ResponseBodyTimeout.
        def bound_progress(clock, seconds)
          @progress_clock = clock
          @progress_seconds = seconds
        end

        # async-http 0.105.0's loop, with its credit wait bounded.
        def write(chunk)
          until chunk.empty?
            stream = @stream or raise IOError, "HTTP/2 stream is closed!"
            maximum_size = stream.available_frame_size
            maximum_size = await_credit(stream) if maximum_size <= 0
            break unless (chunk = send_data(stream, chunk, maximum_size))
          end
        end

        private

        def await_credit(stream)
          @stalled = false
          timer = credit_timer if @progress_seconds
          maximum_size = @guard.synchronize do
            size = stream.available_frame_size
            until size.positive? || @stalled
              @window_updated.wait(@guard)
              stream = @stream or raise IOError, "HTTP/2 stream is closed!"
              size = stream.available_frame_size
            end
            size
          end
          return maximum_size if maximum_size.positive?

          stream.output_stalled
          @stream = nil
          raise ResponseBodyTimeout, "HTTP/2 response body received no flow-control credit within response_body_timeout"
        ensure
          Internal.cancel(timer)
        end

        # Lives only inside one credit wait. It only wakes the waiting
        # output, which resets its own stream, so it never stops a task.
        def credit_timer
          timer = ::Async::Task.new(::Async::Task.current, finished: false) do
            @progress_clock.sleep(@progress_seconds)
            @guard.synchronize do
              @stalled = true
              @window_updated.broadcast
            end
          end
          timer.run
          timer
        end
      end

      module Stream # rubocop:disable Metrics/ModuleLength
        attr_reader :grpc_initial_headers, :grpc_initial_end_stream, :grpc_trailers, :grpc_error, :grpc_reset,
                    :declared_length

        def initialize(...)
          super
          window = connection.stream_receive_window
          local_window.capacity = window if window && window > local_window.capacity
        end

        def grpc_mark! = @grpc = true
        def grpc? = !!@grpc

        def grpc_fields(headers)
          size = headers.sum { |key, value| key.bytesize + value.bytesize + 32 }
          if size > GRPCProtocol::FIELD_LIMIT
            @grpc_error = Error.new(:resource_exhausted, "gRPC field block exceeds byte limit")
            raise GRPCHeaderError.new("gRPC field block exceeds byte limit", ::Protocol::HTTP2::Error::ENHANCE_YOUR_CALM)
          end
          values = {}
          headers.each { |key, value| (values[key] ||= []) << value unless key.start_with?(":") }
          GRPCProtocol.snapshot(values)
        end

        def receive_initial_headers(headers, end_stream)
          @grpc_web ||= headers.any? do |key, value|
            key.casecmp?("content-type") && value.downcase.start_with?("application/grpc-web")
          end
          @grpc ||= !@grpc_web && headers.any? do |key, value|
            key.casecmp?("content-type") && value.downcase.start_with?("application/grpc")
          end
          grpc_fields(headers) if @grpc_web
          if @grpc
            fields = grpc_fields(headers)
            status = headers.find { |key, _value| key == ":status" }&.last.to_i
            informational = status.between?(100, 199)
            if informational && end_stream
              @grpc_error = Error.new(:internal, "Informational gRPC HEADERS ended the stream")
              raise GRPCHeaderError.new("Informational END_STREAM", ::Protocol::HTTP2::Error::PROTOCOL_ERROR)
            end
            if @grpc_initial_headers
              @grpc_error = Error.new(:internal, "Nonfinal second gRPC HEADERS")
              raise GRPCHeaderError.new("Nonfinal second gRPC HEADERS", ::Protocol::HTTP2::Error::PROTOCOL_ERROR)
            end
            unless informational
              @grpc_initial_headers = fields
              @grpc_initial_end_stream = end_stream
            end
          end
          super
        ensure
          @declared_length = @length
        end

        def receive_trailing_headers(headers, end_stream)
          grpc_fields(headers) if @grpc_web
          if @grpc
            @grpc_trailers = grpc_fields(headers)
            unless end_stream
              @grpc_error = Error.new(:internal, "Nonfinal gRPC trailing HEADERS")
              raise GRPCHeaderError.new("Nonfinal gRPC trailing HEADERS", ::Protocol::HTTP2::Error::PROTOCOL_ERROR)
            end
          end
          self.headers.trailer!
          super
        end

        def prepare_input(length)
          raise ArgumentError, "Input body already prepared!" if @input

          @reclaimed_input = 0
          @input = Ingress.new(self, length)
        end

        def receive_data(frame)
          if %i[open half_closed_local].include?(state) && frame.length > local_window.available
            @input&.retire(::Protocol::HTTP2::StreamError.for(::Protocol::HTTP2::Error::FLOW_CONTROL_ERROR))
            send_reset_stream(::Protocol::HTTP2::Error::FLOW_CONTROL_ERROR)
            return
          end

          super
        end

        def receive_reset_stream(frame)
          if @grpc
            @grpc_reset = frame.unpack
            if @grpc_reset == ::Protocol::HTTP2::Error::NO_ERROR
              fields = @grpc_initial_end_stream ? @grpc_initial_headers : @grpc_trailers
              statuses = fields && fields["grpc-status"]
              unless statuses && statuses.size == 1 && !GRPCProtocol.status_number(statuses.first).nil?
                @grpc_error ||= Error.new(:internal, "HTTP/2 NO_ERROR reset without gRPC status")
              end
            else
              code = { 3 => :resource_exhausted, 7 => :unavailable, 8 => :canceled,
                       11 => :resource_exhausted, 12 => :permission_denied }.fetch(@grpc_reset, :internal)
              @grpc_error ||= Error.new(code, "HTTP/2 stream reset")
            end
          end
          @input&.retire(nil, discard: true) unless state == :idle || frame.unpack == ::Protocol::HTTP2::Error::NO_ERROR
          super
        end

        def process_data(frame)
          @grpc_data_end_stream = true if @grpc && frame.end_stream?
          data = frame.unpack
          if (input = @input)
            input.write(data) unless data.empty?
            input.close_write if frame.end_stream?
            discarded = frame.length - data.bytesize
          else
            discarded = frame.length
          end
          reclaim_input(discarded)
          data
        rescue ::Protocol::HTTP2::ProtocolError
          raise
        rescue StandardError
          send_reset_stream(::Protocol::HTTP2::Error::INTERNAL_ERROR)
        end

        # Credit cannot help a retiring connection, whose guard or closed
        # socket would fail a consumer that is only reading buffered input.
        def reclaim_input(amount)
          return if closed? || state == :half_closed_remote || connection.retiring?

          @reclaimed_input = (@reclaimed_input || 0) + amount
          request_window_update
        end

        def request_window_update
          return unless @reclaimed_input&.positive? && local_window.limited?

          connection.synchronize do
            next if closed? || state == :half_closed_remote || connection.retiring?
            next unless @reclaimed_input.positive? && local_window.limited?

            ::Async::Task.current.defer_cancel do
              amount = @reclaimed_input
              @reclaimed_input = 0
              # Reserve credit before flush can yield to the reader. A peer
              # may spend the update as soon as it reaches the socket.
              local_window.expand(amount)
              frame = ::Protocol::HTTP2::WindowUpdateFrame.new(id)
              frame.pack(amount)
              write_frame(frame)
            end
          end
        end

        def closed(error)
          @reclaimed_input = 0
          @input&.retire(error)
          super
        end

        def finish_output(error = nil)
          if (@grpc || @grpc_web) && error.is_a?(ConnectRPC::Error) && error.code == "resource_exhausted"
            @output = nil
            send_reset_stream(::Protocol::HTTP2::Error::ENHANCE_YOUR_CALM) unless closed?
            return
          end
          super
        end

        def send_headers(headers, flags = 0)
          if @grpc || @grpc_web
            values = {}
            headers.each { |key, value| (values[key] ||= []) << value.to_s }
            begin
              GRPCProtocol.check_fields!(values)
            rescue Error => e
              @grpc_error = e
              raise
            end
          end
          super
        end

        def send_body(body, trailer = nil, &)
          # A peer reset can close the stream while deferred headers flush.
          # Upstream otherwise starts an output task on the closed stream.
          return body.close if closed?

          @output = Output.new(self, body, trailer)
          @output.bound_progress(*@output_bound) if @output_bound
          @output.start do |task|
            connection.output_started(task)
            yield task if block_given?
          end
        end
      end

      class RequestStream < ::Async::HTTP::Protocol::HTTP2::Request::Stream
        include Stream

        attr_accessor :exchange
        attr_reader :response_task

        def initialize(...)
          @cancellation_source = CancellationSource.new
          super
        end

        def processing_cancellation = @cancellation_source&.token

        def detach_cancellation
          @cancellation_source = nil
        end

        # Bounds each handler wait for request body progress.
        def bound_input(clock, seconds) = @input&.bound_progress(clock, seconds)

        # The handler waited too long for request body progress. The control
        # writer sends CANCEL and the stream closes without yielding.
        def input_stalled
          connection.upkeep&.reset(self, ::Protocol::HTTP2::Error::CANCEL) unless closed?
        end

        # Bounds each wait of the response body for flow-control credit.
        def bound_output(clock, seconds)
          @output_bound = [clock, seconds]
        end

        # Called by the output task itself, whose response waited too long
        # for credit. Detaching the output first keeps the close from
        # stopping that task; the control writer sends CANCEL.
        def output_stalled
          @output = nil
          connection.upkeep&.reset(self, ::Protocol::HTTP2::Error::CANCEL) unless closed?
        end

        def response_written? = @response_written && !@response_reset && (!@grpc || @grpc_terminal_written)

        def send_body(...)
          super { |task| @response_task = task }
        end

        def receive_reset_stream(frame)
          @cancellation_source&.cancel if frame.unpack == ::Protocol::HTTP2::Error::CANCEL
          @response_reset = true
          super
        end

        def send_headers(headers, flags = 0)
          super
          @grpc_terminal_written = true if @grpc && flags.anybits?(::Protocol::HTTP2::END_STREAM)
          @response_written = true if flags.anybits?(::Protocol::HTTP2::END_STREAM) && !@response_reset
        end

        def finish_output(error = nil)
          # Owned output of a retiring connection must finish locally rather
          # than attempt another write or reset on its closed socket.
          return if connection.retiring?

          writable = !closed? && !@response_reset && error.nil?
          super
          @response_written = true if writable && !@response_reset
        end
      end

      class ResponseStream < ::Async::HTTP::Protocol::HTTP2::Response::Stream
        include Stream

        attr_reader :output_task

        # Output unwinding on a retiring connection must not attempt another
        # write or reset on its closed socket. A cancelled upload means the
        # call no longer needs the stream, which RFC 9113 section 8.7 signals
        # with CANCEL; upstream resets every output error with INTERNAL_ERROR.
        def finish_output(error = nil)
          return if connection.retiring?

          if error.is_a?(::Async::Cancel) && !closed?
            @output = nil
            send_reset_stream(::Protocol::HTTP2::Error::CANCEL)
            return
          end
          super
        end

        def send_body(...)
          super { |task| @output_task = task }
        end

        # Any response HEADERS, including 1xx, show the server saw the request,
        # so a later refusal no longer proves it was never processed.
        def receive_initial_headers(headers, end_stream)
          @response_headers = true
          super
        end

        def response_headers? = !!@response_headers
      end

      class ClientConnection < ::Async::HTTP::Protocol::HTTP2::Client
        include Connection

        def create_response
          ResponseStream.create(self, next_stream_id).response
        ensure
          @resource&.stream_opened
        end

        # Peer SETTINGS can change stream capacity for waiting acquisitions.
        def update_remote_settings(changes)
          super
        ensure
          @resource&.changed
        end

        def receive_goaway(frame)
          _, code, debug = frame.unpack
          @resource&.calmed! if code == ::Protocol::HTTP2::Error::ENHANCE_YOUR_CALM && debug == "too_many_pings"
          super
        ensure
          @resource&.unusable
        end
      end

      class ServerConnection < ::Async::HTTP::Protocol::HTTP2::Server
        include Connection

        # Returned by the session's dispatch instead of a response: the
        # stream is reset with REFUSED_STREAM, which a client may retry.
        REFUSAL = Object.new.freeze

        # The advertised SETTINGS_MAX_CONCURRENT_STREAMS, enforced whether or
        # not the client has acknowledged it.
        attr_accessor :stream_limit

        def accept_stream(stream_id)
          super
        ensure
          @resource&.stream_opened
        end

        # async-http's accept_stream always passes a Request::Stream factory,
        # so remote streams replace it. Local IDs keep the given block.
        def create_stream(id = next_stream_id, &)
          return super unless valid_remote_stream_id?(id)

          super { RequestStream.create(self, id) }
        end

        # A new stream above a drain's cutoff, or over the stream limit, is
        # refused before upstream would accept it or fail the connection. Its
        # header block is decoded to keep HPACK in sync, and it is never
        # dispatched. Later HEADERS for a client stream that is no longer
        # open, such as a refused stream's trailers, are decoded and dropped;
        # its DATA keeps upstream's connection flow control and is ignored.
        def receive_headers(frame)
          stream_id = frame.stream_id
          return super if streams.key?(stream_id) || !valid_remote_stream_id?(stream_id)

          if stream_id <= remote_stream_id
            decode_headers(frame.unpack)
            return
          end

          upkeep = self.upkeep
          return super unless upkeep && (upkeep.refuse?(stream_id) || (stream_limit && streams.size >= stream_limit))

          decode_headers(frame.unpack)
          @remote_stream_id = stream_id
          upkeep.refuse(stream_id)
        end

        # With server keepalive, the reader only decides; the control writer
        # sends the ACK, so the reader never blocks writing it. A PING read
        # after a local GOAWAY closed the connection is ignored rather than
        # answered with a second, PROTOCOL_ERROR GOAWAY.
        def receive_ping(frame)
          return if state == :closed

          upkeep = self.upkeep
          return super unless upkeep&.answers_pings?

          if frame.acknowledgement?
            @resource.ping_acknowledged(frame.unpack)
          else
            upkeep.ping(frame)
          end
        end

        def write_frame(frame)
          super
          upkeep&.stream_written if frame.is_a?(::Protocol::HTTP2::DataFrame) ||
                                    frame.is_a?(::Protocol::HTTP2::HeadersFrame)
        end

        # Only header blocks are written as frame groups.
        def write_frames(&)
          super
          upkeep&.stream_written
        end

        # Never raises a drain's frozen cutoff to the highest stream seen.
        def send_goaway(error_code = 0, message = "")
          frame = ::Protocol::HTTP2::GoawayFrame.new
          frame.pack(upkeep&.cutoff || remote_stream_id, error_code, message)
          write_frame(frame)
        ensure
          close!
        end

        def upkeep = @resource&.upkeep

        # No connection-wide barrier: reset and response failures are local to
        # this stream. Application work is a separately stoppable host task.
        def each(task: ::Async::Task.current)
          while (request = requests.dequeue)
            next if request.stream.closed?

            worker = ::Async::Task.new(task, finished: false) do |_worker, incoming|
              response = error = written = refused = nil
              begin
                response = yield(incoming)
                if (refused = response.equal?(REFUSAL))
                  response = nil
                  next written = refuse(incoming.stream)
                end
                incoming.stream.exchange.http_status = response&.status if incoming.stream.exchange
                unless incoming.stream.closed?
                  synchronize do
                    ::Async::Task.current.defer_cancel { incoming.send_response(response) }
                  end
                  response = nil
                  # send_body schedules output; wait outside the connection guard.
                  incoming.stream.response_task&.wait
                  written = incoming.stream.response_written?
                end
              rescue Exception => e # rubocop:disable Lint/RescueException -- cancellation also ends the exchange
                error = e
                raise unless e.is_a?(StandardError)

                reset(incoming.stream, e)
              ensure
                finish_request(response, incoming, written, error, refused: refused)
              end
            end
            @resource.track(worker)
            worker.run(request)
          end
        end

        private

        # A refusal has no HTTP response; its reset alone completes delivery.
        def finish_request(response, incoming, written, error, refused: false)
          cleaned = close_request(response, incoming) { |failure| error ||= failure }
        rescue Exception => e # rubocop:disable Lint/RescueException -- cleanup escapes also end the exchange
          error ||= e
          raise
        ensure
          incoming.stream.exchange&.finish(written && cleaned && (refused || incoming.stream.response_written?) &&
            !@resource.finished?, error)
          @resource.changed
        end

        def close_requests = requests&.close

        # A refusal is delivered once its reset is written; returns the
        # refused stream then, or nil if the stream had already closed.
        def refuse(stream)
          return if stream.closed?

          stream.send_reset_stream(::Protocol::HTTP2::Error::REFUSED_STREAM)
          stream
        end

        def close_request(response, request)
          response&.close
          request.body&.close
          true
        rescue ::Protocol::HTTP2::ProtocolError, IOError, SystemCallError => e
          yield e if block_given?
          nil
        end

        def reset(stream, error)
          code = if stream.grpc? && error.is_a?(ConnectRPC::Error) && error.code == "resource_exhausted"
                   ::Protocol::HTTP2::Error::ENHANCE_YOUR_CALM
                 else
                   ::Protocol::HTTP2::Error::INTERNAL_ERROR
                 end
          stream.send_reset_stream(code) unless stream.closed?
        rescue ::Protocol::HTTP2::ProtocolError, IOError, SystemCallError
          nil
        end
      end

      class Resource
        # Errors showing the peer already closed or reset the connection.
        PEER_LEFT = [Errno::ECONNRESET, Errno::EPIPE, Errno::ENOTCONN].freeze

        attr_accessor :raw, :socket, :connection, :task, :generation, :snapshot, :ready, :draining, :upkeep
        attr_reader :tasks, :progress, :activity_at, :keepalive
        # Called when the connection stops accepting new streams, before any
        # retirement join, so its owner can update accounting promptly.
        attr_writer :on_unusable

        def initialize(progress:, generation: nil, snapshot: nil)
          @shared_progress = progress
          @progress = ::Async::Condition.new
          @generation = generation
          @snapshot = snapshot
          @tasks = []
          @maintenance = []
          @controls = []
          @supervisors = []
          @keepalive = nil
          @upkeep = nil
          @close_reason = nil
          @activity_at = Internal.now
          @ready = false
          @draining = false
          @aborted = false
          @finishing = false
          @drain_task = nil
          @retirement_members = {}
          @cancelled_roots = {}
          @forced_roots = {}
        end

        def changed
          @activity_at = Internal.now
          @progress.signal
          @shared_progress.signal
          @keepalive&.wake
        end

        # Long-lived connection upkeep. Unlike tracked tasks it never keeps the
        # resource from being empty, but retirement still captures, cancels and
        # joins it, and its own abort is an owned abort. Graceful drains stop
        # ordinary upkeep once the cutoff is written; control writers run until
        # the half-close. A supervisor bounds its own decision and is never
        # stopped by a drain.
        def maintain(task, control: false, supervisor: false)
          tasks = if supervisor
                    @supervisors
                  else
                    (control ? @controls : @maintenance)
                  end
          tasks.reject!(&:finished?)
          tasks << task
          task
        end

        def keep_alive(parent, clock:, time:, timeout:, always:)
          return if @aborted || @keepalive

          @keepalive = KeepaliveMonitor.new(self, clock: clock, time: time, timeout: timeout, always: always)
          @keepalive.start(parent)
        end

        # Work that a dead peer would leave stuck: open streams, frame writers
        # and guard waiters, or stream output still finishing a frame.
        def busy?
          connection = @connection
          return false unless connection

          !connection.streams.empty? || !connection.writers.empty? || connection.guard_waiting? ||
            connection.outputs_pending?
        end

        def stream_opened
          @keepalive&.wake
          @upkeep&.stream_opened
        end

        def frame_read = @keepalive&.read!

        def ping_acknowledged(payload)
          @keepalive&.acknowledged(payload)
          @upkeep&.acknowledged(payload)
        end

        # The peer reported excessive PINGs (GOAWAY ENHANCE_YOUR_CALM).
        def calmed! = @calmed = true
        def calmed? = !!@calmed

        # Why the connection ended. The first recorded reason wins; abort
        # records its own before any yield.
        def record_close(reason)
          @close_reason ||= reason
          nil
        end

        def close_reason = @close_reason || default_close_reason

        def track(task)
          raise IOError, "Connection is retiring" if @aborted

          @tasks.reject!(&:finished?)
          @tasks << task
          task
        end

        def empty?
          (!@connection || @connection.streams.empty?) && @tasks.all?(&:finished?)
        end

        def finished? = @aborted
        def finishing? = @finishing
        def unusable = @on_unusable&.call(self)

        # Whether the drain, not an abort, ends the connection after the
        # reader ended with `error`. Once a drain has started, and until it
        # writes its final GOAWAY, a peer that leaves after every stream has
        # closed, by its GOAWAY, an EOF or a reset, lost nothing: the work left
        # is local, such as a stream worker's cleanup and observers. A
        # too_many_pings decision keeps its own bound instead.
        def outlives_peer?(error)
          return false unless @drain_running && !@finishing && !@aborted && @connection.streams.empty?
          return false if @upkeep&.violated?

          case error
          when nil then @connection.goaway_received?
          when EOFError, *PEER_LEFT then true
          else false
          end
        end

        # Nothing more reaches the peer, so upkeep and control writers stop,
        # and the drain ends the connection once the remaining work finishes
        # within its grace. Each task is cancelled once, so a frame write in
        # progress finishes; only raw-first retirement forces one.
        def peer_departed!
          @peer_departed = @finishing = true
          tasks = @maintenance_stopped ? @controls : [*@maintenance, *@controls]
          tasks.each { |task| task.cancel unless task.current? || task.finished? }
          changed
        end

        def abort(error = IOError.new("HTTP/2 connection closed"))
          return if @cleanup&.current?

          record_close(default_close_reason)
          capture_retirement
          owned = owns_task?(::Async::Task.current)
          launch = !@cleanup
          if launch
            @aborted = true
            # A peer that ends the connection once its streams finished, as a
            # GOAWAY asks, ends a drain cleanly, whether by EOF or a reset.
            peer_left = @connection&.reader_task&.current? && empty?
            @abort_during_drain = true if @drain_task && !@drained && !peer_left
            @cleanup = ::Async::Task.new(@lifetime, transient: true, finished: false) { retire(error) }
          end
          if owned
            start_cleanup if launch
          else
            Internal.join_complete(@cleanup, failure: true) { start_cleanup if launch }
          end
          nil
        end

        # With server upkeep, the connection first runs its two-phase GOAWAY
        # drain with `reason` as debug data, or keeps one already running, and
        # dispatches streams until the cutoff GOAWAY is written. Then it waits
        # for the accepted work, sends a final GOAWAY naming the cutoff,
        # half-closes and discards input until EOF, all within `expires`. A
        # peer that leaves once its streams closed ends the drain when the
        # remaining work finishes, without the final GOAWAY.
        def drain(parent, expires, reason: "graceful_stop")
          return Internal.wait(@drain_task, expires) if @drain_task

          if @aborted
            abort
            return true
          end

          @draining = true
          unless @ready
            abort
            return true
          end
          @drain_task = ::Async::Task.new(parent, finished: false) do
            @drain_running = true
            @upkeep&.drain(reason)&.wait
            next !@abort_during_drain if @aborted

            stop_maintenance unless @peer_departed
            @progress.wait until empty?
            next !@abort_during_drain if @aborted
            # The peer is gone, so nothing more is sent.
            next @drained = true if @peer_departed

            finish_drain { @connection.send_goaway }
          rescue EOFError, *PEER_LEFT
            # The connection was empty, so a peer that resets it after the
            # GOAWAY, as a client retiring it raw-first does, lost no work.
            @drained = true
            true
          rescue StandardError
            false
          ensure
            abort
          end
          @drain_task.run
          Internal.wait(@drain_task, expires)
        rescue ::Async::TimeoutError
          abort
          false
        end

        # Finishes a connection whose final GOAWAY is already written: stops
        # upkeep, half-closes and discards input until EOF, bounded by `within`
        # on `clock`. A later drain joins it under its own grace. Returns the
        # new drain task, or nil when an existing drain or abort already owns
        # the connection.
        def quiesce(parent, clock:, within:)
          return if @drain_task || @aborted

          @drain_task = ::Async::Task.new(parent, finished: false) do |task|
            timer = ::Async::Task.new(task, finished: false) do
              clock.sleep(within)
              abort
            end
            timer.run
            stop_maintenance
            finish_drain
          rescue EOFError, *PEER_LEFT
            @drained = true
            true
          rescue StandardError
            false
          ensure
            Internal.cancel(timer)
            abort
          end
          @drain_task.run
          @drain_task
        end

        private

        def finish_drain
          @finishing = true
          @connection.defer_close = true
          yield if block_given?
          stop_control
          @raw.shutdown(Socket::SHUT_WR)
          reader = @connection.reader_task
          cancel_root(reader)
          Internal.join_complete(reader) if reader
          # Discard unread inbound control bytes before closing TCP, so
          # the peer receives the complete response/GOAWAY rather than RST.
          loop { @raw.readpartial(16_384) }
        end

        def retirement_parent=(parent)
          @lifetime = parent
        end

        def default_close_reason = @connection&.goaway_received? ? :goaway : :closed

        # Graceful close and drain stop upkeep before waiting for emptiness. A
        # keepalive writer still waiting for the guard stops at once.
        def stop_maintenance
          @maintenance_stopped = true
          stop_tasks(@maintenance)
        end

        # Control frames queued after the final GOAWAY are dropped.
        def stop_control = stop_tasks(@controls)

        def stop_tasks(tasks)
          tasks.each { |task| Internal.cancel(task) }
          tasks.reverse_each { |task| Internal.join(task) }
        end

        def retirement_completed? = !!@retirement_completed

        # Retirement starts, closing raw TCP, before the owner notification
        # can yield; a caller cancelled in that notification cannot leave it
        # unstarted. The notification still precedes any retirement join.
        def start_cleanup
          @cleanup.run
        ensure
          unusable
        end

        def retirement_roots
          outputs = (@connection ? @connection.streams.values : []).flat_map do |stream|
            [stream.respond_to?(:response_task) ? stream.response_task : nil,
             stream.respond_to?(:output_task) ? stream.output_task : nil]
          end
          [*@connection&.writers&.keys, @connection&.reader_task, *@tasks, *@maintenance, *@controls, *@supervisors,
           @task, *outputs].compact.uniq
        end

        def capture_retirement
          retirement_roots.each { |task| capture_task(task) }
          members = @retirement_members.keys
          members.each { |task| capture_task(task) }
        end

        def capture_task(task)
          @retirement_members[task] = true
          task.children&.to_a&.each { |child| capture_task(child) unless child.transient? }
        end

        def owns_task?(task)
          capture_retirement
          while task
            return true if @retirement_members.key?(task)
            return false if task.transient?

            task = task.parent
          end
          false
        end

        def cancel_root(task)
          return if !task || @cancelled_roots.key?(task) || task.finished?

          # Record cascade coverage before cancellation can reenter or reparent.
          mark_cancelled(task)
          task.cancel
          # The first request can remain deferred by the pinned frame/response
          # guard. Deliver it now, once, rather than waiting on that guard.
          force_deferred(task)
        end

        def force_deferred(task)
          return if @forced_roots.key?(task) || task.finished? || !task.cancel_deferred?

          @forced_roots[task] = true
          task.cancel
        end

        def mark_cancelled(task)
          @cancelled_roots[task] = true
          task.children&.to_a&.each { |child| mark_cancelled(child) unless child.transient? }
        end

        def retire(error)
          # Raw TCP precedes any protocol, buffered stream or TLS disposal.
          Internal.raw_close(@raw)
          # Best-effort scheduling hint only. A return proves no unwind or
          # completion; the retained-task joins below establish retirement.
          ::Async::Task.current.yield
          @connection&.close!
          loop do
            capture_retirement
            writers = @connection&.writers&.keys || []
            roots = retirement_roots
            roots.reject! do |task|
              ancestor = task
              covered = false
              until ancestor.transient? || !(ancestor = ancestor.parent)
                covered ||= roots.include?(ancestor)
              end
              covered
            end
            roots.each { |task| cancel_root(task) }
            # An ancestor's cascade can leave the registered frame writer with
            # only a deferred request. Logical coverage is not forced delivery.
            writers.each { |writer| force_deferred(writer) }
            @retirement_members.keys.reverse_each { |task| Internal.join_complete(task) }
            capture_retirement
            break if @retirement_members.keys.all?(&:finished?)
          end
          begin
            @connection&.finish_close(error)
          rescue StandardError
            nil
          ensure
            begin
              @socket&.close
            rescue StandardError
              nil
            end
          end
          @retirement_completed = true
          changed
        end
      end
    end
  end
end
