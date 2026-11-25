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

      # The ceiling for dynamic receive windows, or nil when they stay static.
      def self.receive_window_limit(dynamic, max)
        raise ArgumentError, "dynamic_receive_windows must be true or false" unless [true, false].include?(dynamic)

        max = receive_window(max, "max_receive_window")
        max if dynamic
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
          if frame.acknowledgement?
            payload = frame.unpack
            @resource&.ping_acknowledged(payload)
            bandwidth_acknowledged(payload)
          end
          super
        end
      end

      # Receive windows of a client or server connection.
      #
      # With a `limit` above the stream window, the windows grow at run time
      # from a BDPEstimator, as grpc-go's do. Every received DATA frame counts,
      # at connection level and including padding, refused and closed streams.
      # A sample sends a stream-0 WINDOW_UPDATE for pending connection credit,
      # then a PING. Growth sends a stream-0 WINDOW_UPDATE when the estimate
      # exceeds the connection window, then SETTINGS_INITIAL_WINDOW_SIZE.
      # Windows never shrink. Effects the peer can observe happen inside the
      # frame guard just before each frame is encoded: the PING's send time is
      # stamped, and a SETTINGS frame's window is appended to the pending local
      # settings and raised on every live stream, so append order is wire
      # order. The reader reserves connection credit when it queues the
      # frames, and never writes them: client and server connections hand
      # them to a writer task through `send_bandwidth_frames`, and the
      # credit returned at a quarter of the window through `queue_credit`.
      module ReceiveWindows # rubocop:disable Metrics/ModuleLength
        SETTINGS_WINDOW = ::Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE

        # Set before the preface. Streams use the stream window before the
        # peer acknowledges SETTINGS: accepting more than the peer may yet
        # send is safe, and a peer may spend the larger window before its ACK.
        def receive_windows(stream:, connection:, limit: nil)
          @stream_receive_window = stream
          @connection_receive_window = connection
          return unless limit && limit > stream

          @bandwidth = BDPEstimator.new(window: stream, limit: limit)
          @bandwidth_settings = {}.compare_by_identity
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
        # window when the peer acknowledges SETTINGS. It also sets every
        # stream's window to the acknowledged one, which is older and smaller
        # than a grown window published while that ACK was in flight.
        def update_local_settings(changes)
          super
          local_window.desired = @connection_receive_window if @connection_receive_window
          raise_stream_windows(@stream_receive_window) if @bandwidth
        end

        # Returns connection credit once a quarter of the connection window
        # has arrived, as grpc-go does (`trInFlow.onData`). protocol-http2
        # waits for half, which leaves concurrent streams, or a stream window
        # as large as the connection window, short of connection credit.
        #
        # The reader calls this for every DATA frame, so, as grpc-go queues
        # it on its writer, it reserves the credit and hands it to the
        # connection's writer task without yielding: a frame write that holds
        # the guard never stops it applying DATA. Without a writer that can
        # take it, such as on a raw connection or a retiring or finishing one,
        # the reader writes it itself through protocol-http2.
        def request_window_update
          wanted = local_window.wanted
          return unless wanted.positive? && wanted >= receive_window_target / 4

          if receive_window_writer?
            queue_credit(reserve_credit(wanted))
          else
            send_window_update(wanted)
          end
        end

        # Counts a received DATA frame of `bytes`, padding included.
        def sample_bandwidth(bytes)
          bandwidth = @bandwidth or return
          send_bandwidth_frames(true, nil) if bandwidth.starts_sample?(bytes)
        end

        # A PING ACK `payload`, which may end the outstanding sample. Growth
        # is sent at once.
        def bandwidth_acknowledged(payload)
          window = @bandwidth&.acknowledged(payload) or return
          send_bandwidth_frames(false, window)
        end

        private

        def receive_window_target = local_window.desired || local_window.capacity

        # The frames for growth to `window` and for a probe, each in grpc-go's
        # order: connection credit, then the SETTINGS or the PING.
        def bandwidth_frames(probe, window)
          frames = []
          frames.push(credit_frame(growth_credit(window)), settings_frame(window)) if window
          frames.push(credit_frame(probe_credit), ping_frame) if probe
          frames.compact
        end

        # An estimate past the connection window raises its target, and
        # reserves the increment that reaches it.
        def growth_credit(window)
          return 0 unless window > @connection_receive_window

          @connection_receive_window = window
          local_window.desired = window
          reserve_credit(window - local_window.available)
        end

        # Pending connection credit precedes a BDP PING, as in grpc-go, so an
        # intermediary that polices PINGs sees a WINDOW_UPDATE first.
        def probe_credit = reserve_credit(local_window.wanted)

        # Reserved by the reader, before its frame is written: accepting more
        # before the peer reads it is safe, and the reader's own connection
        # WINDOW_UPDATEs then never count the same credit again.
        def reserve_credit(amount)
          return 0 unless amount.positive?

          local_window.expand(amount)
          amount
        end

        def credit_frame(amount)
          return unless amount.positive?

          frame = ::Protocol::HTTP2::WindowUpdateFrame.new(0)
          frame.pack(amount)
          frame
        end

        # Adds reserved `amount` to the one pending stream-0 WINDOW_UPDATE.
        # Returns a new pending frame for the caller to queue, or nil when the
        # amount joined a frame still queued. The frame's increment is packed
        # when it is encoded, so credit added up to then goes with it.
        def pending_credit_frame(amount)
          return unless amount.positive?

          @pending_credit = (@pending_credit || 0) + amount
          return if @pending_credit_frame

          @pending_credit_frame = ::Protocol::HTTP2::WindowUpdateFrame.new(0)
        end

        # Called once the pending frame is encoded, or when a client writer
        # that stopped drops it. A host's stopped control writer drops its
        # queued frame without clearing it, which is harmless: a writer stops
        # only as its connection ends, so the dropped credit is not needed,
        # and later credit goes inline.
        def clear_pending_credit
          @pending_credit_frame = nil
          @pending_credit = 0
        end

        def settings_frame(window)
          frame = ::Protocol::HTTP2::SettingsFrame.new
          frame.pack([[SETTINGS_WINDOW, window]])
          @bandwidth_settings[frame] = window
          frame
        end

        def ping_frame
          frame = @bandwidth_ping = ::Protocol::HTTP2::PingFrame.new
          frame.pack(BDPEstimator::PING)
          frame
        end

        # Runs inside the frame guard, just before `frame` is encoded. The
        # pending WINDOW_UPDATE takes all credit reserved for it so far, and
        # stops being pending, with no yield before it is encoded.
        def publish_receive_window(frame)
          if frame.equal?(@pending_credit_frame)
            frame.pack(@pending_credit)
            return clear_pending_credit
          end
          return unless @bandwidth

          if frame.equal?(@bandwidth_ping)
            @bandwidth_ping = nil
            @bandwidth.sent
          elsif !@bandwidth_settings.empty? && (window = @bandwidth_settings.delete(frame))
            local_settings.append([[SETTINGS_WINDOW, window]])
            @stream_receive_window = window if window > @stream_receive_window
            raise_stream_windows(window)
          end
        end

        # Never lowers a window, and never raises one past the RFC maximum.
        def raise_stream_windows(window)
          streams.each_value do |stream|
            local = stream.local_window
            next unless window > local.capacity

            headroom = ::Protocol::HTTP2::MAXIMUM_ALLOWED_WINDOW_SIZE - local.available
            local.capacity = [window, local.capacity + headroom].min
          end
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
          synchronize do
            ::Async::Task.current.defer_cancel do
              timed_write do
                encoding(frame)
                super
              end
            end
          end
        end

        def write_frames(&)
          synchronize do
            ::Async::Task.current.defer_cancel do
              timed_write do
                encoding_frames
                super(&)
              end
            end
          end
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
          sample_bandwidth(frame.length)
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

        # Retirement fails raw TCP, and abort marks the resource finished,
        # before close! marks this connection closed. Session retirement fails
        # every raw socket before it aborts each resource.
        def retiring? = closed? || !!@resource&.finished? || !!@resource&.send(:raw_failed?)

        private

        # Only server connections have a request queue.
        def close_requests = nil

        # Run inside the frame guard, just before a frame or a header block
        # is encoded.
        def encoding(frame) = publish_receive_window(frame)
        def encoding_frames = nil

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
        # does not restart it. On expiry the stream is reset with CANCEL and
        # the write raises ResponseBodyTimeout.
        def bound_progress(clock, seconds)
          @progress_clock = clock
          @progress_seconds = seconds
        end

        # async-http 0.105.0's loop, with its credit wait bounded. One timer
        # spans a wait until this output sends a frame. The frame size is
        # read after the wait with no yield before the frame consumes it;
        # only then is the timer cancelled, which yields. Otherwise sibling
        # streams woken by the same connection WINDOW_UPDATE could spend the
        # shared credit first, and the stale send would raise
        # FlowControlError, resetting the stream with INTERNAL_ERROR.
        def write(chunk)
          timer = nil
          until chunk.empty?
            stream = @stream or raise IOError, "HTTP/2 stream is closed!"
            maximum_size = stream.available_frame_size
            unless maximum_size.positive?
              timer ||= credit_timer if @progress_seconds
              next await_credit(stream)
            end

            chunk = send_data(stream, chunk, maximum_size)
            if timer
              Internal.cancel(timer)
              timer = nil
            end
            break unless chunk
          end
        ensure
          Internal.cancel(timer)
        end

        private

        # Returns once the stream has credit to send, with no yield after
        # reading it. Raises ResponseBodyTimeout once the wait's timer fires.
        def await_credit(stream)
          credited = @guard.synchronize do
            size = stream.available_frame_size
            until size.positive? || @stalled
              @window_updated.wait(@guard)
              stream = @stream or raise IOError, "HTTP/2 stream is closed!"
              size = stream.available_frame_size
            end
            size.positive?
          end
          return if credited

          stream.output_stalled
          @stream = nil
          raise ResponseBodyTimeout, "HTTP/2 response body received no flow-control credit within response_body_timeout"
        end

        # Lives only inside one credit wait, which write ends. It only wakes
        # the waiting output, which resets its own stream, so it never stops
        # a task.
        def credit_timer
          @stalled = false
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

      # A client request body. A response that ends while the body is open
      # stops it at the next frame boundary: DATA admission holds the frame
      # guard while it rechecks the response's end, the stream and its credit,
      # then debits and writes the frame, so no DATA follows the end and an
      # abandoned frame debits no credit. Credit waits happen outside the guard
      # and end with the response too. The output then reads the producer's end
      # and its stream resets the stream (ResponseStream#finish_output).
      class ClientOutput < Output
        def write(chunk)
          until chunk.empty?
            stream = @stream or raise IOError, "HTTP/2 stream is closed!"
            return if stream.response_ended?
            next await_credit(stream) unless stream.available_frame_size.positive?

            chunk = admit(stream, chunk) or break
          end
        end

        # Whether the producer closed its side before the response ended.
        def committed? = !@body || (@body.respond_to?(:write_closed?) && @body.write_closed?)

        # Called by the stream's reader once the response ended, without
        # yielding: retires the producer and wakes a credit wait.
        def response_ended
          @body.retire if @body.respond_to?(:retire)
          @guard.synchronize { @window_updated.broadcast }
        end

        private

        # Returns the unsent remainder, or nil once the chunk was sent or the
        # response ended.
        def admit(stream, chunk)
          stream.connection.synchronize do
            next if stream.response_ended?
            raise IOError, "HTTP/2 stream is closed!" if stream.closed?

            size = stream.available_frame_size
            next chunk unless size.positive?

            send_data(stream, chunk, size)
          end
        end

        def await_credit(stream)
          @guard.synchronize do
            until stream.available_frame_size.positive? || stream.response_ended?
              @window_updated.wait(@guard)
              stream = @stream or raise IOError, "HTTP/2 stream is closed!"
            end
          end
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
          # Receipt alone owes no stream credit, which counts only consumed or
          # discarded bytes. Reclaiming nothing would still send credit an
          # application read owes, and wait behind that read for the guard.
          reclaim_input(discarded) if discarded.positive?
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
          return unless credit_due?

          connection.synchronize do
            next if closed? || state == :half_closed_remote || connection.retiring?
            next unless credit_due?

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

          @output = output_class.new(self, body, trailer)
          @output.bound_progress(*@output_bound) if @output_bound
          @output.start do |task|
            connection.output_started(task)
            yield task if block_given?
          end
        end

        private

        def output_class = Output

        # Stream credit returns once the application has consumed or
        # discarded a quarter of the window, as grpc-go does (`inFlow.onRead`),
        # so a stream whose reader keeps up has at least three quarters of its
        # window in flight. protocol-http2 waits until half the window is
        # unavailable, which keeps a lone stream's BDP samples near the
        # two-thirds growth threshold. Credit cannot stall: a peer out of
        # credit has a full window of bytes here, and every byte the
        # application reads or discards counts.
        def credit_due?
          reclaimed = @reclaimed_input or return false
          reclaimed.positive? && reclaimed >= local_window.capacity / 4
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

        # Upstream enqueues a valid request for dispatch at the end of this
        # call, right after it assigns the request's headers. A rejected
        # header block leaves them unset, or raises.
        def receive_initial_headers(headers, end_stream)
          result = super
          @request_enqueued = true if @request&.headers
          result
        end

        def enqueued? = !!@request_enqueued

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

      # A response that ends with END_STREAM while the request body is open
      # ends the request too, as in grpc-go's client: the reader latches the
      # end, retires the producer and wakes the output, which stops at a frame
      # boundary and writes RST_STREAM(NO_ERROR). A producer that closed its
      # side first commits a clean END_STREAM instead.
      class ResponseStream < ::Async::HTTP::Protocol::HTTP2::Response::Stream
        include Stream

        attr_reader :output_task

        def response_ended? = !!@response_ended

        # Output unwinding on a retiring connection must not attempt another
        # write or reset on its closed socket. A cancelled upload means the
        # call no longer needs the stream, which RFC 9113 section 8.7 signals
        # with CANCEL; upstream resets every output error with INTERNAL_ERROR.
        # Once the response ended, the reset is NO_ERROR however the output
        # ended, cancellation by cleanup included.
        def finish_output(error = nil)
          return if connection.retiring?

          if @response_ended
            @output = nil
            reset_ended
            return
          end
          if error.is_a?(::Async::Cancel) && !closed?
            @output = nil
            send_reset_stream(::Protocol::HTTP2::Error::CANCEL)
            return
          end
          super
        end

        # A body attached after the response ended starts retired, so its
        # output ends at once with the reset.
        def send_body(body, trailer = nil, &)
          body.retire if @response_ended && body.respond_to?(:retire)
          super { |task| @output_task = task }
        end

        # The one RST_STREAM(NO_ERROR) after the response ended, written by
        # the output or by cleanup, whichever holds the frame guard first. It
        # is skipped once the stream closed, as by a peer reset. Cancellation
        # is deferred across the frame, its flush and the close. A failed
        # write closes the stream locally, keeping the received response, and
        # fails the connection.
        def reset_ended
          connection.synchronize do
            ::Async::Task.current.defer_cancel do
              next if closed? || connection.retiring?

              frame = ::Protocol::HTTP2::ResetStreamFrame.new(id)
              frame.pack(::Protocol::HTTP2::Error::NO_ERROR)
              write_frame(frame)
              close! unless closed?
            end
          end
        rescue UnusableConnection
          nil
        rescue StandardError => e
          close! unless closed?
          resource = connection.resource
          resource&.abort(e) unless resource&.peer_departed? || resource&.outlives_peer?(e)
        end

        # async-http resets a stream whose HEADERS fail validation, and
        # protocol-http2 then records the frame's END_STREAM on the closed
        # stream anyway. Restoring the closed state keeps the end unlatched
        # and stops cleanup from resetting the stream a second time.
        def receive_headers(frame)
          request_open = state == :open
          super
          return @state = :closed unless connection.streams[id].equal?(self)

          response_ended if request_open && state == :half_closed_remote
        end

        def receive_data(frame)
          request_open = state == :open
          super
          response_ended if request_open && state == :half_closed_remote
        end

        # protocol-http2 opens a stream once its request HEADERS are flushed.
        # A response that ended during that flush already moved the stream to
        # half_closed_remote, and the body attached next starts retired.
        def open!
          return super unless state == :half_closed_remote

          @response_ended = true
          self
        end

        # A response that ended was fully received, so a close with an error,
        # as by connection retirement, keeps its unread bytes for the caller.
        def closed(error)
          if @response_ended && (input = @input)
            @input = nil
            input.retire
          end
          super
        end

        # Any response HEADERS, including 1xx, show the server saw the request,
        # so a later refusal no longer proves it was never processed.
        def receive_initial_headers(headers, end_stream)
          @response_headers = true
          super
        end

        def response_headers? = !!@response_headers

        private

        def output_class = ClientOutput

        # Runs in the reader without yielding.
        def response_ended
          output = @output
          return if !output || output.committed?

          @response_ended = true
          output.response_ended
        end
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

        # Starts the task that writes connection credit and receive-window
        # growth, before the reader starts, whether or not windows grow. The
        # reader only records what to send, so a frame write that waits for
        # the guard or the socket never stops it reading. The task is a
        # control task of the resource: retirement captures, cancels and
        # joins it, and a drain stops it once the final GOAWAY is written.
        def start_receive_window_writer(parent)
          return if @resource.finished?

          @receive_window_wake = ::Async::Condition.new
          task = ::Async::Task.new(parent, finished: false) { write_receive_windows }
          @resource.maintain(task, control: true)
          task.run
          task
        end

        private

        # Whether the writer task runs and can still reach the peer.
        def receive_window_writer?
          !!@receive_window_writing && !retiring? && !@resource.finishing? && !@resource.peer_departed?
        end

        # Credit joins the pending WINDOW_UPDATE, which the writer's next
        # batch writes.
        def queue_credit(amount)
          pending_credit_frame(amount)
          @receive_window_wake.signal
        end

        # Recorded without yielding, with connection credit reserved here as
        # on a server. The estimator keeps at most one probe outstanding and
        # estimates only grow, so the writer holds at most one probe and the
        # latest growth, and their credit joins the pending WINDOW_UPDATE. A
        # connection without a writer that can take them abandons its probe
        # and drops its growth.
        def send_bandwidth_frames(probe, window)
          unless receive_window_writer?
            @bandwidth.abandon if probe
            return
          end

          if window
            pending_credit_frame(growth_credit(window))
            @bandwidth_growth = window
          end
          if probe
            pending_credit_frame(probe_credit)
            @bandwidth_probe = true
          end
          @receive_window_wake.signal
        end

        # One batch per wake: the pending WINDOW_UPDATE, then the SETTINGS,
        # then the PING. The WINDOW_UPDATE takes the credit reserved until it
        # is encoded; credit reserved after that gets a new one, which wakes
        # the next batch. Each frame write defers cancellation, and raw-first
        # retirement ends a stuck one. A writer that stops drops what it
        # holds, including a probe whose PING was never written. As in the
        # host's control writer, a write that finds the peer gone during a
        # drain, before the reader does, stops only this writer: the reader,
        # which meets the same end after reading what the peer sent, decides
        # whether the drain outlives the peer.
        def write_receive_windows
          @receive_window_writing = true
          loop do
            @receive_window_wake.wait until @pending_credit_frame || @bandwidth_probe || @bandwidth_growth
            frames = [@pending_credit_frame, @bandwidth_growth && settings_frame(@bandwidth_growth),
                      @bandwidth_probe && ping_frame].compact
            @bandwidth_probe = @bandwidth_growth = nil
            synchronize { frames.each { |frame| write_frame(frame) } }
          end
        rescue UnusableConnection
          nil
        rescue StandardError => e
          @resource.abort(e) unless @resource.peer_departed? || @resource.outlives_peer?(e)
        ensure
          @receive_window_writing = false
          clear_pending_credit
          @bandwidth_probe = @bandwidth_growth = nil
          @bandwidth&.abandon
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

        # The session that reserves host admission for each new stream while
        # its HEADERS are read. Nil skips HEADERS-time admission.
        attr_accessor :admission

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
        # An accepted stream that host admission cannot take is refused the
        # same way once upstream has enqueued its request. Such a stream
        # leaves `streams` at once, so it counts toward the stream limit
        # until its reset is written, as a client that honours the limit
        # counts it until the reset arrives.
        def receive_headers(frame)
          stream_id = frame.stream_id
          return super if streams.key?(stream_id) || !valid_remote_stream_id?(stream_id)

          if stream_id <= remote_stream_id
            decode_headers(frame.unpack)
            return
          end

          upkeep = self.upkeep
          unless upkeep && (upkeep.refuse?(stream_id) || (stream_limit && streams_toward_limit >= stream_limit))
            super
            return admit(streams[stream_id], upkeep)
          end

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
            payload = frame.unpack
            @resource.ping_acknowledged(payload)
            bandwidth_acknowledged(payload)
          else
            upkeep.ping(frame)
          end
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

        # A HEADERS or DATA write forgives the next client PING. As in
        # grpc-go's loopy writer, that happens before the frame is encoded,
        # inside the frame guard, so a PING the peer sends once it reads the
        # frame, such as a BDP probe, is never read before the forgiveness.
        def encoding(frame)
          upkeep&.stream_written if frame.is_a?(::Protocol::HTTP2::DataFrame) ||
                                    frame.is_a?(::Protocol::HTTP2::HeadersFrame)
          super
        end

        # Only header blocks are written as frame groups.
        def encoding_frames
          upkeep&.stream_written
          super
        end

        # Whether the control writer can take receive-window frames. A
        # retiring connection, or one whose drain is finishing or whose peer
        # left, sends none: its control writer may already have stopped.
        def receive_window_writer?
          upkeep = self.upkeep
          !!upkeep && !retiring? && !@resource.finishing? && !@resource.peer_departed? && !upkeep.writer_stopped?
        end

        # Credit joins the WINDOW_UPDATE still queued on the control writer,
        # or queues a new one, without yielding.
        def queue_credit(amount)
          frame = pending_credit_frame(amount) or return
          upkeep.send_receive_window([frame])
        end

        # Queued on the control writer without yielding. Without a control
        # writer that can take them, the probe is abandoned and the growth
        # dropped.
        def send_bandwidth_frames(probe, window)
          unless receive_window_writer?
            @bandwidth.abandon if probe
            return
          end

          upkeep.send_receive_window(bandwidth_frames(probe, window))
        end

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

        # Reserves a host admission slot for a newly enqueued request, or
        # closes its stream at once with REFUSED_STREAM, before the reader
        # takes any of its DATA. The control writer sends the reset; the
        # session observes the refusal. The request is captured first because
        # upstream detaches it from the closed stream. A retiring connection's
        # control writer may already have stopped, so its streams are left to
        # dispatch, which refuses them.
        def admit(stream, upkeep)
          admission = self.admission
          return if !admission || !upkeep || retiring?
          return unless stream.is_a?(RequestStream) && stream.enqueued? && !stream.closed?
          return if admission.send(:reserve_admission, stream)

          request = stream.request
          written = upkeep.reset(stream, ::Protocol::HTTP2::Error::REFUSED_STREAM)
          (@refusals ||= []) << written
          admission.send(:refuse_admission, request, written)
        end

        # Open streams plus streams refused at HEADERS whose reset is unwritten.
        def streams_toward_limit
          @refusals&.reject!(&:resolved?)
          streams.size + (@refusals&.size || 0)
        end

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

        # Whether the peer left a drain once its streams closed, so the drain
        # alone ends the connection.
        def peer_departed? = !!@peer_departed
        def unusable = @on_unusable&.call(self)

        # Whether the drain, not an abort, ends the connection after the
        # reader ended with `error`. Once a drain has started, and until it
        # writes its final GOAWAY, a peer that leaves after every stream has
        # closed, by its GOAWAY, an EOF or a reset, lost nothing: the work left
        # is local, such as a stream worker's cleanup and observers. A
        # too_many_pings decision keeps its own bound instead. The EOF or
        # reset after our own raw failure is never the peer leaving. A control
        # frame or keepalive PING write that fails this way leaves the decision
        # to the reader, which may still read frames the peer sent before it
        # left. On TLS such a write raises the socket's own errno; OpenSSL
        # raises SSLError from a write only after a read has already failed,
        # which the reader decides before anything else runs.
        def outlives_peer?(error)
          return false if @raw_failed
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

        # An interruptible caller raises an interruption at once and leaves
        # retirement to the cleanup task.
        def abort(error = IOError.new("HTTP/2 connection closed"), interruptible: false)
          return if @cleanup&.current?

          record_close(default_close_reason)
          capture_retirement
          owned = owns_task?(::Async::Task.current)
          launch = !@cleanup
          if launch
            @aborted = true
            # A peer that ends the connection once its streams finished, as a
            # GOAWAY asks, ends a drain cleanly, whether by EOF or a reset. The
            # EOF after our own raw failure is not the peer's.
            peer_left = !@raw_failed && @connection&.reader_task&.current? && empty?
            @abort_during_drain = true if @drain_task && !@drained && !peer_left
            # The cleanup task reports its own failure; no joiner raises it.
            @cleanup = ::Async::Task.new(@lifetime, transient: true, finished: false) do
              Internal::Teardown.run { retire(error) }
            end
          end
          if owned
            start_cleanup if launch
          else
            Internal.join_complete(@cleanup, interruptible: interruptible) { start_cleanup if launch }
          end
          nil
        end

        # With server upkeep, the connection first runs its two-phase GOAWAY
        # drain with `reason` as debug data, or keeps one already running, and
        # dispatches streams until the cutoff GOAWAY is written. Then it waits
        # for the accepted work, sends a final GOAWAY naming the cutoff,
        # half-closes and discards input until EOF, all within `expires`. A
        # peer that leaves once its streams closed ends the drain when the
        # remaining work finishes, without the final GOAWAY. A caller gets
        # true when the streams finished by its own `expires`, however the
        # peer leaves afterwards or the connection then retires.
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

            streams_finished
            # The peer is gone, so nothing more is sent.
            next @drained = true if @peer_departed

            finish_drain { @connection.send_goaway }
          rescue EOFError, *PEER_LEFT
            next true if @streams_finished_at
            # A local raw failure also surfaces as EOF or EPIPE.
            next false if @raw_failed

            # The connection was empty, so a peer that resets it after the
            # GOAWAY, as a client retiring it raw-first does, lost no work.
            @drained = true
            true
          rescue StandardError
            !@streams_finished_at.nil?
          ensure
            abort
          end
          @drain_task.run
          Internal.wait(@drain_task, expires)
        rescue ::Async::TimeoutError
          # Decided before the abort, whose forced teardown empties the
          # connection.
          finished = streams_finished_by?(expires)
          abort
          finished
        end

        # Finishes a connection whose final GOAWAY is already written: stops
        # upkeep, half-closes and discards input until EOF, bounded by `within`
        # on `clock`. A later drain joins it under its own grace. Returns the
        # new drain task, or nil when an existing drain or abort already owns
        # the connection.
        def quiesce(parent, clock:, within:)
          return if @drain_task || @aborted

          # A too_many_pings decision can quiesce while streams still run.
          streams_finished if empty? && !@upkeep&.violated?
          @drain_task = ::Async::Task.new(parent, finished: false) do |task|
            timer = ::Async::Task.new(task, finished: false) do
              clock.sleep(within)
              abort
            end
            timer.run
            stop_maintenance
            finish_drain
          rescue EOFError, *PEER_LEFT
            next true if @streams_finished_at
            next false if @raw_failed

            @drained = true
            true
          rescue StandardError
            !@streams_finished_at.nil?
          ensure
            Internal.cancel(timer)
            abort
          end
          @drain_task.run
          @drain_task
        end

        private

        # Records when a drain's streams finished, unless forced teardown
        # emptied the connection.
        def streams_finished
          @streams_finished_at = Internal.now unless @aborted || raw_failed?
        end

        def streams_finished_by?(expires) = !!@streams_finished_at && @streams_finished_at <= expires

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
          # Retirement closes raw only after this read has left it.
          @discarding = true
          begin
            loop { @raw.readpartial(16_384) }
          ensure
            @discarding = false
            changed
          end
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

        # Fails the peer at once, without closing the descriptor. On Ruby 4,
        # IO#close interrupts every fiber blocked on the socket through the
        # scheduler, and Async delivers that IOError by raising into the fiber
        # wherever it is suspended next, even after it has already left the
        # read. A shutdown wakes those fibers with EOF or EPIPE instead.
        # Retirement closes the descriptor once they have all left it.
        def fail_raw
          @raw_failed = true
          @raw.shutdown(Socket::SHUT_RDWR) if @raw && !@raw.closed?
        rescue IOError, SystemCallError
          nil
        end

        def raw_failed? = !!@raw_failed || !!@raw&.closed?

        # Retirement starts, failing raw TCP, before the owner notification
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
          # Failing raw TCP precedes any protocol, buffered stream or TLS
          # disposal.
          fail_raw
          # An interruption of this cleanup task is retained at every step
          # below and skips no later one, so retirement still completes and
          # publishes before it is raised.
          # Best-effort scheduling hint only. A return proves no unwind or
          # completion; the retained-task joins below establish retirement.
          escapes = [Internal::Teardown.capture { ::Async::Task.current.yield }]
          # A failed or interrupted protocol close is reported or retained and
          # skips neither the member joins nor the disposal steps below.
          escapes << Internal::Teardown.capture { @connection&.close! }
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
            roots.each do |task|
              next unless (interruption = Internal::Teardown.capture { cancel_root(task) })

              # Async delivers a cancel by transferring to the task, so this
              # interruption surfaced after delivery and needs no new cancel.
              # It can still skip forcing a deferred request.
              escapes.push(interruption, Internal::Teardown.capture { force_deferred(task) })
            end
            # An ancestor's cascade can leave the registered frame writer with
            # only a deferred request. Logical coverage is not forced delivery.
            writers.each { |writer| escapes << Internal::Teardown.capture { force_deferred(writer) } }
            # A join retains an interruption until its member has settled. A
            # join's own failure, such as joining the current task, still
            # escapes: retrying that member would never end.
            @retirement_members.keys.reverse_each do |task|
              Internal.join_complete(task)
            rescue StandardError
              raise
            rescue Exception => e # rubocop:disable Lint/RescueException -- the member has settled; retirement continues
              escapes << e
            end
            capture_retirement
            # A settled member can stay unfinished, with no signal, until
            # Async clears its fiber; finished? here would spin without yielding.
            break if @retirement_members.keys.all? { |task| CompletionJoin.settled?(task) }
          end
          # A drain's discard read is not a member, but the shutdown ends it
          # with EOF. The protocol stream and TLS socket below close raw too,
          # so every close waits for that read to leave raw.
          escapes << Internal::Teardown.capture { @progress.wait } while @discarding
          # A failed or interrupted disposal step, the final raw close
          # included, is reported or retained and skips neither a later one
          # nor retirement's completion.
          escapes << Internal::Teardown.capture { @connection&.finish_close(error) }
          escapes << Internal::Teardown.capture { @socket&.close }
          escapes << Internal::Teardown.capture { @raw.close if @raw && !@raw.closed? }
          @retirement_completed = true
          changed
          Internal::Teardown.finish(escapes)
        ensure
          # A retirement that ends early, such as on a failed join, still
          # releases the descriptor, although a fiber still blocked on raw may
          # then get a late close interrupt.
          Internal.raw_close(@raw)
        end
      end
    end
  end
end
