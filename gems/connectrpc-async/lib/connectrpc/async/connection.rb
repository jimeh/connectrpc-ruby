# frozen_string_literal: true

require "English"

module ConnectRPC
  module Async
    module Internal
      def self.settings(max_requests)
        {
          ::Protocol::HTTP2::Settings::ENABLE_PUSH => 0,
          ::Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS => max_requests,
          ::Protocol::HTTP2::Settings::MAXIMUM_FRAME_SIZE => 16_384,
          ::Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE => 65_536
        }
      end

      # Version-coupled wrappers around the selected stack, not a frame codec.
      module Connection
        attr_accessor :resource, :defer_close
        attr_reader :writers, :reader_task

        def initialize(...)
          super
          @writers = {}
          @writer = nil
        end

        def synchronize
          task = ::Async::Task.current
          return yield if @writer.equal?(task)

          raise IOError, "HTTP/2 connection closed" if @resource&.finished?

          @writers[task] = true
          begin
            @write_frame_guard.acquire do
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
          synchronize { ::Async::Task.current.defer_cancel { super } }
        end

        def write_frames(&)
          synchronize { ::Async::Task.current.defer_cancel { super(&) } }
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
          if frame.length > @local_window.available
            raise ::Protocol::HTTP2::FlowControlError, "DATA exceeds connection receive window"
          end

          super
        end

        def delete(id)
          super
        ensure
          @resource&.changed
        end

        def close(error = nil)
          @requests&.close
          if @defer_close
            @resource&.changed
          elsif @resource
            @resource.abort(error)
          else
            super
          end
        end

        def finish_close(error = nil)
          @requests&.close
          # Skip our override while retaining upstream queue/reader/framer cleanup.
          super_method = method(:close).super_method
          super_method.call(error)
        end
      end

      class GRPCHeaderError < ::Protocol::HTTP2::HeaderError
        def initialize(message, code)
          super(message)
          @code = code
        end
      end

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
      end

      module Stream # rubocop:disable Metrics/ModuleLength
        attr_reader :grpc_initial_headers, :grpc_initial_end_stream, :grpc_trailers, :grpc_error, :grpc_reset,
                    :declared_length

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
          @headers.trailer!
          super
        end

        def prepare_input(length)
          raise ArgumentError, "Input body already prepared!" if @input

          @reclaimed_input = 0
          @input = Ingress.new(self, length)
        end

        def receive_data(frame)
          if %i[open half_closed_local].include?(@state) && frame.length > @local_window.available
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
          unless @state == :idle || frame.unpack == ::Protocol::HTTP2::Error::NO_ERROR
            @input&.retire(nil, discard: true)
          end
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

        def reclaim_input(amount)
          return if closed? || @state == :half_closed_remote || @connection.closed?

          @reclaimed_input = (@reclaimed_input || 0) + amount
          request_window_update
        end

        def request_window_update
          return unless @reclaimed_input&.positive? && @local_window.limited?

          @connection.synchronize do
            next if closed? || @state == :half_closed_remote || @connection.closed?
            next unless @reclaimed_input.positive? && @local_window.limited?

            ::Async::Task.current.defer_cancel do
              amount = @reclaimed_input
              @reclaimed_input = 0
              # Reserve credit before flush can yield to the reader. A peer
              # may spend the update as soon as it reaches the socket.
              @local_window.expand(amount)
              frame = ::Protocol::HTTP2::WindowUpdateFrame.new(@id)
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
          @output.start(&)
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
          # Raw abort has already retired the connection. Its owned tasks must
          # finish locally rather than attempting another reset on that socket.
          return if @connection.closed?

          writable = !closed? && !@response_reset && error.nil?
          super
          @response_written = true if writable && !@response_reset
        end
      end

      class ResponseStream < ::Async::HTTP::Protocol::HTTP2::Response::Stream
        include Stream

        attr_reader :output_task

        # A raw-first destructive abort has already retired this connection.
        # Output unwinding must not attempt another reset on its closed socket.
        def finish_output(error = nil)
          return if @connection.closed?

          super
        end

        def send_body(...)
          super { |task| @output_task = task }
        end
      end

      class ClientConnection < ::Async::HTTP::Protocol::HTTP2::Client
        include Connection

        def create_response
          ResponseStream.create(self, next_stream_id).response
        end
      end

      class ServerConnection < ::Async::HTTP::Protocol::HTTP2::Server
        include Connection

        def accept_stream(stream_id)
          ::Protocol::HTTP2::Connection.instance_method(:accept_stream).bind_call(self, stream_id) do
            RequestStream.create(self, stream_id)
          end
        end

        # No connection-wide barrier: reset and response failures are local to
        # this stream. Application work is a separately stoppable host task.
        def each(task: ::Async::Task.current)
          while (request = @requests.dequeue)
            next if request.stream.closed?

            worker = ::Async::Task.new(task, finished: false) do |_worker, incoming|
              response = error = written = nil
              begin
                response = yield(incoming)
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
              rescue StandardError => e
                error = e
                reset(incoming.stream, e)
              ensure
                error ||= $ERROR_INFO
                begin
                  cleaned = close_request(response, incoming) { |failure| error ||= failure }
                ensure
                  incoming.stream.exchange&.finish(written && cleaned && incoming.stream.response_written? &&
                    !@resource.finished?, error || $ERROR_INFO)
                  @resource.changed
                end
              end
            end
            @resource.track(worker)
            worker.run(request)
          end
        end

        private

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
        attr_accessor :raw, :socket, :connection, :task, :generation, :snapshot, :ready, :draining
        attr_reader :tasks, :progress, :activity_at

        def initialize(progress:, generation: nil, snapshot: nil)
          @shared_progress = progress
          @progress = ::Async::Condition.new
          @generation = generation
          @snapshot = snapshot
          @tasks = []
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
        end

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

        def abort(error = IOError.new("HTTP/2 connection closed"))
          return if @cleanup&.current?

          capture_retirement
          owned = owns_task?(::Async::Task.current)
          launch = !@cleanup
          if launch
            @aborted = true
            @abort_during_drain = true if @drain_task && !@drained
            @cleanup = ::Async::Task.new(@lifetime, transient: true, finished: false) { retire(error) }
          end
          if owned
            @cleanup.run if launch
          else
            Internal.join_complete(@cleanup, failure: true) { @cleanup.run if launch }
          end
          nil
        end

        def drain(parent, expires)
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
            @progress.wait until empty?
            next !@abort_during_drain if @aborted

            @finishing = true
            @connection.defer_close = true
            @connection.send_goaway
            @raw.shutdown(Socket::SHUT_WR)
            reader = @connection.reader_task
            cancel_root(reader)
            Internal.join_complete(reader) if reader
            # Discard unread inbound control bytes before closing TCP, so
            # the peer receives the complete response/GOAWAY rather than RST.
            loop { @raw.readpartial(16_384) }
          rescue EOFError
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

        private

        def retirement_parent=(parent)
          @lifetime = parent
        end

        def retirement_completed? = !!@retirement_completed

        def retirement_roots
          outputs = (@connection ? @connection.streams.values : []).flat_map do |stream|
            [stream.respond_to?(:response_task) ? stream.response_task : nil,
             stream.respond_to?(:output_task) ? stream.output_task : nil]
          end
          [*@connection&.writers&.keys, @connection&.reader_task, *@tasks, @task, *outputs].compact.uniq
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
