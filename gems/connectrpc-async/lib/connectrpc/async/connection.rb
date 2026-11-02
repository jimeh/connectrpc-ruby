# frozen_string_literal: true

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

        def read_in_background(...)
          @reader_task = super
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

      module LeadingHeaders
        attr_reader :declared_length

        def receive_initial_headers(...)
          super
        ensure
          @declared_length = @length
        end

        def receive_trailing_headers(...)
          @headers.trailer!
          super
        end
      end

      class RequestStream < ::Async::HTTP::Protocol::HTTP2::Request::Stream
        include LeadingHeaders
      end

      class ResponseStream < ::Async::HTTP::Protocol::HTTP2::Response::Stream
        include LeadingHeaders
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

            worker = task.async(request, finished: false) do |_worker, incoming|
              response = nil
              begin
                response = yield(incoming)
                unless incoming.stream.closed?
                  synchronize do
                    ::Async::Task.current.defer_cancel { incoming.send_response(response) }
                  end
                  response = nil
                end
              rescue StandardError
                reset(incoming.stream)
              ensure
                close_request(response, incoming)
                @resource.changed
              end
            end
            @resource.track(worker)
          end
        end

        private

        def close_request(response, request)
          response&.close
          request.body&.close
        rescue ::Protocol::HTTP2::ProtocolError, IOError, SystemCallError
          nil
        end

        def reset(stream)
          stream.send_reset_stream(::Protocol::HTTP2::Error::INTERNAL_ERROR) unless stream.closed?
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
        end

        def changed
          @activity_at = Internal.now
          @progress.signal
          @shared_progress.signal
        end

        def track(task)
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
          return if @aborted

          @aborted = true
          @abort_during_drain = true if @drain_task && !@drained
          @connection&.close!
          # Raw TCP is the non-flushing primitive, including for TLS. Closing
          # SSL/Stream first can block forever trying to flush a partial frame.
          Internal.raw_close(@raw)
          owned = [*@connection&.writers&.keys, @connection&.reader_task, *@tasks, @task].compact.uniq
          # Cancel every writer/reader before joining any: a joined task can
          # itself wait for another owned task blocked on the closed socket.
          owned.each { |task| Internal.cancel(task) }
          # rubocop:disable-next Style/CombinableLoops
          owned.each { |task| Internal.join(task) }
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
            changed
          end
        end

        def drain(parent, expires)
          return Internal.wait(@drain_task, expires) if @drain_task
          return true if @aborted

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
            Internal.cancel(reader)
            Internal.join(reader)
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
      end
    end
  end
end
