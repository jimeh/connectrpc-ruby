# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # One queued fragment and the output task's one in-flight fragment. Queue
      # ownership transfers before waking the producer; no wire-flush promise.
      class OutputBody < ::Protocol::HTTP::Body::Readable
        attr_reader :queued_bytes, :maximum_queued_bytes

        def initialize
          super
          @changed = ::Async::Condition.new
          @fragment = nil
          @queued_bytes = @maximum_queued_bytes = 0
          @ended = @closed = false
        end

        def write(bytes, timeout: nil)
          raise ArgumentError, "Output fragments must fit one frame" if bytes.bytesize > 16_384

          expires = Internal.now + timeout if timeout
          while @fragment && !@closed
            if expires
              waiter = ::Async::Task.current.async(finished: false) { @changed.wait }
              begin
                Internal.wait(waiter, expires)
              ensure
                Internal.cancel(waiter)
                Internal.join(waiter)
              end
            else
              @changed.wait
            end
          end
          raise(@error || IOError.new("Output is closed")) if @closed || @ended

          @fragment = bytes.dup.freeze
          @queued_bytes = bytes.bytesize
          @maximum_queued_bytes = [@maximum_queued_bytes, @queued_bytes].max
          @changed.signal
          nil
        end

        def read
          @changed.wait until @fragment || @ended || @closed
          raise @error if @error

          fragment = @fragment
          @fragment = nil
          @queued_bytes = 0
          @changed.signal
          fragment
        end

        def close_write
          @ended = true
          @changed.signal
          nil
        end

        def close(error = nil)
          return if @closed

          @closed = true
          @error = error
          @fragment = nil
          @queued_bytes = 0
          @changed.signal
          nil
        end
      end

      # Publication and processing completion are independent single-assignment
      # events. The processing task owns both, including exceptional cleanup.
      class Publication
        def initialize
          @head = ::Async::Promise.new
          @completion = ::Async::Promise.new
        end

        def publish(response)
          raise LifecycleError, "Response head already published" if @published

          @published = true
          @transform&.call(response)
          @body = response&.body
          response.body = CompletionBody.new(@body, self) if response
          @head.resolve([:response, response])
          @head_ready = true
          nil
        rescue StandardError => e
          @body = response&.body
          complete(e)
          raise
        end

        def complete(error = nil)
          return if @completed

          @completed = true
          begin
            @body&.close(error) if error
          rescue StandardError => e
            error ||= e
          ensure
            unless @head_ready
              @head_ready = true
              @head.resolve([:error, error || IOError.new("Processing ended without a response")])
            end
            @completion.resolve(error)
          end
          nil
        end

        def on_publish(&transform)
          raise LifecycleError, "Response head already published" if @published

          previous = @transform
          @transform = lambda { |response|
            previous&.call(response)
            transform.call(response)
          }
          nil
        end

        def published? = @published

        def response
          tag, value = @head.wait
          raise value if tag == :error

          value
        end

        def wait
          error = @completion.wait
          raise error if error
        end
      end

      class CompletionBody < ::Protocol::HTTP::Body::Wrapper
        def initialize(body, publication)
          super(body || ::Protocol::HTTP::Body::Buffered.wrap([]))
          @publication = publication
        end

        # Even an empty body must retain this completion boundary.
        def empty? = false
        def length = nil

        def read
          bytes = super
          @publication.wait unless bytes
          bytes
        end
      end

      class ServerOutput
        def initialize(body)
          @body = body
        end

        def write(bytes, timeout: nil) = @body.write(bytes, timeout: timeout)
        def abort = @body.close(IOError.new("Processing abandoned"))
      end

      class GRPCInput < Input
        def read(length, timeout: nil)
          super
        rescue ::Async::TimeoutError
          raise Error.new(:unavailable, "gRPC input operation expired"), cause: nil
        end
      end

      class GRPCServerOutput < ServerOutput
        attr_writer :headers

        def initialize(body, _trailers)
          super(body)
        end

        def write(bytes, timeout: nil)
          super
        rescue ::Async::TimeoutError
          raise Error.new(:unavailable, "gRPC output operation expired"), cause: nil
        end

        def finish(headers:)
          GRPCProtocol.check_fields!(headers)
          headers.each { |key, values| values.each { |value| @headers.add(key, value, trailer: true) } }
          @body.close_write
          nil
        end
      end

      class StreamExchange
        def initialize(transport, operation, body)
          @transport = transport
          @operation = operation
          @body = body
          @response = operation.fetch(:response)
        end

        def response_head(timeout: nil)
          @response_head ||= perform(timeout: timeout) do
            @operation.fetch(:connection).read_response(@response)
            @input = Input.new(@response.body)
            StreamHead.new(status: @response.status, headers: Internal.headers(@response))
          end
        end

        def read(length, timeout: nil)
          response_head(timeout: timeout)
          perform(timeout: timeout) { @input.read(length) }
        end

        def write(bytes)
          perform { @body.write(bytes) }
          nil
        end

        def close_send
          perform { @body.close_write }
          nil
        end

        def finish(timeout: nil)
          perform(timeout: timeout) { @response.stream.output_task&.wait }
          nil
        end

        def abort
          @body.close(IOError.new("Stream abandoned"))
          nil
        end

        private

        def perform(timeout: nil, &)
          @transport.send(:stream_operation, @operation, timeout: timeout, &)
        end
      end

      class GRPCStreamExchange < StreamExchange
        def response_head(timeout: nil)
          @response_head ||= perform(timeout: timeout) do
            @operation.fetch(:connection).read_response(@response)
            @input = Input.new(@response.body)
            stream = @response.stream
            raise stream.grpc_error if stream.grpc_error

            GRPCHead.new(status: @response.status, headers: stream.grpc_initial_headers,
                         end_stream: stream.grpc_initial_end_stream)
          end
        end

        def read(length, timeout: nil)
          bytes = super
          @read_eof = true if bytes.nil? || bytes.empty?
          raise @response.stream.grpc_error if @response.stream.grpc_error

          bytes
        end

        def response_trailers
          raise StreamStateError, "Trailers require response EOF" unless @read_eof

          @response.stream.grpc_trailers
        end

        def finish(timeout: nil)
          super
          stream = @response.stream
          raise stream.grpc_error if stream.grpc_error
          unless stream.grpc_initial_end_stream || stream.grpc_trailers
            raise Error.new(:internal, "Missing final gRPC HEADERS"), cause: nil
          end

          nil
        end
      end
    end
  end
end
