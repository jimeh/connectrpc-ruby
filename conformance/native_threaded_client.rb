# frozen_string_literal: true

require_relative "native_client"
require_relative "native_threaded_observation"

module Conformance
  # Official native client cases through ThreadedTransport. Every call runs on
  # this plain adapter thread; only the transport's owner thread runs a reactor.
  class NativeThreadedClientAdapter < NativeClientAdapter
    SHAPES = %i[STREAM_TYPE_UNARY STREAM_TYPE_SERVER_STREAM].freeze
    # Every call has finished before close, so a clean drain returns true at
    # once. Zero grace would force teardown, which always returns false.
    CLOSE_GRACE = 5

    # Runs the original after-close cancellation on a plain timer thread.
    class CancelTimer
      def initialize(milliseconds, &action)
        @stop = Thread::Queue.new
        @thread = Thread.new { action.call if @stop.pop(timeout: milliseconds / 1000.0).nil? }
      end

      def stop
        @stop << true
        @thread.join
      end
    end

    def perform(request)
      raise "ThreadedTransport adapter calls never run inside a reactor" if Fiber.scheduler

      declaration = native_declaration(request)
      Dir.mktmpdir("native-threaded-conformance-") do |directory|
        @observation = ThreadedObservation.new(request.test_name)
        options = native_transport_options(request, directory)
        transport = ObservedThreadedTransport.new(observation: @observation, **options)
        core = native_core(request, transport, options.fetch(:origin))
        execute_native(request, declaration, transport, core)
      end
    rescue StandardError => e
      Telemetry.record("adapter_error", name: request.test_name, error: "#{e.class}: #{e.message}")
      ClientCompatResponse.new(test_name: request.test_name,
                               error: ClientErrorResult.new(message: "#{e.class}: #{e.message}"))
    end

    def native_declaration(request)
      raise "Unsupported threaded client shape" unless SHAPES.include?(request.stream_type)

      super
    end

    def native_core(request, transport, origin)
      grpc = request.protocol == :PROTOCOL_GRPC
      ObservedNativeClient.new(observation: @observation, base_url: origin, transport: transport, binding: BINDING,
                               use_get: !grpc && request.use_get_http_method,
                               codec: request.codec == :CODEC_PROTO ? :proto : :json,
                               protocol: grpc ? :grpc : :connect,
                               encoding: request.compression == :COMPRESSION_GZIP ? :gzip : :identity,
                               response_limit: Conformance.limit(request.message_receive_limit))
    end

    def close_native(transport) = transport.close(grace: CLOSE_GRACE)

    def schedule_after_close(request, source)
      instruction = request.cancel
      return unless instruction && [nil, :after_close_send_ms].include?(instruction.cancel_timing)

      CancelTimer.new(instruction.after_close_send_ms) { @observation.cancel(source, :after_close_send_ms) }
    end
  end
end

Conformance::NativeThreadedClientAdapter.new.run if $PROGRAM_NAME == __FILE__
