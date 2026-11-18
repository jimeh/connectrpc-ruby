# frozen_string_literal: true

require_relative "native_threaded_client"
require_relative "native_per_call_observation"

module Conformance
  # Official native client cases through PerCallTransport. With
  # CONNECTRPC_CONFORMANCE_PER_CALL_PATH=inline each call runs inside an Async
  # task on the adapter's reactor; otherwise it runs on the plain adapter
  # thread and its own short-lived owner thread. Cleanup is observed when the
  # call returns; nothing is closed or joined afterwards.
  class NativePerCallClientAdapter < NativeThreadedClientAdapter
    PATH = ENV.fetch("CONNECTRPC_CONFORMANCE_PER_CALL_PATH", "threaded")

    def perform(request)
      raise "Unknown per-call path" unless %w[threaded inline].include?(PATH)
      raise "PerCallTransport adapter calls start outside a reactor" if Fiber.scheduler

      PATH == "inline" ? Async(finished: false) { perform_per_call(request) }.wait : perform_per_call(request)
    rescue StandardError => e
      Telemetry.record("adapter_error", name: request.test_name, error: "#{e.class}: #{e.message}")
      ClientCompatResponse.new(test_name: request.test_name,
                               error: ClientErrorResult.new(message: "#{e.class}: #{e.message}"))
    ensure
      PerCallInnerObservation.finish
    end

    def perform_per_call(request)
      declaration = native_declaration(request)
      Dir.mktmpdir("native-per-call-conformance-") do |directory|
        @observation = ThreadedObservation.new(request.test_name)
        PerCallInnerObservation.begin(@observation)
        options = native_transport_options(request, directory)
        origin = options.delete(:origin)
        transport = ObservedPerCallTransport.new(observation: @observation, **options)
        core = native_core(request, transport, origin)
        execute_native(request, declaration, transport, core)
      end
    end

    # Each call closed its own connection before it returned.
    def close_native(_transport) = nil
  end
end

Conformance::NativePerCallClientAdapter.new.run if $PROGRAM_NAME == __FILE__
