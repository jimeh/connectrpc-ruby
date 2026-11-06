# frozen_string_literal: true

require "json"

class HTTP2InteropDiagnostics
  def self.instrumentation(transport, mode:, codec: :proto, output: $stderr)
    subscriber = new(transport: transport, mode: mode, codec: codec, output: output)
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end

  def initialize(transport:, mode:, codec:, output:)
    @transport = transport
    @mode = mode
    @codec = codec
    @output = output
  end

  def start(call)
    Observation.new(call: call, transport: @transport, mode: @mode, codec: @codec, output: @output)
  end

  Observation = Data.define(:call, :transport, :mode, :codec, :output) do
    def processing_finished(outcome)
      return if %w[OK invalid_argument resource_exhausted canceled].include?(outcome.code)

      original = outcome.error || outcome.normalized_error
      return unless original

      diagnostic = {
        mode: mode, codec: codec, method: call.method&.path, phase: transport_phase(original),
        code: outcome.code, duration: outcome.duration,
        original_error: { class: original.class.name, message: original.message,
                          backtrace: original.backtrace&.first(16) },
        resources_after_cleanup: resources_after_cleanup
      }
      output.puts("HTTP/2 interop failure #{JSON.generate(diagnostic)}")
    end

    private

    def transport_phase(error)
      entry = error.backtrace_locations&.find { |location| location.path.end_with?("/connectrpc/async/transport.rb") }
      entry&.base_label
    end

    def resources_after_cleanup
      transport.instance_variable_get(:@resources).map do |resource|
        connection = resource.connection
        { ready: resource.ready, tcp_closed: resource.raw&.closed?,
          reader_finished: connection&.reader_task&.finished?, writers: connection&.writers&.size }
      end
    end
  end
end
