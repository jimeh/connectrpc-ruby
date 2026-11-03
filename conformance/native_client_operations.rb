# frozen_string_literal: true

module Conformance
  class NativeClientAdapter < ClientAdapter
    def invoke_native(core, declaration, messages, request:, options:, result:, source:)
      case declaration
      when ConnectRPC::Method
        @timer = schedule_after_close(request, source)
        response = core.call(declaration, messages.fetch(0), **options)
        result.payloads << (response.message.payload || ConformancePayload.new)
        result.response_headers.replace(Conformance.headers(response.metadata))
        result.response_trailers.replace(Conformance.headers(response.trailers))
      when ConnectRPC::ServerStreamMethod
        invoke_server_stream(core, declaration, messages, request: request, options: options, result: result,
                                                          source: source)
      when ConnectRPC::ClientStreamMethod
        invoke_client_stream(core, declaration, messages, request: request, options: options, result: result,
                                                          source: source)
      when ConnectRPC::BidiStreamMethod
        invoke_bidi_stream(core, declaration, messages, request: request, options: options, result: result,
                                                        source: source)
      else
        raise "Unary belongs to the preserved original profile"
      end
    end

    def invoke_server_stream(core, declaration, messages, request:, options:, result:, source:)
      raise "Expected one server-stream request" unless messages.size == 1

      core.server_stream(declaration, messages.first, **options) do |reader|
        @endpoint = reader
        cancel_after_close(request, source)
        result.response_headers.replace(Conformance.headers(reader.headers))
        receive_all(reader, request, result, source)
      end
    end

    def invoke_client_stream(core, declaration, messages, request:, options:, result:, source:)
      response = core.client_stream(declaration, **options) do |writer|
        @endpoint = writer
        messages.each do |message|
          Conformance.delay(request.request_delay_ms)
          writer.send_message(message)
          @sent += 1
        end
        cancel_before_close(request, source)
        writer.close_send
        @timer = schedule_after_close(request, source)
      end
      result.payloads << (response.message.payload || ConformancePayload.new)
      result.response_headers.replace(Conformance.headers(response.metadata))
      result.response_trailers.replace(Conformance.headers(response.trailers))
    end

    def invoke_bidi_stream(core, declaration, messages, request:, options:, result:, source:)
      core.bidi_stream(declaration, **options) do |duplex|
        @endpoint = duplex
        send_bidi_messages(duplex, messages, request: request, result: result, source: source)
        cancel_before_close(request, source)
        duplex.close_send
        cancel_after_close(request, source)
        result.response_headers.replace(Conformance.headers(duplex.headers))
        receive_all(duplex, request, result, source)
      end
    end

    def send_bidi_messages(duplex, messages, request:, result:, source:)
      full = request.stream_type == :STREAM_TYPE_FULL_DUPLEX_BIDI_STREAM
      messages.each do |message|
        Conformance.delay(request.request_delay_ms)
        duplex.send_message(message)
        @sent += 1
        next unless full

        incoming = duplex.receive
        break if incoming.nil?

        add_payload(incoming, request, result, source)
      end
    end

    def add_payload(message, request, result, source)
      result.payloads << (message.payload || ConformancePayload.new)
      instruction = request.cancel
      if instruction&.cancel_timing == :after_num_responses && result.payloads.size == instruction.after_num_responses
        @observation.cancel(source, :after_num_responses)
      end
    end

    def receive_all(reader, request, result, source)
      reader.each { |message| add_payload(message, request, result, source) }
    end

    def cancel_before_close(request, source)
      @observation.cancel(source, :before_close_send) if request.cancel&.cancel_timing == :before_close_send
    end

    def cancel_after_close(request, source)
      instruction = request.cancel
      return unless instruction && [nil, :after_close_send_ms].include?(instruction.cancel_timing)

      Conformance.delay(instruction.after_close_send_ms)
      @observation.cancel(source, :after_close_send_ms)
    end

    def schedule_after_close(request, source)
      return unless request.cancel && [nil, :after_close_send_ms].include?(request.cancel.cancel_timing)

      ::Async::Task.current.async(finished: false) { cancel_after_close(request, source) }
    end
  end
end
