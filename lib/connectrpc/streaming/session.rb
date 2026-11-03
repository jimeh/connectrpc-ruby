# frozen_string_literal: true

module ConnectRPC
  module Streaming
    # One guard per direction retains operation origin without class/message guesses.
    class Direction
      attr_reader :error, :busy

      def initialize(lifecycle)
        @lifecycle = lifecycle
        @busy = false
      end

      def run
        raise StreamStateError, "Overlapping stream operation" if @busy

        @busy = true
        begin
          yield
        rescue StandardError => e
          record_failure(e)
          raise
        ensure
          @busy = false
        end
      end

      private

      def record_failure(error)
        @error = error
        @lifecycle&.record_failure(error)
      end
    end

    class Session
      attr_reader :trailers, :received, :sent

      def initialize(binding:, method:, codec:, state:, input:, output:, request_limit:, response_limit:,
                     request_wire_limit:, response_wire_limit:, request_encoding:, response_encoding:,
                     terminal_wire_limit:, terminal_limit:, server: false, context: nil, lifecycle: nil,
                     accept_encoding: [:identity])
        initialize_owner(binding, method, codec, state)
        initialize_channels(input, output, server, context, lifecycle)
        @accept_encoding = accept_encoding
        initialize_messages(request_limit, response_limit)
        initialize_framing(request_wire_limit, response_wire_limit, request_encoding, response_encoding,
                           terminal_wire_limit: terminal_wire_limit, terminal_limit: terminal_limit)
      end

      def abandoned? = @abandoned
      def consumed? = @eof
      def send_failure?(error) = error.equal?(@send_failure)
      def owned_error?(error) = error.equal?(@reading.error) || error.equal?(@writing.error)

      def close
        @closed = true
        nil
      end

      private

      def initialize_owner(binding, method, codec, state)
        @binding = binding
        @method = method
        @codec = codec
        @state = state
        @thread = Thread.current
        @scheduler = Fiber.scheduler
        @closed = @send_closed = @eof = @abandoned = false
      end

      def initialize_channels(input, output, server, context, lifecycle)
        @input = input
        @output = output
        @server = server
        @context = context
        @lifecycle = lifecycle
        @reading = Direction.new(lifecycle)
        @writing = Direction.new(lifecycle)
      end

      def initialize_messages(request_limit, response_limit)
        @received = @sent = 0
        @read_limit = @server ? request_limit : response_limit
        @write_limit = @server ? response_limit : request_limit
        @read_type = @server ? @method.request_type : @method.response_type
        @write_type = @server ? @method.response_type : @method.request_type
      end

      def initialize_framing(request_wire, response_wire, request_encoding, response_encoding,
                             terminal_wire_limit:, terminal_limit:)
        initialize_reader(request_wire, response_wire, request_encoding, response_encoding,
                          terminal_wire_limit: terminal_wire_limit, terminal_limit: terminal_limit)
        initialize_writer(request_wire, response_wire, request_encoding, response_encoding,
                          terminal_wire_limit: terminal_wire_limit, terminal_limit: terminal_limit)
      end

      def initialize_reader(request_wire, response_wire, request_encoding, response_encoding,
                            terminal_wire_limit:, terminal_limit:)
        @reader = Framing.new(input: self, request: @server, limit: @read_limit, state: @state,
                              wire_limit: @server ? request_wire : response_wire,
                              encoding: @server ? request_encoding : response_encoding,
                              terminal_wire_limit:, terminal_limit:)
      end

      def initialize_writer(request_wire, response_wire, request_encoding, response_encoding,
                            terminal_wire_limit:, terminal_limit:)
        @writer = Framing.new(input: nil, output: self, request: !@server, limit: @write_limit, state: @state,
                              wire_limit: @server ? response_wire : request_wire,
                              encoding: @server ? response_encoding : request_encoding,
                              terminal_wire_limit: terminal_wire_limit,
                              terminal_limit: terminal_limit)
      end

      def check_open!
        raise StreamStateError, "Stream scope is closed" if @closed || @abandoned
        return if Thread.current.equal?(@thread) && Fiber.scheduler.equal?(@scheduler)

        raise StreamStateError, "Use the stream's owning reactor and thread"
      end

      def operation(direction)
        check_open!
        guard = direction == :reading ? @reading : @writing
        guard.run do
          @state.check!
          yield
        end
      end
    end
  end
end
