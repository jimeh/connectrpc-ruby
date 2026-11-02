# frozen_string_literal: true

module ConnectRPC
  # Bounded host-neutral input reader. A synchronous host still owns blocking I/O.
  class MessageIO
    def initialize(input, limit, context)
      @input = input
      @limit = limit
      @context = context
    end

    def read(length)
      raise Error.new(:resource_exhausted, "Message exceeds byte limit") if length && length > @limit

      body = +"".b
      read_chunks(body) if @input
      @context.check!
      raise Error.new(:invalid_argument, "Content-Length does not match body") if length && body.bytesize != length

      body
    end

    private

    def read_chunks(body)
      loop do
        @context.check!
        chunk = @input.read([16_384, @limit + 1 - body.bytesize].min)
        @context.check!
        break if chunk.nil? || chunk.empty?

        raise Error.new(:resource_exhausted, "Message exceeds byte limit") if body.bytesize + chunk.bytesize > @limit

        body << chunk
      end
    end
  end
end
