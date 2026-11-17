# frozen_string_literal: true

module ConnectRPC
  class StreamStateError < StandardError; end
  class StreamNotConsumedError < StreamStateError; end

  StreamRequest = Struct.new(:url, :http_method, :headers, :deadline, :lifecycle, :cancellation, keyword_init: true)
  StreamHead = Struct.new(:status, :headers, keyword_init: true)

  class StreamingContext < Context
    def initialize(publish:, request_metadata: {}, deadline: nil, lifecycle: nil, cancellation: nil)
      super(request_metadata: request_metadata, deadline: deadline, lifecycle: lifecycle, cancellation: cancellation)
      @publish = publish
      @headers_sent = @headers_published = false
      @owner_thread = Thread.current
      @owner_scheduler = Fiber.scheduler
      @stream_closed = false
    end

    def send_headers(metadata: {})
      check_headers_scope!
      raise StreamStateError, "Response headers already published" if @headers_sent

      check!
      Metadata.new(metadata).each { |key, values| @response_metadata[key] = values }
      publish_snapshot
    end

    def close
      @stream_closed = true
      super
    end

    private

    def check_headers_scope!
      raise StreamStateError, "Stream scope is closed" if @stream_closed
      return if Thread.current.equal?(@owner_thread) && Fiber.scheduler.equal?(@owner_scheduler)

      raise StreamStateError, "Use the stream's owning reactor and thread"
    end

    # A checked caller is about to write a message, which needs a published head.
    def publish_headers(check: true)
      if @headers_sent
        return if @headers_published || !check

        raise Error.new(:internal, "Response headers were not published"), cause: nil
      end
      return send_headers if check

      publish_snapshot
    end

    # An attempt is recorded before the publisher runs, so a raising publisher
    # is never invoked twice; headers_published? stays false after that failure.
    def publish_snapshot
      @response_metadata.freeze
      @headers_sent = true
      @publish.call(@response_metadata)
      @headers_published = true
      nil
    end

    def headers_published? = @headers_published
  end

  class ResponseReader
    def initialize(session)
      @session = session
    end

    def receive = @session.receive
    def headers = @session.headers
    def trailers = @session.trailers
    def abandon = @session.abandon

    def each
      raise ArgumentError, "A block is required" unless block_given?

      while true # rubocop:disable Style/InfiniteLoop -- Kernel.loop swallows application StopIteration.
        case message = receive
        when nil then break
        else yield message
        end
      end
      nil
    end
  end

  class RequestWriter
    def initialize(session)
      @session = session
    end

    def send_message(message) = @session.send_message(message)
    def close_send = @session.close_send
    def abandon = @session.abandon
  end

  class ClientDuplex < ResponseReader
    def send_message(message) = @session.send_message(message)
    def close_send = @session.close_send
  end

  class RequestReader < ResponseReader; end

  class ResponseWriter
    def initialize(session, context)
      @session = session
      @context = context
    end

    def send_message(message) = @session.send_message(message)
    def send_headers(metadata: {}) = @context.send_headers(metadata: metadata)
  end

  class ServerDuplex < RequestReader
    def initialize(session, context)
      super(session)
      @context = context
    end

    def send_message(message) = @session.send_message(message)
    def send_headers(metadata: {}) = @context.send_headers(metadata: metadata)
  end
end
