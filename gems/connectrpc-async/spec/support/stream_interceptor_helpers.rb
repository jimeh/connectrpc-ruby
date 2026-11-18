# frozen_string_literal: true

require "delegate"

# Stream declarations, echo handlers, logging layers and wrappers for stream
# interceptor examples over real hosts. The Falcon bundle loads this file too.
module StreamInterceptorHelpers
  SHAPES = { server_stream: ConnectRPC::ServerStreamMethod, client_stream: ConnectRPC::ClientStreamMethod,
             bidi_stream: ConnectRPC::BidiStreamMethod }.freeze

  # The operations each side's wrappers log while drive_stream and
  # echo_handlers run one shape.
  CLIENT_OPERATIONS = { server_stream: %w[receive receive receive], client_stream: %w[send send],
                        bidi_stream: %w[send receive send receive receive] }.freeze
  SERVER_OPERATIONS = { server_stream: %w[send send], client_stream: %w[receive receive receive],
                        bidi_stream: %w[receive send receive send receive] }.freeze

  # Logs each operation, then delegates. Read-side wrappers override both
  # receive and each, so reads through each pass through the wrapper too.
  class TracingStream < SimpleDelegator
    def initialize(stream, name, log)
      super(stream)
      @name = name
      @log = log
    end

    def send_message(message)
      @log << "#{@name}.send"
      __getobj__.send_message(message)
    end

    def receive
      @log << "#{@name}.receive"
      __getobj__.receive
    end

    def each
      while (message = receive)
        yield message
      end
      nil
    end
  end

  def stream_rpc(shape)
    (@stream_rpcs ||= {})[shape] ||= SHAPES.fetch(shape).new(
      service: "test.Intercept", name: shape.to_s.split("_").map(&:capitalize).join,
      request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE
    )
  end

  def client_operations(shape) = CLIENT_OPERATIONS.fetch(shape)
  def server_operations(shape) = SERVER_OPERATIONS.fetch(shape)

  def stream_message(name) = AsyncHelpers::MESSAGE.new(name: name)

  def stream_target(handlers, **)
    service = ConnectRPC::Service.new(name: "test.Intercept", rpcs: SHAPES.keys.map { |shape| stream_rpc(shape) })
    ConnectRPC::Server.new(services: [service], binding: ConnectRPC::Bindings::GoogleProtobuf.new, handlers: handlers,
                           **)
  end

  # Echo handlers for every shape; each logs "handler" on entry.
  def echo_handlers(log = [])
    {
      stream_rpc(:server_stream).path => lambda { |request, writer, _context|
        log << "handler"
        2.times { |index| writer.send_message(stream_message("#{request.name}-#{index + 1}")) }
      },
      stream_rpc(:client_stream).path => lambda { |reader, _context|
        log << "handler"
        names = []
        reader.each { |message| names << message.name } # rubocop:disable Style/MapIntoArray -- RequestReader has block-only each.
        stream_message(names.join(","))
      },
      stream_rpc(:bidi_stream).path => lambda { |duplex, _context|
        log << "handler"
        duplex.each { |message| duplex.send_message(message) }
      }
    }
  end

  # Runs one shape against echo_handlers and returns what the caller got.
  def drive_stream(core, shape, **)
    rpc = stream_rpc(shape)
    received = []
    case shape
    when :server_stream
      core.server_stream(rpc, stream_message("q"), **) { |reader| reader.each { |message| received << message.name } }
    when :client_stream
      return core.client_stream(rpc, **) { |writer| %w[a b].each { |name| writer.send_message(stream_message(name)) } }
    else
      core.bidi_stream(rpc, **) do |duplex|
        %w[a b].each do |name|
          duplex.send_message(stream_message(name))
          received << duplex.receive.name
        end
        duplex.close_send
        received << duplex.receive
      end
    end
    received
  end

  def interceptor(role, &body)
    Object.new.tap do |layer|
      layer.define_singleton_method(role) { |call, &continuation| body.call(call, &continuation) }
    end
  end

  # Logs around yield and registers a TracingStream. Client layers also add
  # an x-<name> request header; +calls+ collects every call object.
  def tracing_layer(role, name, log, calls: [])
    interceptor(role) do |call, &continuation|
      calls << call
      log << "#{name}>"
      call.request_metadata["x-#{name}"] = "sent" if role == :client_stream
      call.wrap_stream { |stream| TracingStream.new(stream, name, log) }
      continuation.call.tap { log << "<#{name}" }
    end
  end

  # The log of layers +names+ (outermost first) around +inner+ and the
  # wrapper operations, which pass through the outermost wrapper first.
  def traced(names, operations, inner = [])
    names.map { |name| "#{name}>" } + inner + operations.flat_map { |op| names.map { |name| "#{name}.#{op}" } } +
      names.reverse.map { |name| "<#{name}" }
  end
end
