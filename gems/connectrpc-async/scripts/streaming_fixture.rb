# frozen_string_literal: true

require "google/protobuf/wrappers_pb"

module StreamingInterop
  MESSAGE = Google::Protobuf::StringValue
  METHODS = [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod,
             ConnectRPC::Method].zip(%w[SS CS Bidi Control]).map do |type, name|
    type.new(service: "interop.Streams", name: name, request_type: MESSAGE, response_type: MESSAGE)
  end.freeze
  SERVICE = ConnectRPC::Service.new(name: "interop.Streams", methods: METHODS)
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new

  def self.check(condition, message)
    raise message unless condition
  end

  def self.server(coordination: nil)
    release = nil
    count = 0
    handlers = {
      METHODS[0].path => server_stream_handler(coordination, ->(queue) { release = queue }),
      METHODS[1].path => client_stream_handler(coordination, ->(value) { count = value }),
      METHODS[2].path => lambda { |duplex, context|
        context.send_headers(metadata: { "x-head" => "early" })
        context.response_trailers["x-tail"] = ["done"]
        duplex.each { |message| duplex.send_message(message) }
      },
      METHODS[3].path => lambda { |message, _|
        if message.value == "release"
          coordination ? File.write(File.join(coordination, "release"), "ready") : release.enqueue(true)
        end
        count = File.read(File.join(coordination, "count")).to_i if coordination && File.exist?(File.join(coordination,
                                                                                                          "count"))
        MESSAGE.new(value: count.to_s)
      }
    }
    ConnectRPC::Server.new(services: [SERVICE], binding: BINDING, handlers: handlers,
                           request_limit: 1024, encoding: :gzip)
  end

  def self.server_stream_handler(coordination, on_release)
    lambda { |message, writer, context|
      context.send_headers(metadata: { "x-head" => "early" })
      context.response_trailers["x-tail"] = ["done"]
      raise ConnectRPC::Error.new(:not_found, "missing") if message.value == "error"

      if message.value == "gate"
        File.unlink(File.join(coordination, "release")) if coordination && File.exist?(File.join(coordination,
                                                                                                 "release"))
        release = Async::Queue.new
        on_release.call(release)
        writer.send_message(MESSAGE.new(value: "first"))
        if coordination
          Async::Task.current.with_timeout(3) do
            Async::Task.current.yield until File.exist?(File.join(coordination, "release"))
          end
        else
          release.dequeue
        end
      end
      writer.send_message(message)
    }
  end

  def self.client_stream_handler(coordination, on_count)
    lambda { |reader, context|
      count = 0
      on_count.call(count)
      publish_count(coordination, count) if coordination
      context.send_headers(metadata: { "x-head" => "early" })
      context.response_trailers["x-tail"] = ["done"]
      reader.each do |_|
        count += 1
        on_count.call(count)
        publish_count(coordination, count) if coordination
      end
      MESSAGE.new(value: count.to_s)
    }
  end

  # Test-only worker coordination. H1 control RPCs use a separate connection
  # and may reach either process, unlike multiplexed H2 control requests.
  def self.publish_count(directory, count)
    temporary = File.join(directory, "count-#{Process.pid}")
    File.write(temporary, count.to_s)
    File.rename(temporary, File.join(directory, "count"))
  end

  class ClientProbe
    def initialize(core)
      @core = core
    end

    def run
      @core.server_stream(METHODS[0], MESSAGE.new(value: "gate")) do |reader|
        StreamingInterop.check(reader.headers["x-head"] == ["early"], "SS early headers")
        StreamingInterop.check(reader.receive.value == "first", "SS first before producer completion")
        @core.call(METHODS[3], MESSAGE.new(value: "release"))
        StreamingInterop.check(reader.receive.value == "gate" && reader.receive.nil?, "SS terminal")
        StreamingInterop.check(reader.trailers["x-tail"] == ["done"], "SS trailers")
      end
      response = @core.client_stream(METHODS[1]) do |writer|
        writer.send_message(MESSAGE.new)
        Async::Task.current.with_timeout(3) do
          loop do
            break if @core.call(METHODS[3], MESSAGE.new(value: "status")).message.value == "1"
          end
        end
        writer.send_message(MESSAGE.new(value: "two"))
      end
      StreamingInterop.check(response.message.value == "2", "CS progress before all input")
      StreamingInterop.check(@core.client_stream(METHODS[1]) { nil }.message.value == "0", "CS zero input")
      @core.bidi_stream(METHODS[2]) do |duplex|
        ["", "two"].each do |value|
          duplex.send_message(MESSAGE.new(value: value))
          StreamingInterop.check(duplex.receive.value == value, "bidi response before halfclose/default")
        end
        duplex.close_send
        StreamingInterop.check(duplex.receive.nil? && duplex.trailers["x-tail"] == ["done"], "bidi terminal")
      end
      errors
      cancellation
    end

    def errors
      { "error" => "not_found", "x" * 2048 => "resource_exhausted" }.each do |value, code|
        @core.server_stream(METHODS[0], MESSAGE.new(value: value)) { |reader| reader.each { |_| nil } }
        raise "Missing #{code}"
      rescue ConnectRPC::Error => e
        StreamingInterop.check(e.code == code, "Wrong early error/bound: #{e.code}")
      end
    end

    def cancellation
      source = ConnectRPC::CancellationSource.new
      begin
        @core.bidi_stream(METHODS[2], timeout: nil, cancellation: source.token) do |duplex|
          duplex.send_message(MESSAGE.new(value: "progress"))
          StreamingInterop.check(duplex.receive.value == "progress", "cancellation wire progress")
          source.cancel
          duplex.receive
        end
        raise "Missing cancellation"
      rescue ConnectRPC::Error => e
        StreamingInterop.check(e.code == "canceled" && e.cause.nil?, "Wrong canceled result")
      end
      StreamingInterop.check(@core.client_stream(METHODS[1]) { nil }.message.value == "0", "healthy after cancel")
    end
  end
end
