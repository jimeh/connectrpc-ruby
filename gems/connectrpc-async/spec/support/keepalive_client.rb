# frozen_string_literal: true

require "protocol/http/body/buffered"

# A raw h2c client for server keepalive specs. It records the server's
# SETTINGS, PINGs, ACKs, GOAWAYs, resets, HEADERS, DATA and WINDOW_UPDATE frames. It acknowledges server
# PINGs only while `ack_pings` is set, and records a GOAWAY without applying
# it, so it can keep opening streams like a peer that raced the GOAWAY. With
# `ack_settings: false` it neither applies nor acknowledges server SETTINGS.
# `settings` override the client's advertised SETTINGS, and with
# `read: false` the client reads only the server's first SETTINGS frame.
module KeepaliveClient
  DRAIN_PING = [1, 6, 1, 8, 0, 3, 3, 9].pack("C*")
  MAX_STREAM_ID = 0x7fff_ffff
  NO_ERROR = Protocol::HTTP2::Error::NO_ERROR
  ENHANCE_YOUR_CALM = Protocol::HTTP2::Error::ENHANCE_YOUR_CALM
  REFUSED_STREAM = Protocol::HTTP2::Error::REFUSED_STREAM
  CANCEL = Protocol::HTTP2::Error::CANCEL
  INITIAL_WINDOW_SIZE = Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE
  MAXIMUM_WINDOW = Protocol::HTTP2::MAXIMUM_ALLOWED_WINDOW_SIZE

  class Wire
    # `sequence` orders the ACKs and GOAWAYs as they arrived.
    # `reset_counts` counts every RST_STREAM per stream; `credit` sums the
    # WINDOW_UPDATE increments per stream, 0 for the connection.
    attr_reader :settings, :pings, :acks, :goaways, :resets, :reset_counts, :headers, :data, :data_bytes, :credit,
                :sequence
    attr_accessor :ack_pings

    def initialize
      @settings = []
      @pings = []
      @acks = []
      @goaways = []
      @resets = {}
      @reset_counts = Hash.new(0)
      @headers = Hash.new(0)
      @data = Hash.new(0)
      @data_bytes = Hash.new(0)
      @credit = Hash.new(0)
      @sequence = []
      @ack_pings = true
    end

    def record(frame)
      return record_stream(frame) unless frame.connection?

      case frame
      when Protocol::HTTP2::WindowUpdateFrame then @credit[0] += frame.unpack
      when Protocol::HTTP2::SettingsFrame then @settings << frame.unpack unless frame.acknowledgement?
      when Protocol::HTTP2::PingFrame then record_ping(frame)
      when Protocol::HTTP2::GoawayFrame
        @goaways << frame.unpack
        @sequence << [:goaway, *frame.unpack]
      end
    end

    def record_stream(frame)
      case frame
      when Protocol::HTTP2::ResetStreamFrame
        @resets[frame.stream_id] = frame.unpack
        @reset_counts[frame.stream_id] += 1
      when Protocol::HTTP2::WindowUpdateFrame then @credit[frame.stream_id] += frame.unpack
      when Protocol::HTTP2::HeadersFrame then @headers[frame.stream_id] += 1
      when Protocol::HTTP2::DataFrame
        @data[frame.stream_id] += 1
        @data_bytes[frame.stream_id] += frame.length
      end
    end

    def record_ping(frame)
      (frame.acknowledgement? ? @acks : @pings) << frame.unpack
      @sequence << [:ack, frame.unpack] if frame.acknowledgement?
    end
  end

  Fixture = Struct.new(:host, :clock, :client, :resource, :log)

  Client = Struct.new(:connection, :wire, :raw, :reader) do
    def eof? = !!reader&.finished?
  end

  # With a `tls` context the client negotiates h2 over TLS; `raw` stays TCP.
  # A positive `receive_buffer` sets SO_RCVBUF before connecting.
  def keepalive_client(task, port, tls: nil, ack_settings: true, settings: {}, read: true, receive_buffer: nil)
    raw = keepalive_socket(port, receive_buffer)
    socket = raw
    if tls
      socket = OpenSSL::SSL::SSLSocket.new(raw, tls)
      socket.sync_close = true
      socket.hostname = "localhost"
      socket.connect
    end
    connection = Async::HTTP::Protocol::HTTP2::Client.new(IO::Stream(socket))
    wire = Wire.new
    connection.define_singleton_method(:read_frame) do
      super() { |frame| wire.record(frame) }
    end
    connection.define_singleton_method(:receive_ping) do |frame|
      super(frame) if frame.acknowledgement? || wire.ack_pings
    end
    connection.define_singleton_method(:receive_goaway) { |_frame| nil }
    unless ack_settings
      connection.define_singleton_method(:receive_settings) { |frame| super(frame) if frame.acknowledgement? }
    end
    connection.send_connection_preface(AsyncHelpers::INTERNAL.settings(128).merge(settings))
    reader = connection.read_in_background(parent: task) if read
    Client.new(connection, wire, raw, reader)
  end

  def keepalive_socket(port, receive_buffer)
    return IO::Endpoint.tcp("127.0.0.1", port).connect unless receive_buffer

    socket = Socket.new(:INET, :STREAM)
    socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, receive_buffer)
    socket.connect(Socket.sockaddr_in(port, "127.0.0.1"))
    socket
  end

  def close_keepalive_client(client)
    return unless client

    AsyncHelpers::INTERNAL.raw_close(client.raw)
    client.connection.close
  end

  def send_ping(client, payload, ack: false)
    frame = Protocol::HTTP2::PingFrame.new(0, ack ? Protocol::HTTP2::ACKNOWLEDGEMENT : 0)
    frame.pack(payload)
    client.connection.write_frame(frame)
  end

  # Each payload is unique, so its ACK proves the server read every earlier frame.
  def ping_through(task, client, payload)
    send_ping(client, payload)
    wait_until(task, "ACK for #{payload.inspect}; acks #{client.wire.acks}") { client.wire.acks.include?(payload) }
  end

  def connect_request(path, name, content_type: "application/proto")
    bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: name))
    bytes = [0, bytes.bytesize].pack("CN") + bytes if content_type == "application/connect+proto"
    Protocol::HTTP::Request["POST", path, { "content-type" => content_type }, [bytes],
                            scheme: "http", authority: "127.0.0.1"]
  end

  # A request stream written frame by frame, outside the client's stream state.
  def raw_headers(client, stream_id, headers, end_stream: false)
    connection = client.connection
    frame = Protocol::HTTP2::HeadersFrame.new(stream_id, end_stream ? Protocol::HTTP2::END_STREAM : 0)
    connection.write_frames do |framer|
      frame.pack(connection.encode_headers(headers), maximum_size: connection.maximum_frame_size)
      framer.write_frame(frame)
    end
    frame
  end

  def raw_data(client, stream_id, bytes, end_stream: false)
    frame = Protocol::HTTP2::DataFrame.new(stream_id, end_stream ? Protocol::HTTP2::END_STREAM : 0)
    frame.pack(bytes)
    client.connection.write_frame(frame)
  end

  def raw_window_update(client, stream_id, amount)
    frame = Protocol::HTTP2::WindowUpdateFrame.new(stream_id)
    frame.pack(amount)
    client.connection.write_frame(frame)
  end

  def raw_reset(client, stream_id, code = Protocol::HTTP2::Error::CANCEL)
    frame = Protocol::HTTP2::ResetStreamFrame.new(stream_id)
    frame.pack(code)
    client.connection.write_frame(frame)
  end

  def request_headers(path, *extra)
    [[":method", "POST"], [":scheme", "http"], [":authority", "127.0.0.1"], [":path", path],
     ["content-type", "application/proto"], *extra]
  end

  def await_due(task, clock, due)
    wait_until(task, "a clock sleep due at #{due}; pending #{clock.sleepers.map(&:due)}") do
      clock.sleepers.any? { |sleeper| (sleeper.due - due).abs < 1e-9 }
    end
  end
end
