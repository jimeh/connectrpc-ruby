# frozen_string_literal: true

module Soak
  # An h2c client that writes its preface, a large flow-control window and one
  # Connect streaming request, and never reads its socket.
  class RawH2
    FIONREAD = 0x541B # Linux

    attr_reader :socket

    def self.open(port, window:) = new(port, window)

    def initialize(port, window)
      @socket = IO::Endpoint.tcp("127.0.0.1", port).connect
      @framer = Protocol::HTTP2::Framer.new(IO::Stream(@socket))
      @connection = Protocol::HTTP2::Client.new(@framer)
      @framer.write_connection_preface
      @connection.send_settings([[Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE, window]])
      update = Protocol::HTTP2::WindowUpdateFrame.new(0)
      update.pack(window - Protocol::HTTP2::Window::DEFAULT_CAPACITY)
      @connection.write_frame(update)
    end

    def request(path, ledger_id:, action:, count:)
      headers = [[":method", "POST"], [":scheme", "http"], [":authority", "127.0.0.1"], [":path", path],
                 ["content-type", "application/connect+proto"], [Ledger::ID, ledger_id], [Topology::OP, action],
                 [Topology::COUNT, count.to_s]]
      frame = Protocol::HTTP2::HeadersFrame.new(1, Protocol::HTTP2::END_HEADERS)
      frame.pack(@connection.encode_headers(headers))
      @connection.write_frame(frame)
      bytes = Topology::MESSAGE.encode(Topology.message(1))
      data = Protocol::HTTP2::DataFrame.new(1, Protocol::HTTP2::END_STREAM)
      data.pack([0, bytes.bytesize].pack("CN") + bytes)
      @connection.write_frame(data)
    end

    # Bytes the server wrote that this client has not read.
    def unread
      buffer = [0].pack("i")
      @socket.ioctl(FIONREAD, buffer)
      buffer.unpack1("i")
    end

    def close = Soak.close_io(@socket)
  end
end
