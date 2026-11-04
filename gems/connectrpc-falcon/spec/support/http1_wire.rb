# frozen_string_literal: true

# Raw HTTP peer: no Ruby streaming client capability is implied.
class ConnectH1Wire
  attr_reader :socket, :raw

  def initialize(origin, context: nil)
    @raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
    @socket = @raw
    return unless context

    @socket = OpenSSL::SSL::SSLSocket.new(@raw, context)
    @socket.hostname = "localhost"
    @socket.connect
    @socket.post_connection_check("localhost")
  end

  def write(bytes) = @socket.write(bytes)

  def head
    status = @socket.gets("\r\n")
    raise EOFError, "No response head" unless status

    fields = Hash.new { |hash, key| hash[key] = [] }
    while (line = @socket.gets("\r\n")) != "\r\n"
      raise EOFError, "Truncated response head" unless line

      key, value = line.strip.split(":", 2)
      fields[key.downcase] << value.strip
    end
    [Integer(status.split[1]), fields]
  end

  def chunk
    line = @socket.gets("\r\n")
    raise EOFError, "Missing HTTP terminal chunk" unless line

    size = Integer(line.strip, 16)
    bytes = @socket.read(size)
    raise EOFError, "Truncated HTTP chunk" unless bytes && bytes.bytesize == size
    raise "Bad HTTP chunk terminator" unless @socket.read(2) == "\r\n"

    size.zero? ? nil : bytes
  end

  def body
    result = +"".b
    while (bytes = chunk)
      result << bytes
    end
    result
  end

  def close
    AsyncHelpers::INTERNAL.raw_close(@raw)
  end
end
