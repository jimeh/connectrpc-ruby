# frozen_string_literal: true

# Minimal blocking HTTP/1.1 message reads for peers on plain threads.
module HTTP1Wire
  # One request or response head, or nil when the peer ends its connection
  # before sending any of it.
  def self.read_head(io)
    head = +""
    until head.end_with?("\r\n\r\n")
      byte = io.read(1)
      return if byte.nil? && head.empty?
      raise EOFError, "Truncated HTTP/1 head" if byte.nil?

      head << byte
    end
    head
  end

  # The body the head frames: Content-Length, chunked (decoded) or none.
  def self.read_body(io, head)
    if (length = head[/^content-length: *([0-9]+)\r$/i, 1])
      io.read(Integer(length)).to_s
    elsif head.match?(/^transfer-encoding: *chunked\r$/i)
      body = +""
      while (size = Integer(line(io).strip, 16)).positive?
        body << io.read(size).to_s
        io.read(2)
      end
      nil until line(io) == "\r\n"
      body
    else
      ""
    end
  end

  def self.line(io) = io.gets("\r\n") || raise(EOFError, "Truncated HTTP/1 chunked body")
end
