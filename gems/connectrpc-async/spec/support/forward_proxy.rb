# frozen_string_literal: true

require_relative "connect_proxy"
require_relative "http1_wire"

# A forward proxy for http:// origins that answers absolute-form HTTP/1.1
# requests itself, as a proxy that relays them appears to its client. Every
# request head is recorded in `heads`. The block builds each reply from the
# head, the body, the request's number on its connection and the connection's
# number; a nil reply closes the connection without answering, and
# `[reply, :close]` closes it after answering, ending a close-delimited body.
# A client that ends its connection is reported in `closed`.
class ForwardProxy < ConnectProxy
  def initialize(&respond)
    @connections = 0
    super { |client, head, _proxy| answer(client, head, respond) }
  end

  private

  def answer(client, head, respond)
    connection = @lock.synchronize { @connections += 1 }
    count = 0
    loop do
      count += 1
      reply, after = respond.call(head, HTTP1Wire.read_body(client, head), count, connection)
      return client.close unless reply

      client.write(reply)
      return client.close if after == :close

      head = HTTP1Wire.read_head(client)
      break unless head

      @heads << head
    end
    @closed << true unless @stopping
  end
end
