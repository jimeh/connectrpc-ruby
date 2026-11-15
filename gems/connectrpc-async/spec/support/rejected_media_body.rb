# frozen_string_literal: true

# Counts response DATA the peer actually hands to its HTTP/2 output and
# reports how the output ended; the trailer is only added after the full body.
class RejectedMediaBody < Protocol::HTTP::Body::Readable
  attr_reader :sent

  def initialize(headers, total, outcome)
    super()
    @headers = headers
    @remaining = total
    @outcome = outcome
    @sent = 0
  end

  def read
    if @remaining.zero?
      @headers.add("grpc-status", "13")
      return
    end
    chunk = "x" * [@remaining, 16_384].min
    @remaining -= chunk.bytesize
    @sent += chunk.bytesize
    chunk
  end

  def close(error = nil)
    @outcome.push([@sent, error])
    super
  end
end
