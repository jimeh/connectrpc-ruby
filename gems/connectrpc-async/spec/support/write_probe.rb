# frozen_string_literal: true

require "delegate"

# Marks while a socket write is in progress. A write that does not block
# finishes without yielding, so another fiber sees `writing?` only while
# the write waits for the socket to become writable.
class WriteProbe < SimpleDelegator
  def writing? = !!@writing

  def write(...)
    @writing = true
    super
  ensure
    @writing = false
  end
end
