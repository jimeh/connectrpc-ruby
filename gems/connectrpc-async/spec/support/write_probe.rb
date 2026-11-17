# frozen_string_literal: true

require "delegate"

# Marks while a socket write is in progress and counts finished writes. A
# write that does not block finishes without yielding, so another fiber sees
# `writing?` only while the write waits for the socket to become writable.
class WriteProbe < SimpleDelegator
  def writing? = !!@writing
  def written = @written || 0

  def write(...)
    @writing = true
    result = super
    @written = written + 1
    result
  ensure
    @writing = false
  end
end
