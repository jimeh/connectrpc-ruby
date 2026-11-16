# frozen_string_literal: true

require "delegate"

class FrameGate < SimpleDelegator
  attr_accessor :armed
  attr_reader :started, :release, :writer

  def initialize(socket, frame_type: nil, stream_id: nil)
    super(socket)
    @started = Async::Queue.new
    @release = Async::Queue.new
    @armed = false
    @frame_type = frame_type
    @stream_id = stream_id
  end

  def write(buffer)
    return super unless @armed && matches?(buffer.to_s)

    @armed = false
    @writer = Async::Task.current
    bytes = buffer.to_s
    __getobj__.write(bytes.byteslice(0, 5))
    @started.enqueue(true)
    @release.dequeue
    __getobj__.write(bytes.byteslice(5..))
    @written = true
    bytes.bytesize
  end

  # Whether the held write reached the socket in full rather than being cut.
  def written? = !!@written

  private

  def matches?(bytes)
    (!@frame_type || bytes.getbyte(3) == @frame_type) &&
      (!@stream_id || (bytes.byteslice(5, 4).unpack1("N") & 0x7fff_ffff) == @stream_id)
  end
end
