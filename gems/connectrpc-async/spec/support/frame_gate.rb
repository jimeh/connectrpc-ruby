# frozen_string_literal: true

require "delegate"

class FrameGate < SimpleDelegator
  attr_accessor :armed
  attr_reader :started, :release

  def initialize(socket, frame_type: nil)
    super(socket)
    @started = Async::Queue.new
    @release = Async::Queue.new
    @armed = false
    @frame_type = frame_type
  end

  def write(buffer)
    return super unless @armed && (!@frame_type || buffer.to_s.getbyte(3) == @frame_type)

    @armed = false
    bytes = buffer.to_s
    __getobj__.write(bytes.byteslice(0, 5))
    @started.enqueue(true)
    @release.dequeue
    __getobj__.write(bytes.byteslice(5..))
    bytes.bytesize
  end
end
