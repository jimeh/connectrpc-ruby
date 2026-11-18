# frozen_string_literal: true

module FlushHold
  # Holds the next frame the block selects once it is flushed, while its
  # writer still holds the frame guard. The reader is not held: its next
  # buffer refill finds no pending output to flush.
  def hold_after_flush(connection, &select)
    gate = Data.define(:started, :release).new(Async::Queue.new, Async::Queue.new)
    framer = connection.framer
    state = { armed: true, holding: false }
    framer.define_singleton_method(:write_frame) do |frame|
      state[:holding] = true if state[:armed] && select.call(frame)
      super(frame)
    end
    framer.define_singleton_method(:flush) do
      super()
      next unless state[:holding]

      state[:holding] = state[:armed] = false
      gate.started.enqueue(true)
      gate.release.dequeue
    end
    gate
  end
end
