# frozen_string_literal: true

module TeardownHelpers
  # The fixed warning each reported teardown failure writes, in order.
  def teardown_warnings(*classes) = classes.map { |error| "ConnectRPC teardown error: #{error}\n" }.join

  # Fails `raw`'s first close with an error that is not an IOError or
  # SystemCallError, leaving it open, and records whether `task` had finished
  # by then. Later closes close it.
  def fail_first_raw_close(raw, task)
    observed = []
    raw.define_singleton_method(:close) do
      next super() unless observed.empty?

      observed << task.finished?
      raise "raw close failed"
    end
    observed
  end
end
