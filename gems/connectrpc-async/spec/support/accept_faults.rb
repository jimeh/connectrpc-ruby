# frozen_string_literal: true

# Wrap only each actual listening socket's accept operations. Host, session,
# transport and generated clients continue to run normally over loopback.
#
# A listener attempt waits for readability, then takes the peer with
# accept_nonblock. The operation runs at the start of each attempt, before
# readiness; `after` runs when each readiness wait ends, including by
# cancellation.
class AcceptFaults
  attr_reader :listeners, :tasks, :attempts, :accepted

  def initialize(after: nil, &operation)
    @operation = operation
    @after = after
    @listeners = []
    @tasks = []
    @attempts = Hash.new(0)
    @accepted = Hash.new(0)
    @trace = TracePoint.new(:call) do |event|
      next unless event.defined_class == ConnectRPC::Async::Host && event.method_id == :accept_loop

      instrument(event.binding.local_variable_get(:listener))
    end
  end

  def enable(&) = @trace.enable(&)

  def instrument(listener)
    index = @listeners.length
    @listeners << listener
    @tasks << Async::Task.current
    wait = listener.method(:wait_readable)
    accept = listener.method(:accept_nonblock)
    faults = self
    listener.define_singleton_method(:wait_readable) { |*args| faults.wait(index, wait, *args) }
    listener.define_singleton_method(:accept_nonblock) do |*args, **kwargs|
      faults.accept(index, accept, *args, **kwargs)
    end
  end

  def wait(index, original, *)
    @attempts[index] += 1
    @operation.call(index, @attempts[index])
    original.call(*)
  ensure
    @after&.call
  end

  def accept(index, original, *, **)
    pair = original.call(*, **)
    @accepted[index] += 1 unless pair == :wait_readable
    pair
  end
end
