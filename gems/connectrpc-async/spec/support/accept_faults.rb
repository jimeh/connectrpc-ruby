# frozen_string_literal: true

# Wrap only each actual listening socket's accept operation. Host, session,
# transport and generated clients continue to run normally over loopback.
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

      listener = event.binding.local_variable_get(:listener)
      index = @listeners.length
      @listeners << listener
      @tasks << Async::Task.current
      original = listener.method(:accept)
      faults = self
      listener.define_singleton_method(:accept) { |*args, **kwargs| faults.accept(index, original, *args, **kwargs) }
    end
  end

  def enable(&) = @trace.enable(&)

  def accept(index, original, *, **)
    @attempts[index] += 1
    @operation.call(index, @attempts[index])
    pair = original.call(*, **)
    @accepted[index] += 1
    pair
  ensure
    @after&.call
  end
end
