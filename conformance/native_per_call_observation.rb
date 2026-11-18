# frozen_string_literal: true

require_relative "native_threaded_observation"

module Conformance
  # Observes the private inner transports PerCallTransport builds for the
  # adapter's one active request. The adapter runs requests one at a time, so
  # the active observation is process-wide.
  module PerCallInnerObservation
    PER_CALL = ConnectRPC::Async::PerCallTransport
    OWNER = ConnectRPC::Async::ThreadedTransport.const_get(:OWNER)

    class << self
      attr_reader :observation

      def begin(observation)
        @lock = Mutex.new
        @inners = []
        @observation = observation
      end

      def record(inner) = @lock.synchronize { @inners << inner }
      def inners = @lock.synchronize { @inners.dup }

      def finish
        @observation = nil
      end
    end

    # ThreadedInnerObservation supplies the scope wrappers and closed facts;
    # this sets their observation for per-call inners.
    def initialize(**)
      super
      front = Thread.current.thread_variable_get(OWNER)
      return unless is_a?(PER_CALL.const_get(:Inline)) || front.is_a?(PER_CALL.const_get(:Threaded))

      @per_call_front = front
      @observation = PerCallInnerObservation.observation
      PerCallInnerObservation.record(self) if @observation
    end

    # Read when the transport returns, without joining anything: every owner
    # thread the front started must already have exited and the scope task
    # finished.
    def boundary_facts
      facts = closed_facts
      if @per_call_front
        engines = @per_call_front.instance_variable_get(:@engines)
        facts.merge(path: "threaded", released: engines.none? { |engine| engine.thread.alive? })
      else
        facts.merge(path: "inline", released: @scope.finished? && pending(@scope).empty?)
      end
    end

    private

    def pending(task)
      task.children.to_a.flat_map { |child| [*(child unless child.finished?), *pending(child)] }
    end
  end
  inline = ConnectRPC::Async::PerCallTransport.const_get(:Inline)
  inline.prepend(ThreadedInnerObservation)
  [inline, ConnectRPC::Async::ThreadedTransport.const_get(:Inner)].each do |inner|
    inner.prepend(PerCallInnerObservation)
  end

  class ObservedPerCallTransport < ConnectRPC::Async::PerCallTransport
    def initialize(observation:, **)
      @observation = observation
      # The transport freezes itself; this holder keeps the exit facts.
      @exit_facts = []
      super(**)
    end

    def call(...)
      @observation.observe(:transport) { at_exit_facts { super } }
    end

    def open_stream(...)
      @observation.observe(:transport) { at_exit_facts { super } }
    end

    def open_grpc_stream(...)
      @observation.observe(:transport) { at_exit_facts { super } }
    end

    # The facts captured as the adapter's one transport call returned. A case
    # that never reached the transport has none, so they are read now.
    def closed_facts = @exit_facts.last || boundary_facts

    private

    def at_exit_facts
      yield
    ensure
      @exit_facts << boundary_facts
    end

    # Every inner transport the call built, as it stood at that moment. A call
    # rejected before construction built none.
    def boundary_facts
      inners = PerCallInnerObservation.inners.map(&:boundary_facts)
      { **all_of(inners, %i[tasks_finished streams_closed resources_finished lifetime_children_finished released]),
        **%i[calls acquiring].to_h { |fact| [fact, inners.sum { |inner| inner.fetch(fact) }] },
        state: closed?(inners) ? :closed : :open, raw_closed: inners.none? { |inner| inner[:raw_closed] == false },
        inners: inners.size, paths: inners.map { |inner| inner.fetch(:path) }.uniq, socket_interruption: nil }
    end

    def all_of(inners, facts) = facts.to_h { |fact| [fact, inners.all? { |inner| inner.fetch(fact) }] }
    def closed?(inners) = inners.all? { |inner| inner.fetch(:state) == :closed }
  end
end
