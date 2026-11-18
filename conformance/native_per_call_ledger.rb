# frozen_string_literal: true

require_relative "native_threaded_ledger"

module Conformance
  # The ThreadedTransport client inventories, through PerCallTransport on one
  # execution path. Each call's own cleanup was complete when it returned.
  class NativePerCallLedger < NativeThreadedLedger
    def initialize(path:, **)
      @path = path
      super(**)
    end

    private

    # Every inner transport closed and was released, with its owner thread
    # exited or its scope task finished, before the call returned.
    def validate_client_lifetimes!(events)
      events.select { |event| event.fetch("event") == "native_closed_lifetime" }.each do |event|
        verify(event.fetch("released"), "Per-call owner or scope outlived the call")
        verify(event.fetch("raw_closed"), "Per-call raw socket leaked")
        verify(event.fetch("paths") - [@path] == [], "Per-call ran on the wrong path")
      end
      NativeLifetimes.instance_method(:validate_client_lifetimes!).bind_call(self, events)
      validate_threads!(events)
    end

    # Inline, every scope runs in the adapter's reactor; threaded, as for
    # ThreadedTransport.
    def validate_threads!(events)
      return super unless @path == "inline"

      events.select { |event| event.fetch("event") == "native_scope_enter" }.each do |event|
        verify(event.fetch("thread") == "owner", "Inline #{event.fetch('phase')} scope ran outside the reactor")
      end
    end
  end
end
