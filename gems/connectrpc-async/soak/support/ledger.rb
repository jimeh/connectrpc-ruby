# frozen_string_literal: true

module Soak
  # Execution and outcome ledger for one round. Handlers record each entry by
  # its `x-soak-id`; callers record each logical call's outcome. Every id runs
  # at most once and every successful id ran exactly once. Thread-safe: hosts,
  # owner threads and caller threads all write to it.
  class Ledger
    ID = "x-soak-id"

    attr_reader :entered

    def initialize
      @mutex = Mutex.new
      @sequence = 0
      @entered = Thread::Queue.new
      reset
    end

    def reset
      @mutex.synchronize do
        @entries = Hash.new(0)
        @ends = {}
        @outcomes = {}
        @expected = {}
        @headers = Hash.new(0)
      end
      @entered.clear
    end

    def next_id(prefix = "call") = @mutex.synchronize { "#{prefix}-#{@sequence += 1}" }
    def metadata(id, **extra) = { ID => id, **extra.transform_keys(&:to_s) }

    def enter(id)
      @mutex.synchronize { @entries[id] += 1 }
      @entered << id
    end

    # The handler's view of how its execution ended.
    def finish(id, reason) = @mutex.synchronize { @ends[id] = reason }

    def outcome(id, value, expect: nil)
      @mutex.synchronize do
        @outcomes[id] = value
        @expected[id] = Array(expect) if expect
      end
      value
    end

    # Request HEADERS a raw peer read per id.
    def headers(id) = @mutex.synchronize { @headers[id] += 1 }
    def header_counts = @mutex.synchronize { @headers.dup }

    def entries(id) = @mutex.synchronize { @entries[id] }
    def ended(id) = @mutex.synchronize { @ends[id] }
    def outcome_of(id) = @mutex.synchronize { @outcomes[id] }

    # Pops entered ids until every wanted one arrived, within a bounded deadline.
    def await_entered(ids, timeout: 10)
      waiting = Array(ids).dup
      expires = Soak.now + timeout
      until waiting.empty?
        remaining = expires - Soak.now
        id = @entered.pop(timeout: [remaining, 0].max) if remaining.positive?
        raise "Timed out waiting for handlers #{waiting.inspect} to start" unless id

        waiting.delete(id)
      end
    end

    def summary
      @mutex.synchronize do
        @outcomes.values.map { |value| value.is_a?(Symbol) ? value.to_s : value }.tally
      end
    end

    # At-most-once execution, exactly-once success, and each recorded outcome
    # within its allowlist.
    def problems
      @mutex.synchronize do
        problems = @entries.filter_map { |id, count| "#{id} executed #{count} times" if count > 1 }
        @outcomes.each do |id, value|
          problems << "#{id} succeeded but executed #{@entries[id]} times" if value == :ok && @entries[id] != 1
          allowed = @expected[id]
          problems << "#{id} outcome #{value.inspect} not in #{allowed.inspect}" if allowed && !allowed.include?(value)
        end
        problems
      end
    end
  end

  # Cross-thread handler gates. A handler waits until its id (or every id) is
  # opened, or until its own context is cancelled.
  class Gates
    def initialize
      @mutex = Mutex.new
      @queues = {}
      @all = false
    end

    def wait(id, context = nil)
      queue = queue(id)
      registration = context&.on_cancel { queue.close }
      queue.pop
    ensure
      registration&.unregister
    end

    def open(id) = queue(id).close

    def open_all
      @mutex.synchronize do
        @all = true
        @queues.each_value(&:close)
      end
    end

    def reset
      @mutex.synchronize do
        @all = false
        @queues.clear
      end
    end

    private

    def queue(id)
      @mutex.synchronize do
        @queues[id] ||= Thread::Queue.new.tap { |queue| queue.close if @all }
      end
    end
  end
end
