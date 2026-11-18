# frozen_string_literal: true

# The disposable Go peer (gems/connectrpc-hyper/interop), one per spec run. It
# prints its loopback origins, then one event line per server-side fact;
# stdin carries control commands. Waits are bounded and report what the peer
# last said on timeout.
class Peer
  BINARY = File.join(ROOT, "vendor/hyper/peer")

  class << self
    attr_reader :instance
  end

  def self.start
    @start_lock ||= Mutex.new
    @start_lock.synchronize { @instance ||= new }
  end

  attr_reader :certs

  def initialize
    raise "Build the peer first: mise run hyper:peer" unless File.executable?(BINARY)

    @certs = Dir.mktmpdir("connectrpc-hyper-peer-")
    @stdin, @stdout, @wait = Open3.popen2(BINARY, @certs)
    @origins = JSON.parse(@stdout.gets || raise("peer exited before printing origins"))
    @lock = Mutex.new
    @changed = ConditionVariable.new
    @log = []
    @reader = Thread.new do
      while (line = @stdout.gets)
        event = line.chomp.delete_prefix("EVENT ")
        @lock.synchronize do
          @log << event
          @changed.broadcast
        end
      end
    end
  end

  def [](name) = @origins.fetch(name.to_s)

  def log = @lock.synchronize { @log.dup }
  def mark = @lock.synchronize { @log.size }

  # Events since `mark`, after a barrier: the peer reports every event that
  # happened before this call ahead of the barrier's confirmation, even when
  # its output lags behind a response the client already has.
  def since(mark)
    sync
    @lock.synchronize { @log.drop(mark) }
  end

  def sync
    @syncs = (@syncs || 0) + 1
    command("sync #{@syncs}")
  end

  # The first event at or after `from` matching `pattern`.
  def wait_event(pattern, from: 0, timeout: 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    @lock.synchronize do
      loop do
        found = @log.drop(from).find { |event| event.match?(pattern) }
        return found if found

        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise "Timed out waiting for #{pattern.inspect}; last events: #{@log.last(10).inspect}" if remaining <= 0

        @changed.wait(@lock, remaining)
      end
    end
  end

  def wait_count(pattern, count, from: 0, timeout: 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    @lock.synchronize do
      loop do
        found = @log.drop(from).grep(pattern)
        return found if found.size >= count

        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        if remaining <= 0
          raise "Timed out waiting for #{count} x #{pattern.inspect}; saw #{found.size}; last: #{@log.last(10).inspect}"
        end

        @changed.wait(@lock, remaining)
      end
    end
  end

  def count(pattern, from: 0) = since(from).grep(pattern).size

  # Runs a control command and waits for the peer to confirm it ran.
  def command(line)
    from = mark
    @stdin.puts(line)
    @stdin.flush
    wait_event(/\Acontrol done #{Regexp.escape(line)}\z/, from: from)
  end

  def close
    @stdin.close
    @wait.value
    @reader.join
    FileUtils.rm_rf(@certs)
  end
end
