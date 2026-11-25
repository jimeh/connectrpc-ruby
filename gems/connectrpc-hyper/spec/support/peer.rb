# frozen_string_literal: true

# The disposable Go peer (gems/connectrpc-hyper/interop), one per spec run. It
# prints its loopback origins, then one event line per server-side fact;
# stdin carries control commands. Waits are bounded and report what the peer
# last said on timeout.
class Peer
  BINARY = File.join(ROOT, "vendor/hyper/peer#{RbConfig::CONFIG.fetch('EXEEXT')}")

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

  # The log position after a barrier. An earlier example's events (a request
  # or an accept the client already has the response to) can still be in the
  # peer's output when the next example starts; without the barrier they
  # would land after the position and count as this example's.
  def mark
    sync
    size
  end

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

  # Waits until the first `count` connections `tag` accepted at or after
  # `from` have closed. The peer logs a close only after the client's close
  # returned, so an earlier example's close may land after `from`; matching
  # this example's own connections by address keeps it from standing in.
  def wait_closed(tag, from:, count: 1, timeout: 10)
    addresses = wait_count(/\A#{tag} accept /, count, from: from, timeout: timeout).first(count).map { it.split.last }
    addresses.each do |address|
      wait_event(/\A#{tag} conn-closed #{Regexp.escape(address)}\z/, from: from, timeout: timeout)
    end
  end

  def count(pattern, from: 0) = since(from).grep(pattern).size

  # A key for a held server stream (request value `hold:<key>`), unique in
  # this peer's log. The peer logs a hold's end only once it sees the reset,
  # which can come after its example ended or failed, so each example waits
  # for its own key's events and never takes an earlier example's.
  def hold_key = "h#{@hold_keys = (@hold_keys || 0) + 1}"

  def hold_sent(key, tag: "grpc") = /\A#{tag} ss-hold-sent #{Regexp.escape(key)}\z/

  def hold_done(key, tag: "grpc", reason: /.+/) = /\A#{tag} ss-hold-done #{Regexp.escape(key)} #{reason}\z/

  # Runs a control command and waits for the peer to confirm it ran.
  def command(line)
    from = size
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

  private

  def size = @lock.synchronize { @log.size }
end
