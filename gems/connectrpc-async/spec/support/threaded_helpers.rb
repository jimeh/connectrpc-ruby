# frozen_string_literal: true

# Peers run on their own thread and reactor, so examples can call
# ThreadedTransport from plain threads that block.
module ThreadedHelpers
  THREADED = ConnectRPC::Async::ThreadedTransport

  # Counts raw accepts at the hosting SPI boundary without changing serving.
  class CountingHost < ConnectRPC::Async::Host
    class Session < ConnectRPC::Async::ServerSession
      def initialize(accepts:, **)
        @accepts = accepts
        super(**)
      end

      def accept(raw)
        @accepts << true
        super
      end
    end

    attr_reader :accepts

    def initialize(**)
      @accepts = []
      super
    end

    private

    def build_session(**) = Session.new(accepts: @accepts, **)
  end

  # A backoff clock the test thread fires; the owner's timer blocks on it.
  class ThreadClock
    attr_reader :now

    def initialize
      @now = 0.0
      @requested = Thread::Queue.new
      @release = Thread::Queue.new
    end

    def sleep(seconds)
      @requested << seconds
      @release.pop
      @now += seconds
    end

    def next_delay = @requested.pop(timeout: 5) || raise("No backoff delay was requested")
    def fire = @release << true
  end

  # Runs a block's peer inside its own Sync reactor thread until stopped.
  class PeerThread
    attr_reader :peer

    def initialize(&build)
      ready = Thread::Queue.new
      @stop = Thread::Queue.new
      @thread = Thread.new do
        Sync do |task|
          peer, close = build.call(task)
          ready << [peer]
          @stop.pop
          close.call
        end
      rescue Exception => e # rubocop:disable Lint/RescueException -- report setup failures to the example
        ready << [e]
        raise
      end
      @thread.report_on_exception = false
      @peer, = ready.pop
      raise @peer if @peer.is_a?(Exception)
    end

    def stop
      @stop << true
      raise "Peer thread did not stop" unless @thread.join(10)
    end
  end

  def threaded_host(**)
    PeerThread.new do
      host = CountingHost.new(**).start
      [host, -> { host.close(grace: 0) }]
    end
  end

  # A recording raw HTTP/2 peer whose handler runs the given block.
  def threaded_recording_peer(settings: {}, &handler)
    PeerThread.new do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first, settings: settings, &handler)
      peer.define_singleton_method(:port) { bound.sockets.first.local_address.ip_port }
      [peer, lambda {
        peer.stop
        bound.close
      }]
    end
  end

  # Host TLS, origin scheme and client options for :h2c, :tls or :mtls.
  def threaded_mode(mode, ca_file)
    tls = (host_tls(anchors: mode == :mtls ? [tls_material.fetch(:client_ca)] : nil) unless mode == :h2c)
    options = mode == :h2c ? {} : { ca_file: ca_file }
    options.merge!(host_tls(tls_material.fetch(:identity)).except(:client_ca)) if mode == :mtls
    [tls, mode == :h2c ? "http" : "https", options]
  end

  def threaded(**) = THREADED.new(**)
  def engine_of(transport) = transport.instance_variable_get(:@engine)
  def admitted(transport) = transport.instance_variable_get(:@admission)&.count || 0
  def owner_threads(port) = Thread.list.select { |thread| thread.name == "connectrpc-async 127.0.0.1:#{port}" }

  # Holds the owner's whole thread in native-style blocking until the gate opens.
  def hold_owner(transport)
    held = Thread::Queue.new
    gate = Thread::Queue.new
    engine_of(transport).run_on_owner(lambda {
      held << true
      Fiber.blocking { gate.pop }
    })
    held.pop
    gate
  end

  def on_owner(transport)
    result = Thread::Queue.new
    engine_of(transport).run_on_owner(lambda {
      result << begin
        [:ok, yield]
      rescue StandardError => e
        [:error, e]
      end
    })
    result.pop
  end

  def install_thread_clock(transport)
    clock = ThreadClock.new
    on_owner(transport) do
      manager = engine_of(transport).inner.instance_variable_get(:@manager)
      manager.instance_variable_set(:@clock, clock)
      manager.instance_variable_set(:@random, ConnectionPeer::SequenceRandom.new)
    end
    clock
  end

  # Polls an asserted condition under a bounded deadline, for state with no
  # direct signal, and reports the last observation on timeout.
  def eventually(message, timeout: 5)
    expires = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until (last = yield)
      if Process.clock_gettime(Process::CLOCK_MONOTONIC) > expires
        raise "Timed out waiting for #{message}; last observed: #{last.inspect}"
      end

      sleep 0.005 # Poll interval only; the deadline bounds the wait.
    end
    last
  end

  # Pops queued observations until one matches, within a bounded deadline.
  def pop_until(queue, message, timeout: 5)
    expires = monotonic + timeout
    last = nil
    loop do
      remaining = expires - monotonic
      raise "Timed out waiting for #{message}; last observed: #{last.inspect}" unless remaining.positive?

      last = queue.pop(timeout: remaining)
      return last if !last.nil? && yield(last)
    end
  end

  def blocked?(thread) = thread.status == "sleep" || thread.stop?

  def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  def capture_error
    yield
    nil
  rescue Exception => e # rubocop:disable Lint/RescueException -- examples assert the exact escaping exception
    e
  end
end
