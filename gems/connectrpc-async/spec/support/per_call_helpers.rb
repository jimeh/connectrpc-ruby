# frozen_string_literal: true

# PerCallTransport examples. Calls run on plain threads (the threaded path) or
# inside `reactor` (the inline path); peers run on their own threads.
module PerCallHelpers
  PER_CALL = ConnectRPC::Async::PerCallTransport
  PATHS = %i[threaded inline].freeze

  # Observes each call's own resources at the moment PerCallTransport's
  # execution method returns, on the caller's thread: the owner thread, the
  # scope task and its subtree, and every socket the call's inner transport
  # dialed, including a second connection for a resend.
  class Boundaries
    def initialize
      @lock = Mutex.new
      @dials = {}.compare_by_identity
      @observations = Thread::Queue.new
      @traces = [
        trace(ConnectRPC::Async::Transport.instance_method(:dial)) { |point| record_dial(point) },
        trace(PER_CALL.instance_method(:threaded)) { |point| @observations << observe_threaded(point) },
        trace(PER_CALL.instance_method(:inline)) { |point| @observations << observe_inline(point) }
      ]
    end

    def stop = @traces.each(&:disable)

    # Every observation recorded so far, in boundary order.
    def take
      taken = []
      taken << @observations.pop until @observations.empty?
      taken
    end

    def dialed = @lock.synchronize { @dials.values.flatten }

    private

    def trace(method, &)
      TracePoint.new(:return, &).tap { |point| point.enable(target: method) }
    end

    def record_dial(point)
      socket = point.return_value
      return unless socket

      @lock.synchronize { (@dials[point.self] ||= []) << [socket, Thread.current] }
    end

    def dials_of(inner) = inner ? @lock.synchronize { (@dials[inner] || []).dup } : []

    # Every engine the front started, including one a failed start replaced.
    # A front whose owner never started has no engine, owner or dials.
    def observe_threaded(point)
      engines = point.binding.local_variable_get(:inner)&.instance_variable_get(:@engines) || []
      owners = engines.map(&:thread)
      dials = engines.flat_map { |engine| dials_of(engine.inner) }
      sockets(dials, owners.last).merge(path: :threaded, owner: owners.last, owners: owners,
                                        owner_alive: owners.any?(&:alive?))
    end

    def observe_inline(point)
      scope = point.binding.local_variable_get(:scope)
      attached = Async::Task.current.children.to_a.any? { |child| child.equal?(scope) }
      sockets(dials_of(point.binding.local_variable_get(:inner)), Thread.current)
        .merge(path: :inline, scope_finished: scope.finished?, attached: attached, live: unfinished(scope))
    end

    # The call's dialed sockets, those still open, and dials made on any
    # thread other than the expected one.
    def sockets(dials, thread)
      { sockets: dials.map(&:first), open: dials.map(&:first).reject(&:closed?),
        foreign_dialers: dials.map(&:last).reject { |dialer| dialer.equal?(thread) } }
    end

    def unfinished(task)
      task.children.to_a.flat_map { |child| [*(child unless child.finished?), *unfinished(child)] }
    end
  end

  # One call's boundary left nothing of its own behind.
  def expect_released(observation, dials: 1)
    expect(observation.fetch(:sockets).size).to eq(dials)
    # Inspected, because RSpec's failure message would read a leaked socket.
    expect([observation.fetch(:open).map(&:inspect), observation.fetch(:foreign_dialers)]).to eq([[], []])
    if observation.fetch(:path) == :threaded
      expect(observation.fetch(:owner_alive)).to be(false)
    else
      expect(observation.values_at(:scope_finished, :attached, :live)).to eq([true, false, []])
    end
  end

  # Never closes an accepted connection itself, so the peer sends no FIN
  # after the client's GOAWAY and EOF; stopping the peer closes its sockets.
  # `close_requests` lists each connection whose reader ended, which is where
  # the peer would have closed it.
  class FinWithholdingServer < ConnectionPeer::RecordingServer
    attr_reader :close_requests

    def initialize(...)
      @close_requests = []
      super
    end

    def stop
      @stopping = true
      super
    end

    private

    def instrument(server, index)
      peer = self
      super.tap do |connection|
        connection.define_singleton_method(:close) do |*arguments|
          peer.close_requests << index unless peer.close_requests.include?(index)
          super(*arguments) if peer.instance_variable_get(:@stopping)
        end
      end
    end
  end

  # A raw peer that also records each RST_STREAM as [connection, stream id,
  # error code].
  class ResetRecordingServer < ConnectionPeer::RecordingServer
    def resets = (@resets ||= [])

    def record(index, frame)
      resets << [index, frame.stream_id, frame.unpack] if frame.is_a?(Protocol::HTTP2::ResetStreamFrame)
      super
    end
  end

  def reset_recording_peer(&)
    ThreadedHelpers::PeerThread.new do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = ResetRecordingServer.new(task, bound.sockets.first, &)
      peer.define_singleton_method(:port) { bound.sockets.first.local_address.ip_port }
      [peer, lambda {
        peer.stop
        bound.close
      }]
    end
  end

  # A Host that never sends FIN: its accepted sockets ignore shutdown and
  # close until the Host itself closes.
  class FinWithholdingHost < ConnectRPC::Async::Host
    class Session < ConnectRPC::Async::ServerSession
      def initialize(held:, **)
        @held = held
        super(**)
      end

      def accept(raw)
        @held << raw
        raw.define_singleton_method(:shutdown) { |*| 0 }
        raw.define_singleton_method(:close) { nil }
        super
      end
    end

    def initialize(**)
      @held = []
      super
    end

    def close(**)
      super
    ensure
      @held.each { |raw| IO.instance_method(:close).bind_call(raw) }
    end

    private

    def build_session(**) = Session.new(held: @held, **)
  end

  def withholding_host(server:)
    ThreadedHelpers::PeerThread.new do
      host = FinWithholdingHost.new(server: server).start
      [host, -> { host.close(grace: 0) }]
    end
  end

  def withholding_peer(&)
    ThreadedHelpers::PeerThread.new do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = FinWithholdingServer.new(task, bound.sockets.first, &)
      peer.define_singleton_method(:port) { bound.sockets.first.local_address.ip_port }
      [peer, lambda {
        peer.stop
        bound.close
      }]
    end
  end

  def with_boundaries
    boundaries = Boundaries.new
    yield boundaries
  ensure
    boundaries&.stop
  end

  # Runs the block on the given execution path and returns its value or the
  # exception that escaped it.
  def on_path(path, &)
    return value_within(Thread.new { capture_error_or_value(&) }, "a per-call caller") if path == :threaded

    reactor { capture_error_or_value(&) }
  end

  def capture_error_or_value
    yield
  rescue Exception => e # rubocop:disable Lint/RescueException -- examples assert the exact escaping exception
    e
  end

  def per_call(**) = PER_CALL.new(**)
end
