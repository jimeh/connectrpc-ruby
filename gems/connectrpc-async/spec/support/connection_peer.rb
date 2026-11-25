# frozen_string_literal: true

require_relative "ingress_wire"

module ConnectionPeer
  # Records every manager sleep and holds it until the example releases it.
  # `fire` releases the oldest pending sleep, or the next one if none waits,
  # and that sleep alone adds its seconds to `now`; `sleeper` addresses one
  # pending sleep, so concurrent backoff and drain timers can be released
  # individually. `advance` instead moves `now` once and releases every sleep
  # due by then, so concurrent keepalive timers share one timeline.
  class FakeClock
    class Sleeper
      attr_reader :seconds, :due

      def initialize(seconds, due)
        @seconds = seconds
        @due = due
        @release = Async::Promise.new
      end

      def fire = @release.resolve(:fired)
      def advance = @release.resolve(:advanced)
      def fired? = @release.resolved?
      def wait = @release.wait
    end

    attr_reader :delays, :now

    def initialize
      @now = 0.0
      @delays = []
      @pending = []
      @credits = 0
      @requested = Async::Queue.new
    end

    def sleep(seconds)
      sleeper = Sleeper.new(seconds, @now + seconds)
      @delays << seconds
      @pending << sleeper
      if @credits.positive?
        @credits -= 1
        sleeper.fire
      end
      @requested.enqueue(seconds)
      @now += seconds if sleeper.wait == :fired
    ensure
      @pending.delete(sleeper)
    end

    def advance(seconds)
      @now += seconds
      sleepers.select { |sleeper| sleeper.due <= @now }.each(&:advance)
    end

    def next_delay = @requested.dequeue
    def sleepers = @pending.reject(&:fired?)
    def sleeper(seconds) = sleepers.find { |entry| entry.seconds == seconds }

    def fire
      sleeper = sleepers.first
      sleeper ? sleeper.fire : @credits += 1
    end
  end

  # Open client sockets, including a setup attempt's. Only setup opens a
  # socket, so sampling at each raw socket assignment records the true peak.
  class SocketMeter
    attr_reader :peak

    def initialize(transport)
      @resources = transport.instance_variable_get(:@resources)
      @peak = 0
      manager = transport.instance_variable_get(:@manager)
      setup = manager.instance_variable_get(:@setup)
      meter = self
      manager.instance_variable_set(:@setup, lambda do |resource|
        resource.define_singleton_method(:raw=) do |raw|
          super(raw)
          meter.observe
        end
        setup.call(resource)
      end)
    end

    # A failed raw socket has shut down both directions; retirement closes its
    # descriptor once its tasks have joined.
    def live = @resources.count { |resource| resource.raw && !resource.send(:raw_failed?) }

    def observe
      @peak = [@peak, live].max
    end
  end

  class SequenceRandom
    def initialize(*values)
      @values = values
    end

    def rand = @values.shift || 0.0
  end

  # A recording HTTP/2 peer on raw sockets. Each accepted connection either
  # closes immediately or serves requests through the given block. It records
  # every frame, PING payloads with their connection index and ACK flag, and
  # GOAWAY cutoff, code and debug data. Clearing `ack_pings` black-holes every
  # connection's PINGs; `black_holes` lists connection indexes to black-hole.
  # `wire(index)` holds IngressWire observations of what each connection
  # read, and `sent_resets` the [index, stream id, code] of every RST_STREAM
  # the peer wrote.
  class RecordingServer
    attr_reader :accepted, :frames, :requests, :connections, :raws, :pings, :goaways, :black_holes, :sent_resets
    attr_accessor :ack_pings

    def initialize(task, listener, settings: {}, refuse: [], tls: nil, gate: nil, &handler)
      @listener = listener
      @gate = gate
      @settings = settings
      @refuse = refuse
      @tls = tls
      @handler = handler || ->(request) { request.send_response(Protocol::HTTP::Response[200, {}, ["ok"]]) }
      @accepted = 0
      @frames = []
      @requests = Async::Queue.new
      @connections = []
      @raws = []
      @pings = []
      @goaways = []
      @ack_pings = true
      @black_holes = []
      @wires = {}
      @sent_resets = []
      @task = task.async { accept_loop(task) }
    end

    def stop
      @task.stop
      @connections.each(&:close)
      @raws.each { |raw| AsyncHelpers::INTERNAL.raw_close(raw) }
    end

    # PING payloads (not ACKs) the client sent, optionally on one connection.
    def ping_payloads(connection = nil)
      @pings.filter_map { |index, ack, payload| payload if !ack && (connection.nil? || index == connection) }
    end

    def acks?(index) = @ack_pings && !@black_holes.include?(index)

    def wire(index = 0) = @wires[index] ||= IngressWire.observations

    def record(index, frame)
      IngressWire.record(wire(index), frame)
      case frame
      when Protocol::HTTP2::PingFrame then @pings << [index, frame.acknowledgement?, frame.unpack]
      when Protocol::HTTP2::GoawayFrame then @goaways << [index, *frame.unpack]
      end
    end

    def headers_frames = @frames.select { |type, *| type == Protocol::HTTP2::HeadersFrame::TYPE }
    def data_frames = @frames.select { |type, *| type == Protocol::HTTP2::DataFrame::TYPE }

    private

    def instrument(server, index)
      frames = @frames
      peer = self
      server.define_singleton_method(:read_frame) do |&block|
        super() do |frame|
          frames << [frame.type, frame.stream_id, frame.flags]
          peer.record(index, frame)
          block&.call(frame)
        end
      end
      server.define_singleton_method(:receive_ping) do |frame|
        super(frame) if frame.acknowledgement? || peer.acks?(index)
      end
      server.define_singleton_method(:write_frame) do |frame|
        peer.sent_resets << [index, frame.stream_id, frame.unpack] if frame.is_a?(Protocol::HTTP2::ResetStreamFrame)
        super(frame)
      end
      server
    end

    def accept_loop(task)
      loop do
        raw, = @listener.accept
        @accepted += 1
        @raws << raw
        if @refuse.include?(@accepted)
          raw.close
          next
        end
        task.async { serve(task, raw) }
      end
    end

    def serve(task, raw)
      # A gate holds TLS and the server's first SETTINGS, so the client stays connecting.
      @gate&.dequeue
      socket = raw
      if @tls
        socket = @tls.socket(raw)
        socket.accept
      end
      server = instrument(Async::HTTP::Protocol::HTTP2::Server.new(IO::Stream(socket)), @connections.size)
      @connections << server
      server.read_connection_preface(@settings)
      server.start_connection
      while (request = server.requests.dequeue)
        @requests.enqueue(request)
        task.async { @handler.call(request) }
      end
    rescue IOError, SystemCallError, Protocol::HTTP2::Error, OpenSSL::SSL::SSLError
      nil
    end
  end

  def connection_manager(transport) = transport.instance_variable_get(:@manager)

  # Tasks blocked on a connection's frame guard, including unsent dispatchers
  # that are deliberately not registered as retirement writers.
  def guard_waiters(connection) = connection.instance_variable_get(:@frame_guard).waiting.size

  # Binding without listening reserves a port while TCP connect is refused.
  def refusing_socket
    socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
    socket.bind(Socket.sockaddr_in(0, "127.0.0.1"))
    socket
  end

  def install_clock(transport, clock = FakeClock.new, random: SequenceRandom.new)
    manager = connection_manager(transport)
    manager.instance_variable_set(:@clock, clock)
    manager.instance_variable_set(:@random, random)
    clock
  end

  def recording_transport(origin, events, **)
    ConnectRPC::Async::Transport.new(origin: origin, connection_observer: ->(event) { events << event }, **)
  end

  def attempt_outcomes(events) = events.select { |event| event.type == :attempt_finished }.map(&:outcome)
  def state_changes(events) = events.select { |event| event.type == :state_changed }.map(&:state)

  def unavailable_error
    raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
  end
end
