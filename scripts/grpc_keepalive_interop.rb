# frozen_string_literal: true

# HTTP/2 keepalive interoperability with the pinned grpc-go peer (#69). `async`
# runs every case on h2c, TLS and mTLS concurrently; `falcon` runs the age
# drain through real Falcon workers. Assertions read frames recorded on the
# Ruby side of each connection and the Go peers' own checks, never elapsed
# time alone.

# Ruby 4.0's fiber scheduler reads the Go peers' stdout pipes through the
# experimental IO::Buffer; that warning is not about the code under test.
Warning[:experimental] = false

require "json"
require "open3"
require "tmpdir"
require "uri"
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
root = File.expand_path("..", __dir__)
$LOAD_PATH.unshift(File.join(root, "interop/grpc/ruby"), File.join(root, "spec/fixtures/generation/echo"))
require "peer_connect"
require "echo_connect"
require_relative "../gems/connectrpc-async/spec/support/helpers"
host_kind = ARGV.fetch(0, "async")
raise "Expected async or falcon" unless %w[async falcon].include?(host_kind)

module KeepaliveInterop
  PEER = Connectrpc::Grpcinterop::V1::Peer
  MESSAGE = Connectrpc::Grpcinterop::V1::Message
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new
  HEADS_UP = (2**31) - 1
  ENHANCE_YOUR_CALM = 0xb
  # grpc-go's BDP estimator PING payload (internal/transport/bdp_estimator.go).
  BDP_PING = [2, 4, 16, 16, 9, 14, 7, 7].pack("C*").unpack1("H*")
  # grpc-go client keepalive PINGs carry eight zero bytes (http2_client.go).
  KEEPALIVE_PING = "00" * 8
  TYPES = { Protocol::HTTP2::DataFrame::TYPE => "data", Protocol::HTTP2::HeadersFrame::TYPE => "headers",
            Protocol::HTTP2::PingFrame::TYPE => "ping", Protocol::HTTP2::GoawayFrame::TYPE => "goaway" }.freeze

  # One frame as the Ruby side of a connection read or wrote it. `at` is the
  # system-wide monotonic clock, so worker records compare with the parent's.
  Frame = Data.define(:connection, :local_port, :remote_port, :direction, :type, :stream, :flags, :payload,
                      :goaway, :at) do
    def read? = direction == "read"
    def write? = direction == "write"
    def ping? = type == "ping" && flags.nobits?(1)
    def ack? = type == "ping" && flags.allbits?(1)
    def end_stream? = %w[data headers].include?(type) && flags.allbits?(1)
    def calm? = type == "goaway" && goaway[1] == ENHANCE_YOUR_CALM && goaway[2] == "too_many_pings"
    def summary = [direction, type, stream, flags, payload, goaway].compact.join(" ")
  end

  # Records every frame through protocol-http2's framer, the layer every
  # client, Host and Falcon connection shares. Test instrumentation only.
  module FramerRecording
    def read_frame(...)
      frame = super
      KeepaliveInterop.record(self, "read", frame)
      frame
    end

    def write_frame(frame)
      result = super
      KeepaliveInterop.record(self, "write", frame)
      result
    end
  end

  class << self
    attr_accessor :sink

    def record(framer, direction, frame)
      local, remote = (@endpoints ||= ObjectSpace::WeakMap.new)[framer] ||= endpoints(framer)
      type = TYPES.fetch(frame.type, "other")
      payload = frame.unpack.unpack1("H*") if type == "ping"
      goaway = frame.unpack if type == "goaway"
      sink.call(Frame.new(connection: "#{Process.pid}:#{framer.object_id}", local_port: local, remote_port: remote,
                          direction: direction, type: type, stream: frame.stream_id, flags: frame.flags,
                          payload: payload, goaway: goaway, at: Process.clock_gettime(Process::CLOCK_MONOTONIC)))
    end

    def endpoints(framer)
      io = framer.instance_variable_get(:@stream)
      io = io.io if io.respond_to?(:io)
      io = io.to_io
      [io.local_address.ip_port, io.remote_address.ip_port]
    rescue StandardError
      [nil, nil]
    end
  end

  # Frames from this process (and Falcon workers, through the event pipe),
  # with a notification for observation-driven waits.
  class Wire
    def initialize
      @frames = []
      @notification = Async::Notification.new
    end

    def <<(frame)
      @frames << frame
      signal
    end

    def signal = @notification.signal

    def frames(connection = nil, local_port: nil, remote_port: nil)
      @frames.select do |frame|
        (connection.nil? || frame.connection == connection) && (local_port.nil? || frame.local_port == local_port) &&
          (remote_port.nil? || frame.remote_port == remote_port)
      end
    end

    def connections(**) = frames(**).map(&:connection).uniq

    # Each PING written waits `time` after the latest frame read before it.
    def ping_gaps(connection)
      last_read = nil
      frames(connection).filter_map do |frame|
        last_read = frame.at if frame.read?
        frame.at - last_read if frame.write? && frame.ping? && last_read
      end
    end

    # Returns the block's first truthy value. On timeout, reports the last
    # observed state: `state` or the most recent frames.
    def wait(description, within:, state: nil)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + within
      loop do
        result = yield
        return result if result

        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        if remaining <= 0
          observed = state ? state.call : @frames.last(12).map(&:summary)
          raise "Timed out after #{within}s waiting for #{description}; last observed: #{observed.inspect}"
        end
        Async::Task.current.with_timeout(remaining) { @notification.wait }
      rescue Async::TimeoutError
        next
      end
    end
  end

  # A grpc-go keepalive command. It prints progress lines and waits for the
  # harness's lines on stdin; its stderr passes through.
  class GoPeer
    def initialize(label, *command)
      @label = label
      @input, @output, @process = Open3.popen2(*command)
      @lines = []
    end

    def read(within:)
      line = Async::Task.current.with_timeout(within) { @output.gets }
      raise "#{@label}: grpc-go exited; output #{@lines.inspect}" unless line

      note(line)
    rescue Async::TimeoutError
      raise "#{@label}: no grpc-go output within #{within}s; output #{@lines.inspect}"
    end

    def expect(expected, within:)
      line = read(within: within)
      raise "#{@label}: expected #{expected.inspect}, got #{line.inspect}" unless line == expected
    end

    def say(line)
      @input.puts(line)
      @input.flush
    end

    def finish(within:)
      @input.close unless @input.closed?
      status = Async::Task.current.with_timeout(within) do
        while (line = @output.gets)
          note(line)
        end
        @process.value
      end
      raise "#{@label}: grpc-go failed (#{status}); output #{@lines.inspect}" unless status.success?

      @lines
    rescue Async::TimeoutError
      raise "#{@label}: grpc-go did not exit within #{within}s; output #{@lines.inspect}"
    ensure
      kill
    end

    def kill
      return unless @process.alive?

      Process.kill("KILL", @process.pid)
      @process.join
    rescue Errno::ESRCH
      nil
    end

    private

    def note(line)
      line = line.chomp
      @lines << line
      puts "#{line} [#{@label}]" if line.start_with?("PASS")
      line
    end
  end

  def self.server
    ConnectRPC::Server.new(services: [PEER::SERVICE], binding: BINDING, protocol: :grpc,
                           handlers: PEER.handlers(
                             unary: ->(request, _context) { request },
                             server_stream: lambda { |request, writer, _context|
                               [request.value, "second", ""].each do |value|
                                 writer.send_message(MESSAGE.new(value: value, payload: request.payload))
                               end
                             },
                             client_stream: lambda { |reader, _context|
                               values = []
                               reader.each { |message| values << message.value } # rubocop:disable Style/MapIntoArray -- RequestReader has block-only each.
                               MESSAGE.new(value: values.join(","))
                             },
                             bidi: ->(duplex, _context) { duplex.each { |message| duplex.send_message(message) } }
                           ))
  end

  # Wire evidence shared by the Host and Falcon age-drain cases.
  module AgeDrain
    # Connection A carried the bidi through both drain GOAWAYs: request DATA
    # read and response DATA plus END_STREAM trailers written after the
    # cutoff. Connection B carried the next RPC.
    def self.verify(wire, port, label)
      first, second, = wire.connections(local_port: port)
      raise "#{label}: expected a second connection, saw #{[first, second].compact}" unless first && second

      frames = wire.frames(first)
      heads_up = frames.index { |frame| frame.write? && frame.type == "goaway" && frame.goaway[0] == HEADS_UP }
      cutoff = frames.index { |frame| frame.write? && frame.type == "goaway" && frame.goaway[0..1] == [1, 0] }
      unless heads_up && cutoff && cutoff > heads_up
        raise "#{label}: drain GOAWAY pair missing on A: #{frames.map(&:summary)}"
      end

      after = frames[(cutoff + 1)..]
      bidi = after.select { |frame| frame.stream == 1 }
      completed = bidi.any? { |frame| frame.read? && frame.type == "data" } &&
                  bidi.any? { |frame| frame.write? && frame.type == "data" } &&
                  bidi.any? { |frame| frame.write? && frame.type == "headers" && frame.end_stream? }
      raise "#{label}: bidi did not complete on A after GOAWAY: #{after.map(&:summary)}" unless completed

      next_rpc = wire.frames(second)
      unless next_rpc.any? { |frame| frame.read? && frame.type == "headers" } &&
             next_rpc.any? { |frame| frame.write? && frame.type == "headers" && frame.end_stream? }
        raise "#{label}: next RPC missing on B: #{next_rpc.map(&:summary)}"
      end

      "GOAWAY #{HEADS_UP} then 1 on A, bidi completed on A after them, next RPC on B"
    end

    # Drives a keepalive-age peer: the bidi is open, then continues only once
    # the server wrote the cutoff GOAWAY on its connection.
    def self.run(wire, peer, port, label)
      peer.expect("OPEN", within: 10)
      wire.wait("#{label} cutoff GOAWAY", within: 10) do
        first = wire.connections(local_port: port).first
        wire.frames(first).any? { |frame| frame.write? && frame.type == "goaway" && frame.goaway[0..1] == [1, 0] }
      end
      peer.say("drained")
      peer.finish(within: 15)
      verify(wire, port, label)
    ensure
      peer.kill
    end
  end

  # One mode's peers, credentials and bidi helpers.
  class ModeCases
    def initialize(mode, binary:, fixtures:, helpers:, identities:, authority:, wire:)
      @mode = mode
      @binary = binary
      @fixtures = fixtures
      @helpers = helpers
      @identities = identities
      @authority = authority
      @wire = wire
    end

    def go(*command, origin: nil)
      GoPeer.new("#{@mode} #{command.first}", @binary, *command, *origin, @mode, @fixtures)
    end

    def origin(port) = "#{h2c? ? 'http' : 'https'}://127.0.0.1:#{port}"
    def h2c? = @mode == "h2c"

    def client_options
      return {} if h2c?

      options = { ca_file: File.join(@fixtures, "ca.pem") }
      options.merge!(@helpers.host_tls(@identities.fetch("client")).except(:client_ca)) if @mode == "mtls"
      options
    end

    def peer_client(origin, transport)
      core = ConnectRPC::Client.new(base_url: origin, binding: BINDING, transport: transport, protocol: :grpc)
      PEER::Client.new(client: core)
    end

    def echo(duplex, value)
      duplex.send_message(MESSAGE.new(value: value))
      reply = duplex.receive
      raise "#{@mode}: bidi echo #{value.inspect} got #{reply&.value.inspect}" unless reply&.value == value
    end

    def finish_bidi(duplex)
      duplex.close_send
      raise "#{@mode}: bidi EOF" unless duplex.receive.nil?
    end
  end

  # The Ruby client against a grpc-go server.
  class ClientCases < ModeCases
    # Ruby client keepalive against grpc-go enforcement (MinTime 1s, no PINGs
    # without streams), compliant and over-eager, on one Go server.
    def ruby_client
      server = go("keepalive-serve")
      origin = server.read(within: 10)
      port = URI(origin).port
      compliant_client(origin, port)
      eager_client(origin, port)
    ensure
      server&.finish(within: 10)
    end

    def keepalive_transport(origin, time, events)
      policy = ConnectRPC::Async::ConnectionPolicy.new(keepalive: ConnectRPC::Async::Keepalive.new(time: time))
      observer = lambda do |event|
        events << event
        @wire.signal
      end
      ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy, connection_observer: observer,
                                       **client_options)
    end

    def new_connection(port, seen, except: nil)
      @wire.wait("#{@mode} client connection to #{port}", within: 5) do
        (@wire.connections(remote_port: port) - seen - [except]).first
      end
    end

    # The quiet bidi outlives the strike threshold: grpc-go's fourth PING
    # would carry the third strike, so four ACKs followed by an echo on the
    # same connection prove every PING was accepted.
    def compliant_client(origin, port)
      seen = @wire.connections(remote_port: port)
      events = []
      transport = keepalive_transport(origin, 1, events)
      connection = acks = nil
      peer_client(origin, transport).bidi do |duplex|
        echo(duplex, "first")
        connection = new_connection(port, seen)
        acks = @wire.wait("#{@mode} four PING ACKs", within: 20) do
          received = @wire.frames(connection).select { |frame| frame.read? && frame.ack? }
          received if received.size >= 4
        end
        echo(duplex, "after-pings")
        finish_bidi(duplex)
      end
      connections = @wire.connections(remote_port: port) - seen
      goaways = @wire.frames(connection).select { |frame| frame.read? && frame.type == "goaway" }
      closed = events.select { |event| event.type == :connection_closed }
      unless connections == [connection] && goaways.empty? && closed.empty?
        raise "#{@mode}: compliant client moved connections #{connections}, GOAWAY #{goaways.map(&:summary)}, " \
              "closed #{closed}"
      end

      puts "PASS Ruby keepalive 1s -> grpc-go MinTime 1s #{@mode}: #{acks.size} PING ACKs on one quiet bidi " \
           "connection (min gap #{@wire.ping_gaps(connection).min.round(2)}s), no GOAWAY, then echo"
    ensure
      transport&.close(grace: 0)
    end

    # grpc-go answers 0.2s PINGs with GOAWAY too_many_pings; the next
    # connection waits at least twice as long between PINGs.
    def eager_client(origin, port)
      seen = @wire.connections(remote_port: port)
      events = []
      transport = keepalive_transport(origin, 0.2, events)
      client = peer_client(origin, transport)
      first = calmed_connection(client, port, seen)
      closed = @wire.wait("#{@mode} connection_closed event", within: 5, state: -> { events }) do
        events.find { |event| event.type == :connection_closed }
      end
      raise "#{@mode}: connection_closed outcome #{closed.outcome.inspect}" unless closed.outcome == :goaway

      second = nil
      client.bidi do |duplex|
        echo(duplex, "again")
        second = new_connection(port, seen, except: first)
        @wire.wait("#{@mode} two PINGs on the next connection", within: 5) do
          @wire.frames(second).count { |frame| frame.write? && frame.ping? } >= 2
        end
        finish_bidi(duplex)
      end
      before = @wire.ping_gaps(first)
      after = @wire.ping_gaps(second)
      raise "#{@mode}: PING gaps not doubled: #{before} -> #{after}" unless after.size >= 2 && after.min >= 0.4

      puts "PASS Ruby keepalive 0.2s -> grpc-go #{@mode}: GOAWAY ENHANCE_YOUR_CALM too_many_pings, " \
           "connection_closed :goaway, next connection PING gaps #{after.map { |gap| gap.round(2) }} " \
           "(was #{before.map { |gap| gap.round(2) }})"
    ensure
      transport&.close(grace: 0)
    end

    def calmed_connection(client, port, seen)
      connection = nil
      begin
        client.bidi do |duplex|
          echo(duplex, "first")
          connection = new_connection(port, seen)
          duplex.receive
          raise "#{@mode}: over-eager bidi was not ended"
        end
      rescue ConnectRPC::Error => e
        raise "#{@mode}: over-eager bidi failed with #{e.code}" unless e.code == "unavailable"
      end
      goaway = @wire.frames(connection).find { |frame| frame.read? && frame.type == "goaway" }
      raise "#{@mode}: expected GOAWAY too_many_pings, got #{goaway&.summary.inspect}" unless goaway&.calm?

      connection
    end
  end

  # grpc-go clients and a raw framer against Ruby hosts.
  class HostCases < ModeCases
    def host(keepalive)
      tls = @helpers.host_tls(@identities.fetch("server"), anchors: @mode == "mtls" ? [@authority] : nil) unless h2c?
      ConnectRPC::Async::Host.new(server: KeepaliveInterop.server, tls: tls, keepalive: keepalive).start
    end

    # grpc-go's documented example client keepalive (10s/1s/permit) against
    # the default ServerKeepalive. grpc-go raises any client Time below 10s to
    # 10s, so this case waits on real wall-clock PINGs at that external
    # minimum: four PINGs take about 40 seconds.
    def documented_client
      host = host(ConnectRPC::Async::ServerKeepalive.new)
      client = go("keepalive-client", origin: origin(host.port))
      client.expect("READY", within: 10)
      observed = -> { received_pings(host.port) }
      pings = @wire.wait("#{@mode} four grpc-go keepalive PINGs", within: 60, state: -> { observed.call.map(&:at) }) do
        received = observed.call
        received if received.size >= 4
      end
      client.say("continue")
      client.finish(within: 15)
      frames = @wire.frames(local_port: host.port)
      acks = frames.count { |frame| frame.write? && frame.ack? && frame.payload == KEEPALIVE_PING }
      acked = acks >= pings.size
      goaways = frames.select { |frame| frame.write? && frame.type == "goaway" }
      connections = @wire.connections(local_port: host.port)
      unless connections.size == 1 && acked && goaways.empty?
        raise "#{@mode}: documented client connections #{connections}, acked #{acked}, GOAWAY " \
              "#{goaways.map(&:summary)}"
      end

      gaps = pings.each_cons(2).map { |earlier, later| (later.at - earlier.at).round(1) }
      puts "PASS grpc-go keepalive 10s/1s/permit -> Host default #{@mode}: #{pings.size} keepalive PINGs acked " \
           "(gaps #{gaps}s), one connection, no GOAWAY, then unary"
    ensure
      client&.kill
      host&.close(grace: 0)
    end

    def received_pings(port)
      @wire.frames(local_port: port).select { |frame| frame.read? && frame.ping? && frame.payload == KEEPALIVE_PING }
    end

    def flood
      host = host(ConnectRPC::Async::ServerKeepalive.new)
      go("keepalive-flood", origin: origin(host.port)).finish(within: 15)
      calm = @wire.frames(local_port: host.port).select { |frame| frame.write? && frame.calm? }
      raise "#{@mode}: Host did not record GOAWAY too_many_pings" unless calm.size == 1

      puts "PASS Host default #{@mode}: raw framer flood answered with GOAWAY #{calm.first.goaway.inspect}"
    ensure
      host&.close(grace: 0)
    end

    def bdp
      host = host(ConnectRPC::Async::ServerKeepalive.new)
      go("keepalive-bdp", origin: origin(host.port)).finish(within: 30)
      frames = @wire.frames(local_port: host.port)
      bdp = frames.select { |frame| frame.read? && frame.ping? && frame.payload == BDP_PING }
      acked = frames.count { |frame| frame.write? && frame.ack? && frame.payload == BDP_PING }
      goaways = frames.select { |frame| frame.write? && frame.type == "goaway" }
      unless bdp.any? && acked == bdp.size && goaways.empty?
        raise "#{@mode}: BDP PINGs #{bdp.size}, acked #{acked}, GOAWAY #{goaways.map(&:summary)}"
      end

      puts "PASS grpc-go default client -> Host default #{@mode}: #{bdp.size} BDP PINGs acked, not rejected"
    ensure
      host&.close(grace: 0)
    end

    def age_drain
      host = host(ConnectRPC::Async::ServerKeepalive.new(max_connection_age: 1))
      evidence = AgeDrain.run(@wire, go("keepalive-age", origin: origin(host.port)), host.port,
                              "#{@mode} Host")
      puts "PASS Host max_connection_age 1s #{@mode}: #{evidence}"
    ensure
      host&.close(grace: 0)
    end
  end

  # The `async` and `falcon` entry points.
  module Run
    # Every mode and case at once: the documented client's 40 seconds of PINGs
    # bound the wall time. The first failure stops the rest.
    def self.async(wire, **)
      barrier = Async::Barrier.new
      %w[h2c tls mtls].each do |mode|
        client = ClientCases.new(mode, wire: wire, **)
        host = HostCases.new(mode, wire: wire, **)
        barrier.async { client.ruby_client }
        %i[documented_client flood bdp age_drain].each { |name| barrier.async { host.public_send(name) } }
      end
      barrier.wait
    ensure
      barrier.stop
    end

    # The age drain through two real Falcon workers over TLS. Worker frames
    # reach this process as single-write JSON events on the worker event pipe.
    def self.falcon(wire, binary:, fixtures:, helpers:, identities:, **)
      require "connectrpc/falcon"
      require_relative "../gems/connectrpc-falcon/spec/support/process_cluster"
      reader, writer = IO.pipe
      writer = FalconProcessFixtures::FalconEventWriter.new(writer)
      writer.sync = true
      KeepaliveInterop.sink = ->(frame) { writer.puts(JSON.generate(kind: "frame", **frame.to_h)) }
      host = FalconProcessFixtures::FalconProcessCluster.new(falcon_configuration(writer, helpers, identities), reader,
                                                             writer)
      host.event(:running, count: 2)
      pids = host.events.select { |event| event.fetch("kind") == "running" }.map { |event| event.fetch("pid") }
      raise "Real Falcon workers missing" unless pids.uniq.size == 2 && !pids.include?(Process.pid)

      pump = Async::Task.current.async { pump_events(reader, host, wire) }
      port = host.service.port
      peer = GoPeer.new("tls keepalive-age", binary, "keepalive-age", "https://127.0.0.1:#{port}", "tls", fixtures)
      evidence = AgeDrain.run(wire, peer, port, "tls Falcon")
      host.stop
      closed = wire.wait("Falcon workers closed", within: 10) do
        found = host.events.select { |event| event.fetch("kind") == "closed" }
        found if found.size >= 2
      end
      raise "Falcon leaked owned requests/connections" unless closed.all? do |event|
        event.values_at("requests", "connections") == [0, 0]
      end

      workers = wire.connections(local_port: port).map { |connection| connection.split(":").first }
      puts "PASS Falcon max_connection_age 1s tls: #{evidence} (worker pids #{workers.join(', ')})"
    ensure
      pump&.stop
      host&.close
    end

    def self.falcon_configuration(writer, helpers, identities)
      tls = helpers.host_tls(identities.fetch("server"))
      options = { keepalive: ConnectRPC::Async::ServerKeepalive.new(max_connection_age: 1) }
      Async::Service::Configuration.build do
        service "grpc-keepalive" do
          include ConnectRPC::Falcon::Environment
          include FalconProcessFixtures::FalconWorkerObservation

          url "https://127.0.0.1:0"
          count 2
          health_check_timeout nil
          events writer
          connectrpc_server { KeepaliveInterop.server }
          tls tls
          server_options options
        end
      end
    end

    def self.pump_events(reader, host, wire)
      while (line = reader.gets)
        event = JSON.parse(line)
        if event.fetch("kind") == "frame"
          wire << Frame.new(**event.except("kind").transform_keys(&:to_sym))
        else
          host.events << event
          wire.signal
        end
      end
    end
  end
end

helpers = Object.new.extend(AsyncHelpers)
# A separate binary: async:verify may build the shared peer concurrently.
binary = File.join(root, "vendor/grpc/peer-keepalive")
system("go", "mod", "verify", chdir: File.join(root, "interop/grpc"), exception: true)
system("go", "build", "-mod=readonly", "-o", binary, ".", chdir: File.join(root, "interop/grpc"), exception: true)
Protocol::HTTP2::Framer.prepend(KeepaliveInterop::FramerRecording)
started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
Dir.mktmpdir("grpc-keepalive-", File.join(root, "vendor/grpc")) do |fixtures|
  ca = helpers.credentials(1, "grpc peer CA", authority: true)
  identities = { "server" => helpers.credentials(101, "localhost", issuer: ca, usage: "serverAuth"),
                 "client" => helpers.credentials(201, "client", issuer: ca, usage: "clientAuth") }
  File.write(File.join(fixtures, "ca.pem"), ca.first.to_pem)
  identities.each do |name, (certificate, key)|
    File.write(File.join(fixtures, "#{name}.pem"), certificate.to_pem)
    File.write(File.join(fixtures, "#{name}.key"), key.private_to_pem)
  end
  wire = KeepaliveInterop::Wire.new
  KeepaliveInterop.sink = ->(frame) { wire << frame }
  options = { binary: binary, fixtures: fixtures, helpers: helpers, identities: identities, authority: ca }
  Sync do |task|
    task.with_timeout(150) { KeepaliveInterop::Run.public_send(host_kind, wire, **options) }
  end
end
elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
puts format("Keepalive interop (%<kind>s) finished in %<seconds>.1fs", kind: host_kind, seconds: elapsed)
