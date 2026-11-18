# frozen_string_literal: true

# Times unary bodies over HTTP/2 through a local proxy that adds round-trip
# time, to compare receive window settings. Not a gating check: the numbers
# depend on the machine. The "Flow-control windows" table in the README came
# from these defaults. Every run opens a new connection, so its windows start
# at their configured sizes rather than where an earlier run grew them; a
# small call establishes the connection before the timed one. Examples:
#
#   mise run async:bench:windows -- --stream 1048576
#   mise run async:bench:windows -- --static
#   mise run async:bench:windows -- --max 4194304
#   mise run async:bench:windows -- --growth --rtt 0.2 --stream 65536
#
# `--growth` instead traces one sustained download and one sustained upload
# per run (16 MiB bodies by default): each step of the receiving side's stream
# window, and each BDP sample as a share of the estimate it was measured
# against. Growth needs a sample of at least two thirds of the estimate. The
# trace hooks private methods of this checkout's connections and estimator.
require "optparse"
require "socket"

root = File.expand_path("../../..", __dir__)
$LOAD_PATH.unshift(File.join(root, "lib"), File.expand_path("../lib", __dir__),
                   File.join(root, "spec/fixtures/generation/echo"))
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
require "echo_connect"

options = { rtt: 0.05, windows: {} }
OptionParser.new do |parser|
  parser.banner = "Usage: window_benchmark.rb [options]"
  parser.on("--rtt SECONDS", Float, "Added round-trip time (default 0.05)") { |value| options[:rtt] = value }
  parser.on("--size BYTES", Integer, "Body size (default 3 MiB, or 16 MiB with --growth)") do |value|
    options[:size] = value
  end
  parser.on("--runs N", Integer, "Runs per measurement; the median is reported (default 3, or 1 with --growth)") do |n|
    options[:runs] = n
  end
  parser.on("--stream BYTES", Integer, "stream_receive_window for both sides") do |value|
    options[:windows][:stream_receive_window] = value
  end
  parser.on("--connection BYTES", Integer, "connection_receive_window for both sides") do |value|
    options[:windows][:connection_receive_window] = value
  end
  parser.on("--max BYTES", Integer, "max_receive_window for both sides") do |value|
    options[:windows][:max_receive_window] = value
  end
  parser.on("--static", "dynamic_receive_windows: false for both sides") do
    options[:windows][:dynamic_receive_windows] = false
  end
  parser.on("--growth", "Trace window growth of one sustained download and one upload per run") do
    options[:growth] = true
  end
end.parse!
growth = options[:growth]
options[:size] ||= (growth ? 16 : 3) * 1024 * 1024
options[:runs] ||= growth ? 1 : 3

module WindowBenchmark
  # A TCP relay that delays each chunk by half the round-trip time in each
  # direction. Writes stay ordered and pipelined, so only latency is added.
  class DelayProxy
    attr_reader :port

    def initialize(target_port, delay)
      @server = TCPServer.new("127.0.0.1", 0)
      @port = @server.addr[1]
      @target_port = target_port
      @delay = delay
      Thread.new { loop { relay(@server.accept) } }
    end

    private

    def relay(client)
      upstream = TCPSocket.new("127.0.0.1", @target_port)
      [[client, upstream], [upstream, client]].each { |from, to| pipe(from, to) }
    end

    def pipe(from, to)
      queue = Thread::Queue.new
      Thread.new do
        loop do
          # Stamp the deadline after the read returns, so idle time before the
          # chunk arrived does not count toward its delay.
          bytes = from.readpartial(65_536)
          queue << [now + @delay, bytes]
        end
      rescue IOError, SystemCallError
        queue << nil
      end
      Thread.new do
        while (item = queue.pop)
          due, bytes = item
          # The added latency is the point of this relay. Read the clock once:
          # a second read can pass the deadline and hand sleep a negative value.
          wait = due - now
          sleep(wait) if wait.positive?
          to.write(bytes)
        end
      rescue IOError, SystemCallError
        nil
      ensure
        to.close_write unless to.closed?
      end
    end

    def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  # Records, per side, each stream window a connection publishes and each BDP
  # sample its estimator ends. The client runs on the main thread and the host
  # on its own, which tells the sides apart.
  module GrowthTrace
    @events = []
    @lock = Mutex.new
    @main = Thread.current

    def self.record(kind, *values)
      side = Thread.current.equal?(@main) ? :client : :host
      at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      @lock.synchronize { @events << [side, kind, at, *values] }
    end

    def self.take
      @lock.synchronize { @events.slice!(0..) }
    end

    module Windows
      private

      def publish_receive_window(frame)
        before = @stream_receive_window
        super
      ensure
        GrowthTrace.record(:window, @stream_receive_window) unless @stream_receive_window == before
      end
    end

    module Samples
      def acknowledged(payload)
        sample = @sample
        estimate = @estimate
        sent = @state == :sent
        super.tap do |grown|
          GrowthTrace.record(:sample, sample, estimate, grown, @limit) if sent && @state.nil?
        end
      end
    end
  end
end

if growth
  internal = ConnectRPC::Async.const_get(:Internal)
  internal::ClientConnection.prepend(WindowBenchmark::GrowthTrace::Windows)
  internal::ServerConnection.prepend(WindowBenchmark::GrowthTrace::Windows)
  internal::BDPEstimator.prepend(WindowBenchmark::GrowthTrace::Samples)
end

echo = Example::V1::EchoService
message = Example::V1::EchoMessage
binding = ConnectRPC::Bindings::GoogleProtobuf.new
body = "z" * options.fetch(:size)
limits = { request_limit: [options.fetch(:size) + 1024, ConnectRPC::Protocol::DEFAULT_LIMIT].max }
limits[:response_limit] = limits[:request_limit]
# The default 16 MiB pending-bytes budget would refuse larger uploads.
policy = ConnectRPC::Async::ConnectionPolicy.new(max_pending_bytes: 2 * limits.fetch(:request_limit))
server = ConnectRPC::Server.new(
  services: [echo::SERVICE], binding: binding, **limits,
  handlers: echo.handlers(
    echo: ->(request, _context) { message.new(name: request.name == "download" ? body : "ok") },
    fail: ->(_request, _context) { raise ConnectRPC::Error.new(:internal, "unused") }
  )
)
windows = options.fetch(:windows)
ports = Thread::Queue.new
stop = Thread::Queue.new
host_thread = Thread.new do
  Sync do
    host = ConnectRPC::Async::Host.new(server: server, **windows).start
    ports << host.port
    stop.pop
    host.close(grace: 0)
  end
end
proxy = WindowBenchmark::DelayProxy.new(ports.pop(timeout: 10) || abort("Host did not start"), options.fetch(:rtt) / 2)
origin = "http://127.0.0.1:#{proxy.port}"
clock = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
# Each run gets its own transport, warmed by a small call that grows no window.
timed = lambda do |&block|
  Array.new(options.fetch(:runs)) do
    transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy, **windows)
    client = echo::Client.new(client: ConnectRPC::Client.new(base_url: origin, transport: transport,
                                                             binding: binding, **limits))
    client.echo(message.new(name: "warm"), timeout: 60)
    WindowBenchmark::GrowthTrace.take
    started = clock.call
    block.call(client)
    [clock.call - started, started]
  ensure
    transport&.close
  end
end
median = lambda do |&block|
  times = timed.call(&block).map(&:first)
  times.sort[times.size / 2]
end
mib = ->(bytes) { format("%.2f MiB", bytes / 1_048_576.0) }

# One line for the window steps and one for the samples of the receiving side.
trace = lambda do |name, side, elapsed, started|
  events = WindowBenchmark::GrowthTrace.take.select { |event| event.first == side }
  steps = events.select { |event| event[1] == :window }.map do |_, _, at, window|
    "#{mib.call(window)} at #{format('%.2fs', at - started)}"
  end
  samples = events.select { |event| event[1] == :sample }
  ceiling = samples.last&.last
  puts format("%<name>s (%<side>s receives) %<size>s in %<elapsed>.2fs; stream window steps: %<steps>s; " \
              "ceiling %<ceiling>s",
              name: name, side: side, size: mib.call(options.fetch(:size)), elapsed: elapsed,
              steps: steps.empty? ? "none" : steps.join(", "), ceiling: ceiling ? mib.call(ceiling) : "n/a")
  shares = samples.map do |event|
    sample, estimate, grown = event[3, 3]
    format("%<share>d%%%<mark>s", share: (100.0 * sample / estimate).floor, mark: grown ? "+" : "")
  end
  shown = shares.first(16)
  shown << "... #{shares.size - 16} more" if shares.size > 16
  puts "  #{samples.size} BDP samples as a share of the estimate, + grew: #{shown.join(' ')}"
end

Sync do |task|
  if growth
    puts format("windows=%<windows>s rtt=%<rtt>.3fs",
                windows: windows.empty? ? "defaults" : windows.inspect, rtt: options.fetch(:rtt))
    timed.call do |client|
      started = clock.call
      client.echo(message.new(name: "download"), timeout: 300)
      trace.call("download", :client, clock.call - started, started)
    end
    timed.call do |client|
      started = clock.call
      client.echo(message.new(name: body), timeout: 300)
      trace.call("upload", :host, clock.call - started, started)
    end
    next
  end

  download = median.call { |client| client.echo(message.new(name: "download"), timeout: 300) }
  concurrent = median.call do |client|
    Array.new(4) { task.async { client.echo(message.new(name: "download"), timeout: 300) } }.each(&:wait)
  end
  upload = median.call { |client| client.echo(message.new(name: body), timeout: 300) }
  puts format("windows=%<windows>s rtt=%<rtt>.3fs size=%<size>d download=%<download>.2fs " \
              "4x-download=%<concurrent>.2fs upload=%<upload>.2fs (medians of %<runs>d runs, each on a new " \
              "connection)",
              windows: windows.empty? ? "defaults" : windows.inspect, rtt: options.fetch(:rtt),
              size: options.fetch(:size), download: download, concurrent: concurrent, upload: upload,
              runs: options.fetch(:runs))
end
stop << true
host_thread.join(10)
