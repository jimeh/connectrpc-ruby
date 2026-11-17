# frozen_string_literal: true

# Times unary bodies over HTTP/2 through a local proxy that adds round-trip
# time, to compare receive window settings. Not a gating check: the numbers
# depend on the machine. The "Flow-control windows" table in the README came
# from these defaults. Example:
#
#   mise run async:bench:windows -- --stream 1048576
require "optparse"
require "socket"

root = File.expand_path("../../..", __dir__)
$LOAD_PATH.unshift(File.join(root, "lib"), File.expand_path("../lib", __dir__),
                   File.join(root, "spec/fixtures/generation/echo"))
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
require "echo_connect"

options = { rtt: 0.05, size: 3 * 1024 * 1024, runs: 3, windows: {} }
OptionParser.new do |parser|
  parser.banner = "Usage: window_benchmark.rb [options]"
  parser.on("--rtt SECONDS", Float, "Added round-trip time (default 0.05)") { |value| options[:rtt] = value }
  parser.on("--size BYTES", Integer, "Body size (default 3 MiB)") { |value| options[:size] = value }
  parser.on("--runs N", Integer, "Runs per measurement; the median is reported (default 3)") do |value|
    options[:runs] = value
  end
  parser.on("--stream BYTES", Integer, "stream_receive_window for both sides") do |value|
    options[:windows][:stream_receive_window] = value
  end
  parser.on("--connection BYTES", Integer, "connection_receive_window for both sides") do |value|
    options[:windows][:connection_receive_window] = value
  end
end.parse!

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
      loop { queue << [now + @delay, from.readpartial(65_536)] }
    rescue IOError, SystemCallError
      queue << nil
    end
    Thread.new do
      while (item = queue.pop)
        due, bytes = item
        # The added latency is the point of this relay.
        sleep(due - now) if due > now
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

echo = Example::V1::EchoService
message = Example::V1::EchoMessage
binding = ConnectRPC::Bindings::GoogleProtobuf.new
body = "z" * options.fetch(:size)
server = ConnectRPC::Server.new(
  services: [echo::SERVICE], binding: binding,
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
proxy = DelayProxy.new(ports.pop(timeout: 10) || abort("Host did not start"), options.fetch(:rtt) / 2)
origin = "http://127.0.0.1:#{proxy.port}"
clock = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
median = lambda do |&block|
  times = Array.new(options.fetch(:runs)) do
    started = clock.call
    block.call
    clock.call - started
  end
  times.sort[times.size / 2]
end

Sync do |task|
  transport = ConnectRPC::Async::Transport.new(origin: origin, **windows)
  client = echo::Client.new(client: ConnectRPC::Client.new(base_url: origin, transport: transport, binding: binding))
  client.echo(message.new(name: "warm"), timeout: 60)
  download = median.call { client.echo(message.new(name: "download"), timeout: 300) }
  concurrent = median.call do
    Array.new(4) { task.async { client.echo(message.new(name: "download"), timeout: 300) } }.each(&:wait)
  end
  upload = median.call { client.echo(message.new(name: body), timeout: 300) }
  transport.close
  puts format("windows=%<windows>s rtt=%<rtt>.3fs size=%<size>d download=%<download>.2fs " \
              "4x-download=%<concurrent>.2fs upload=%<upload>.2fs",
              windows: windows.empty? ? "defaults" : windows.inspect, rtt: options.fetch(:rtt),
              size: options.fetch(:size), download: download, concurrent: concurrent, upload: upload)
end
stop << true
host_thread.join(10)
