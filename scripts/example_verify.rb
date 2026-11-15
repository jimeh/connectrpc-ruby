# frozen_string_literal: true

# Starts every runnable example server on an ephemeral port, runs every
# supported client call against it plus representative refusals, then stops it.
require "fileutils"
require "json"
require "rbconfig"
require_relative "../examples/demo/support"

ROOT = DemoExample::ROOT
LAUNCHER = File.join(ROOT, "scripts/example.rb")
READY_TIMEOUT = 120
CLIENT_TIMEOUT = 60
STOP_TIMEOUT = 20
WORKERS = 4

# One child process in its own process group, with combined output captured
# by a reader thread and every wait bounded.
class Child
  def initialize(env, command)
    @reader, writer = IO.pipe
    @pid = Process.spawn(env, *command, out: writer, err: writer, in: File::NULL, pgroup: true, chdir: ROOT)
    writer.close
    @waiter = Process.detach(@pid)
    @lines = Queue.new
    @output = +""
    @lock = Mutex.new
    @pump = Thread.new do
      @reader.each_line do |line|
        @lock.synchronize { @output << line }
        @lines << line
      end
    ensure
      @lines.close
    end
  end

  def output = @lock.synchronize { @output.dup }

  def wait_for_line(pattern, timeout)
    deadline = now + timeout
    loop do
      line = @lines.pop(timeout: [deadline - now, 0].max)
      match = line&.match(pattern)
      return match if match
      next if line

      reason = @lines.closed? ? "exited" : "timed out after #{timeout}s"
      raise "#{reason} before printing #{pattern.inspect}; output:\n#{output}"
    end
  end

  # Returns the exit status, or nil when the process outlives the timeout.
  def wait(timeout)
    status = @waiter.join(timeout)&.value
    @pump.join(5) if status
    status
  end

  def signal(name)
    Process.kill(name, -@pid)
  rescue Errno::ESRCH
    nil
  end

  def stop(name, timeout)
    signal(name)
    wait(timeout) || kill
  end

  def kill
    signal("TERM")
    return if wait(5)

    signal("KILL")
    wait(5)
    nil
  end

  def close
    kill if @waiter.alive?
    @pump.join(5)
    @reader.close
  end

  def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

Case = Struct.new(:client, :args, :status, :expected, keyword_init: true)

def run_client(env, kase)
  child = Child.new(env, [RbConfig.ruby, LAUNCHER, "client", kase.client, *kase.args])
  status = child.wait(CLIENT_TIMEOUT)
  child.kill unless status
  [status, child.output]
ensure
  child&.close
end

UNARY = [[], %w[--codec json], %w[--get], %w[--gzip]].freeze
STREAM = [[], %w[--codec json], %w[--gzip]].freeze
GRPC = [%w[--protocol grpc], %w[--protocol grpc --gzip]].freeze
H1_UNARY = { "greet" => UNARY, "fail" => [[]] }.freeze
H1_STREAMING = H1_UNARY.merge("countdown" => STREAM, "collect" => STREAM).freeze
H2_ALL = H1_STREAMING.merge("chat" => STREAM).transform_values { |variants| variants + GRPC }.freeze
# Independent of the support table: what each pairing must actually do.
ALLOWED = {
  "rack" => { "nethttp" => H1_UNARY, "async-http1" => H1_UNARY },
  "rails" => { "nethttp" => H1_UNARY, "async-http1" => H1_UNARY },
  "async" => { "async-h2" => H2_ALL },
  "falcon" => { "nethttp" => H1_UNARY, "async-http1" => H1_STREAMING, "async-h2" => H2_ALL }
}.freeze
NETHTTP_STREAMING = ["nethttp", %w[countdown],
                     "nethttp supports only unary Connect calls; countdown is server streaming"].freeze
REFUSED = {
  "rack" => [["async-h2", %w[greet], "async-h2 uses HTTP/2, but rack serves only HTTP/1.1"],
             ["async-http1", %w[countdown],
              "rack serves only unary Connect calls over HTTP/1.1; countdown is server streaming"],
             NETHTTP_STREAMING],
  "rails" => [["async-h2", %w[greet], "async-h2 uses HTTP/2, but rails serves only HTTP/1.1"],
              ["async-http1", %w[countdown],
               "rails serves only unary Connect calls over HTTP/1.1; countdown is server streaming"],
              NETHTTP_STREAMING],
  "async" => [["async-http1", %w[greet], "async-http1 uses HTTP/1.1, but async serves only HTTP/2"],
              NETHTTP_STREAMING,
              ["async-h2", %w[greet --protocol grpc --codec json], "gRPC requires the protobuf codec"],
              ["async-h2", %w[greet --protocol grpc --get], "gRPC has no GET form"]],
  "falcon" => [["async-http1", %w[chat], "async-http1 supports only unary, server streaming, client streaming " \
                                         "Connect calls; chat is bidi streaming"],
               ["async-http1", %w[greet --protocol grpc],
                "async-http1 does not support gRPC; native gRPC needs HTTP/2"],
               ["nethttp", %w[greet --protocol grpc], "nethttp does not support gRPC; native gRPC needs HTTP/2"],
               NETHTTP_STREAMING,
               ["async-h2", %w[countdown --get], "--get applies only to unary RPCs"]]
}.freeze
SIGNALS = { "rack" => "INT", "rails" => "TERM", "async" => "INT", "falcon" => "TERM" }.freeze

def expected_output(server, rpc, variant)
  protocol = variant.include?("grpc") ? "grpc" : "connect"
  codec = variant.include?("json") ? "json" : "proto"
  encoding = variant.include?("--gzip") ? "gzip" : "identity"
  banner = "at http://127.0.0.1:"
  options = "(#{protocol}, #{codec}, #{encoding}#{', GET' if variant.include?('--get')})"
  served = "headers: demo-served-by=#{server}"
  expected = case rpc
             when "greet" then [served, "message: Hello, Ada!", "trailers: demo-greeted=Ada"]
             when "fail" then ["error: invalid_argument: name is required"]
             when "countdown" then [served, "message: 3\nmessage: 2\nmessage: 1\n", "trailers: demo-countdown=done"]
             when "collect" then [served, "message: Collected 2 names: Ada, Grace"]
             when "chat" then ["message: Hello, Ada!\nmessage: Hello, Grace!\n", served]
             end
  [rpc == "fail" ? 1 : 0, [banner, options, *expected]]
end

def cases_for(server, url)
  explicit = ["--server", server, "--url", url]
  allowed = ALLOWED.fetch(server).flat_map do |client, rpcs|
    rpcs.flat_map do |rpc, variants|
      variants.map do |variant|
        arguments = { "greet" => %w[Ada], "countdown" => %w[3], "collect" => %w[Ada Grace], "chat" => %w[Ada Grace] }
        status, expected = expected_output(server, rpc, variant)
        Case.new(client: client, args: [rpc, *arguments.fetch(rpc, []), *variant, *explicit],
                 status: status, expected: expected)
      end
    end
  end
  # The first call finds the server through the state file alone; another passes only a matching --url.
  allowed.first.args -= explicit
  allowed << Case.new(client: allowed.first.client, args: ["greet", "Ada", "--url", url], status: 0,
                      expected: ["headers: demo-served-by=#{server}", "message: Hello, Ada!"])
  refused = REFUSED.fetch(server).map do |client, args, reason|
    guide = reason.start_with?("gRPC", "--get") ? "supported options" : "supported pairings"
    Case.new(client: client, args: [*args, *explicit], status: 2,
             expected: ["Refused: #{reason}", "See examples/README.md for the #{guide}."])
  end
  [allowed, refused]
end

def discovery_cases(url, state)
  dead = Process.spawn(RbConfig.ruby, "-e", "")
  Process.wait(dead)
  stale = "#{state}.stale"
  File.write(File.join(ROOT, stale), JSON.generate(server: "rack", url: url, pid: dead))
  [
    [{ "CONNECTRPC_EXAMPLE_STATE" => state },
     Case.new(client: "nethttp", args: %w[greet --url http://127.0.0.1:9], status: 2,
              expected: ["--url http://127.0.0.1:9 is not the running example server (rack at #{url})"])],
    [{ "CONNECTRPC_EXAMPLE_STATE" => state },
     Case.new(client: "nethttp", args: %w[greet --server nope], status: 2,
              expected: ["Unknown server nope; choose one of: rack, rails, async, falcon"])],
    [{ "CONNECTRPC_EXAMPLE_STATE" => "#{state}.missing" },
     Case.new(client: "nethttp", args: %w[greet], status: 2,
              expected: ["No running example server found (no state file", "mise run example:server -- NAME"])],
    [{ "CONNECTRPC_EXAMPLE_STATE" => stale },
     Case.new(client: "nethttp", args: %w[greet], status: 2, expected: ["names pid #{dead}, which has exited"])]
  ]
end

def check(server, env, kase)
  status, output = run_client(env, kase)
  label = "#{server} <- #{kase.client} #{kase.args.join(' ')}"
  problem = if status.nil? then "timed out"
            elsif status.exitstatus != kase.status then "exit #{status.exitstatus.inspect}, expected #{kase.status}"
            elsif (missing = kase.expected.reject { |text| output.include?(text) }).any?
              "missing #{missing.map(&:inspect).join(', ')}"
            end
  [label, problem, output]
end

def run_all(server, pairs)
  work = Queue.new
  pairs.each_with_index { |pair, index| work << [index, pair] }
  work.close
  results = []
  Array.new(WORKERS) do
    Thread.new do
      while (item = work.pop)
        index, (env, kase) = item
        results[index] = check(server, env, kase)
      end
    end
  end.each(&:join)
  results
end

state_dir = "vendor/examples/verify-#{Process.pid}"
failures = []
totals = Hash.new(0)
puts "Runnable examples; Ruby #{RUBY_VERSION}; Rails lane #{ENV.fetch('RAILS_LANE', '8.1')}"
begin
  SIGNALS.each do |server, signal|
    # A relative override resolves against the repository root even though runners change directory.
    state = "#{state_dir}/#{server}.json"
    env = { "CONNECTRPC_EXAMPLE_STATE" => state }
    child = Child.new(env.merge("PORT" => "0"), [RbConfig.ruby, LAUNCHER, "server", server])
    begin
      url = child.wait_for_line(%r{#{server} server listening on (http://127\.0\.0\.1:\d+)}, READY_TIMEOUT)[1]
      record = JSON.parse(File.read(File.join(ROOT, state)))
      raise "#{server}: state file records #{record.inspect}" unless record["server"] == server && record["url"] == url

      allowed, refused = cases_for(server, url)
      pairs = [*allowed, *refused].map { |kase| [env, kase] }
      pairs += discovery_cases(url, state) if server == "rack"
      run_all(server, pairs).each do |label, problem, output|
        puts "#{problem ? 'FAIL' : 'ok  '} #{label}"
        failures << "#{label}: #{problem}\n#{output}" if problem
      end
      totals[:allowed] += allowed.size
      totals[:refused] += pairs.size - allowed.size
      status = child.stop(signal, STOP_TIMEOUT)
      stopped = status&.success? && child.output.include?("#{server} server stopped")
      raise "#{server} did not stop cleanly on SIG#{signal}:\n#{child.output}" unless stopped
      raise "#{server} left its state file behind" if File.exist?(File.join(ROOT, state))

      puts "ok   #{server} stopped on SIG#{signal}"
      totals[:servers] += 1
    rescue StandardError => e
      failures << "#{server}: #{e.message}"
    ensure
      child.close
    end
  end
ensure
  FileUtils.rm_rf(File.join(ROOT, state_dir))
end

unless failures.empty?
  warn "\n#{failures.join("\n\n")}"
  abort "Runnable examples failed: #{failures.size} problem(s)"
end
puts "Runnable examples passed: #{totals[:servers]} servers, #{totals[:allowed]} supported calls, " \
     "#{totals[:refused]} refusals"
