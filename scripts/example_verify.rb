# frozen_string_literal: true

# Starts every runnable example server on an ephemeral port, runs every
# supported client call against it plus representative refusals, then stops it.
# The Async and Falcon servers run again with --mtls for a representative set.
require "fileutils"
require "io/wait"
require "json"
require "openssl"
require "rbconfig"
require "socket"
require "uri"
require_relative "../examples/demo/support"

ROOT = DemoExample::ROOT
LAUNCHER = File.join(ROOT, "scripts/example.rb")
READY_TIMEOUT = 120
CLIENT_TIMEOUT = 60
STOP_TIMEOUT = 20
EVIDENCE_TIMEOUT = 10
ROTATION_TIMEOUT = 30
CERTS = File.join(ROOT, "examples/demo/certs.rb")
WORKERS = 4

# One child process in its own process group, with combined output captured
# by a reader thread and every wait bounded.
class Child
  def initialize(env, command)
    @reader, writer = IO.pipe
    @pid = Process.spawn(env, *command, out: writer, err: writer, in: File::NULL, pgroup: true, chdir: ROOT)
    writer.close
    @waiter = Process.detach(@pid)
    @output = +""
    @ended = false
    @lock = Mutex.new
    @changed = ConditionVariable.new
    @pump = Thread.new do
      @reader.each_line { |line| update { @output << line } }
    ensure
      update { @ended = true }
    end
  end

  def output = @lock.synchronize { @output.dup }

  # Returns the first match in the captured output, or nil once the output ends
  # or the deadline passes. Every wakeup rechecks the deadline.
  def wait_for(pattern, timeout)
    deadline = now + timeout
    scanned = 0
    @lock.synchronize do
      loop do
        # The pump appends whole lines, so each pass scans only lines added since the last.
        match = @output.match(pattern, scanned)
        scanned = @output.size
        remaining = deadline - now
        return match if match || @ended || remaining <= 0

        @changed.wait(@lock, remaining)
      end
    end
  end

  def wait_for_line(pattern, timeout)
    wait_for(pattern, timeout) ||
      raise("#{@ended ? 'exited' : "timed out after #{timeout}s"} before printing #{pattern.inspect}; " \
            "output:\n#{output}")
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

  private

  def update
    @lock.synchronize do
      yield
      @changed.broadcast
    end
  end
end

# evidence is the method name and facts the server must log for the call
# (around its request ID), or nil when no server line is checked.
Case = Struct.new(:client, :args, :status, :expected, :evidence, keyword_init: true)

def run_client(env, kase)
  child = nil
  # Defer Thread#kill until the spawned child is assigned, so ensure can always stop it.
  Thread.handle_interrupt(Object => :never) do
    child = Child.new(env, [RbConfig.ruby, LAUNCHER, "client", kase.client, *kase.args])
  end
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
THREADED = H1_UNARY.merge("countdown" => STREAM).transform_values { |variants| variants + GRPC }.freeze
# Independent of the support table: what each pairing must actually do.
ALLOWED = {
  "rack" => { "nethttp" => H1_UNARY, "async-http1" => H1_UNARY },
  "rails" => { "nethttp" => H1_UNARY, "async-http1" => H1_UNARY },
  "async" => { "async-h2" => H2_ALL, "async-threaded" => THREADED },
  "falcon" => { "nethttp" => H1_UNARY, "async-http1" => H1_STREAMING, "async-h2" => H2_ALL,
                "async-threaded" => THREADED }
}.freeze
NETHTTP_STREAMING = ["nethttp", %w[countdown],
                     "nethttp supports only unary Connect calls; countdown is server streaming"].freeze
REFUSED = {
  "rack" => [["async-h2", %w[greet], "async-h2 uses HTTP/2, but rack serves only HTTP/1.1"],
             ["async-threaded", %w[greet], "async-threaded uses HTTP/2, but rack serves only HTTP/1.1"],
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
              ["async-h2", %w[greet --protocol grpc --get], "gRPC has no GET form"],
              ["async-threaded", %w[collect],
               "async-threaded supports only unary, server streaming Connect calls; collect is client streaming"],
              ["async-threaded", %w[chat --protocol grpc],
               "async-threaded supports only unary, server streaming gRPC calls; chat is bidi streaming"]],
  "falcon" => [["async-http1", %w[chat], "async-http1 supports only unary, server streaming, client streaming " \
                                         "Connect calls; chat is bidi streaming"],
               ["async-http1", %w[greet --protocol grpc],
                "async-http1 does not support gRPC; native gRPC needs HTTP/2"],
               ["nethttp", %w[greet --protocol grpc], "nethttp does not support gRPC; native gRPC needs HTTP/2"],
               NETHTTP_STREAMING,
               ["async-h2", %w[countdown --get], "--get applies only to unary RPCs"],
               ["async-threaded", %w[chat],
                "async-threaded supports only unary, server streaming Connect calls; chat is bidi streaming"]]
}.freeze
# Under --mtls, a representative set of calls covers each client, RPC shape and
# option over TLS with client certificates.
MTLS_H2 = { "greet" => [[], %w[--get --gzip]], "fail" => [[]], "countdown" => [%w[--gzip]],
            "collect" => [%w[--codec json]], "chat" => [[], %w[--protocol grpc]] }.freeze
MTLS_H1 = { "greet" => [[], %w[--get]], "countdown" => [[]], "collect" => [%w[--gzip]] }.freeze
MTLS_THREADED = { "greet" => [[], %w[--get --gzip], %w[--protocol grpc]], "fail" => [[]],
                  "countdown" => [%w[--gzip], %w[--protocol grpc]] }.freeze
MTLS_ALLOWED = {
  "async" => { "async-h2" => MTLS_H2, "async-threaded" => MTLS_THREADED },
  "falcon" => { "async-h2" => MTLS_H2, "async-http1" => MTLS_H1, "async-threaded" => MTLS_THREADED }
}.freeze
NETHTTP_MTLS = ["nethttp", %w[greet], "nethttp cannot present a client certificate, so it cannot call an --mtls server"]
               .freeze
MTLS_REFUSED = {
  "async" => [NETHTTP_MTLS, ["async-http1", %w[greet], "async-http1 uses HTTP/1.1, but async serves only HTTP/2"]],
  "falcon" => [NETHTTP_MTLS, ["async-http1", %w[chat], "async-http1 supports only unary, server streaming, " \
                                                       "client streaming Connect calls; chat is bidi streaming"]]
}.freeze
# Each server run: the server, the signal that stops it, and whether it runs with --mtls.
RUNS = [["rack", "INT", false], ["rails", "TERM", false], ["async", "INT", false], ["falcon", "TERM", false],
        ["async", "INT", true], ["falcon", "TERM", true]].freeze
MTLS_CLIENT = "demo-client=CN=demo-client"
METHODS = { "greet" => "Greet", "fail" => "Greet", "countdown" => "Countdown", "collect" => "Collect",
            "chat" => "Chat" }.freeze

# The protocol, codec and encoding that a client option variant asks for.
def requested(variant)
  [variant.include?("grpc") ? "grpc" : "connect", variant.include?("json") ? "json" : "proto",
   variant.include?("--gzip") ? "gzip" : "identity"]
end

# What the server must report receiving: the client's banner alone cannot show
# that it actually sent the protocol, codec, method and encoding it was asked for.
def evidence(rpc, variant)
  protocol, codec, encoding = requested(variant)
  http = variant.include?("--get") ? "GET" : "POST"
  "#{METHODS.fetch(rpc)} protocol=#{protocol} http=#{http} codec=#{codec} encoding=#{encoding} " \
    "code=#{rpc == 'fail' ? 'invalid_argument' : 'OK'}"
end

# Under mTLS the handler also reports the client certificate the host verified.
def expected_output(server, rpc, variant, mtls)
  protocol, codec, encoding = requested(variant)
  banner = "at #{mtls ? 'https' : 'http'}://127.0.0.1:"
  options = "(#{protocol}, #{codec}, #{encoding}#{', GET' if variant.include?('--get')}#{', mTLS' if mtls})"
  served = "headers: demo-served-by=#{server}#{" #{MTLS_CLIENT}" if mtls}"
  expected = case rpc
             when "greet" then [served, "message: Hello, Ada!", "trailers: demo-greeted=Ada"]
             when "fail" then ["error: invalid_argument: name is required"]
             when "countdown" then [served, "message: 3\nmessage: 2\nmessage: 1\n", "trailers: demo-countdown=done"]
             when "collect" then [served, "message: Collected 2 names: Ada, Grace"]
             when "chat" then ["message: Hello, Ada!\nmessage: Hello, Grace!\n", served]
             end
  [rpc == "fail" ? 1 : 0, [banner, options, *expected]]
end

def cases_for(server, url, mtls)
  explicit = ["--server", server, "--url", url]
  allowed = (mtls ? MTLS_ALLOWED : ALLOWED).fetch(server).flat_map do |client, rpcs|
    rpcs.flat_map do |rpc, variants|
      variants.map do |variant|
        arguments = { "greet" => %w[Ada], "countdown" => %w[3], "collect" => %w[Ada Grace], "chat" => %w[Ada Grace] }
        status, expected = expected_output(server, rpc, variant, mtls)
        Case.new(client: client, args: [rpc, *arguments.fetch(rpc, []), *variant, *explicit],
                 status: status, expected: expected, evidence: evidence(rpc, variant))
      end
    end
  end
  # The first call finds the server through the state file alone; another passes only a matching --url.
  allowed.first.args -= explicit
  allowed << Case.new(client: allowed.first.client, args: ["greet", "Ada", "--url", url], status: 0,
                      expected: ["headers: demo-served-by=#{server}", "message: Hello, Ada!"],
                      evidence: evidence("greet", []))
  refused = (mtls ? MTLS_REFUSED : REFUSED).fetch(server).map do |client, args, reason|
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
  mtls_record = "#{state}.mtls"
  File.write(File.join(ROOT, mtls_record),
             JSON.generate(server: "falcon", url: url.sub("http://", "https://"), pid: Process.pid))
  invalid_url = "--url localhost:8080 is not an http:// or https:// URL with a host"
  port = URI(url).port.to_s
  malformed = [{ server: "rack", url: url, pid: Process.pid.to_s }, { server: "nope", url: url, pid: Process.pid },
               { server: "rack", pid: Process.pid }].each_with_index.map do |record, index|
    File.write(File.join(ROOT, "#{state}.malformed#{index}"), JSON.generate(record))
    [{ "CONNECTRPC_EXAMPLE_STATE" => "#{state}.malformed#{index}" },
     Case.new(client: "nethttp", args: %w[greet], status: 2,
              expected: ["#{File.basename(state)}.malformed#{index} does not hold a server name, URL and pid"])]
  end
  [
    *malformed,
    [{ "CONNECTRPC_EXAMPLE_STATE" => state },
     Case.new(client: "nethttp", args: %w[greet --url localhost:8080], status: 2, expected: [invalid_url])],
    [{ "CONNECTRPC_EXAMPLE_STATE" => state },
     Case.new(client: "nethttp", args: %w[greet --server rack --url localhost:8080], status: 2,
              expected: [invalid_url])],
    # --server without --url falls back to PORT, which here is where the state file says rack runs.
    [{ "CONNECTRPC_EXAMPLE_STATE" => state, "PORT" => port },
     Case.new(client: "nethttp", args: %w[greet --server rails], status: 2,
              expected: ["--server rails does not match the running example server at http://127.0.0.1:#{port} " \
                         "(rack); pass --url URL for the rails server, or drop --server"])],
    # PORT is decimal: a zero-padded PORT names the same port, so the conflict is still caught.
    [{ "CONNECTRPC_EXAMPLE_STATE" => state, "PORT" => "0#{port}" },
     Case.new(client: "nethttp", args: %w[greet --server rails], status: 2,
              expected: ["--server rails does not match the running example server at http://127.0.0.1:#{port} " \
                         "(rack)"])],
    # The conflict check compares addresses, so an http:// fallback still conflicts with an
    # https:// record. The record is synthetic: this verifier's own pid keeps it live.
    [{ "CONNECTRPC_EXAMPLE_STATE" => mtls_record, "PORT" => port },
     Case.new(client: "nethttp", args: %w[greet --server rack], status: 2,
              expected: ["--server rack does not match the running example server at http://127.0.0.1:#{port} " \
                         "(falcon)"])],
    # A zero-padded or uppercase --url still matches, and the client calls the recorded URL.
    [{ "CONNECTRPC_EXAMPLE_STATE" => state },
     Case.new(client: "nethttp", args: ["greet", "Ada", "--url", "HTTP://127.0.0.1:0#{port}/"], status: 0,
              expected: ["nethttp -> rack at #{url} ", "message: Hello, Ada!"],
              evidence: evidence("greet", []))],
    # An explicit --url that the state file attributes to another server is refused too.
    [{ "CONNECTRPC_EXAMPLE_STATE" => state },
     Case.new(client: "nethttp", args: ["greet", "--server", "rails", "--url", url], status: 2,
              expected: ["--server rails does not match the running example server at #{url} (rack)"])],
    [{ "CONNECTRPC_EXAMPLE_STATE" => state, "PORT" => "abc" },
     Case.new(client: "nethttp", args: %w[greet --server rails], status: 2,
              expected: ["PORT=abc is not a port number from 1 to 65535"])],
    # A matching record wins over PORT.
    [{ "CONNECTRPC_EXAMPLE_STATE" => state, "PORT" => "9" },
     Case.new(client: "nethttp", args: %w[greet Ada --server rack], status: 0,
              expected: ["nethttp -> rack at #{url}", "message: Hello, Ada!"], evidence: evidence("greet", []))],
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
     Case.new(client: "nethttp", args: %w[greet], status: 2, expected: ["names pid #{dead}, which has exited"])],
    # An https:// URL needs the demo client certificate, which only an --mtls server issues.
    [{ "CONNECTRPC_EXAMPLE_STATE" => "#{state}.nocerts/server.json" },
     Case.new(client: "async-h2", args: %w[greet --server falcon --url https://127.0.0.1:9], status: 2,
              expected: ["https://127.0.0.1:9 needs the demo client certificate", "--mtls"])],
    [{ "CONNECTRPC_EXAMPLE_STATE" => "#{state}.nocerts/server.json" },
     Case.new(client: "async-http1", args: %w[greet --server rack --url https://127.0.0.1:9], status: 2,
              expected: ["rack does not serve mutual TLS in these examples"])],
    # The scheme is case-insensitive, so an uppercase HTTPS:// URL is an mTLS call too.
    [{ "CONNECTRPC_EXAMPLE_STATE" => "#{state}.nocerts/server.json" },
     Case.new(client: "nethttp", args: %w[greet --server falcon --url HTTPS://127.0.0.1:9], status: 2,
              expected: ["nethttp cannot present a client certificate"])]
  ]
end

def label(server, kase) = "#{server} <- #{kase.client} #{kase.args.join(' ')}"

def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

# Renews the demo certificates under a running --mtls server, which must read
# the new version, present the renewed certificate on new handshakes and
# accept the renewed client certificate on a real call.
def rotation_problem(env, host, server, url, state)
  tls_dir = File.join(ROOT, File.dirname(state), "tls")
  known = host.output.scan(/^Read TLS certificates \(version (\h+)\)$/).flatten
  return "server logged no TLS certificate version" if known.empty?

  renew = Child.new(env, [RbConfig.ruby, "-r", CERTS, "-e", "DemoExample::Certs.renew!"])
  status = renew.wait(READY_TIMEOUT) || renew.kill
  return "renewing the certificates failed:\n#{renew.output}" unless status&.success?

  read = /^Read TLS certificates \(version (?!#{known.join('|')})\h+\)$/
  return "server logged no new version within #{ROTATION_TIMEOUT}s" unless host.wait_for(read, ROTATION_TIMEOUT)

  renewed_serial_problem(url, tls_dir) || call_after_renewal_problem(env, host, server, url)
ensure
  renew&.close
end

# The host publishes the new certificate after it validates what it read, so
# poll handshakes until it appears, within a bounded deadline.
def renewed_serial_problem(url, tls_dir)
  expected = OpenSSL::X509::Certificate.new(File.read(File.join(tls_dir, "server.pem"))).serial
  deadline = monotonic + ROTATION_TIMEOUT
  loop do
    served = served_serial(url, tls_dir)
    return if served == expected
    return "server still presents serial #{served}, not the renewed #{expected}" if monotonic > deadline

    sleep 0.1 # Poll interval only; the deadline bounds the wait.
  end
end

# Under TLS 1.3 the client finishes its handshake before the server checks the
# client certificate, so only a completed call shows the renewed one is accepted.
def call_after_renewal_problem(env, host, server, url)
  kase = Case.new(client: "async-h2", args: ["greet", "Ada", "--server", server, "--url", url], status: 0,
                  expected: ["headers: demo-served-by=#{server} #{MTLS_CLIENT}", "message: Hello, Ada!"],
                  evidence: evidence("greet", []))
  _label, problem, output = check(server, host, env, kase)
  "call after renewal: #{problem}\n#{output}" if problem
end

# Completes one mutual TLS handshake as the demo client and returns the serial
# of the certificate the server presented.
def served_serial(url, tls_dir)
  uri = URI(url)
  context = OpenSSL::SSL::SSLContext.new
  context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, ca_file: File.join(tls_dir, "ca.pem"))
  context.add_certificate(OpenSSL::X509::Certificate.new(File.read(File.join(tls_dir, "client.pem"))),
                          OpenSSL::PKey.read(File.read(File.join(tls_dir, "client-key.pem"))))
  context.alpn_protocols = ["h2"]
  socket = Socket.tcp(uri.host, uri.port, connect_timeout: 5)
  tls = OpenSSL::SSL::SSLSocket.new(socket, context)
  tls.sync_close = true
  tls.hostname = uri.host
  handshake(tls, socket, monotonic + 5)
  tls.peer_cert.serial
ensure
  tls ? tls.close : socket&.close
end

def handshake(tls, socket, deadline)
  tls.connect_nonblock
rescue IO::WaitReadable, IO::WaitWritable => e
  remaining = deadline - monotonic
  ready = remaining.positive? &&
          (e.is_a?(IO::WaitReadable) ? socket.wait_readable(remaining) : socket.wait_writable(remaining))
  raise "TLS handshake timed out" unless ready

  retry
end

# Servers that must exit before binding a port: the name, extra environment,
# server arguments, exit status and message.
SERVER_REFUSALS = [
  ["rack --mtls", { "PORT" => "0" }, %w[rack --mtls], 2, "Refused: rack does not serve mutual TLS in these examples"],
  ["rack PORT=abc", { "PORT" => "abc" }, %w[rack], 1, "PORT=abc is not a port number from 0 to 65535"],
  ["rack PORT=65536", { "PORT" => "65536" }, %w[rack], 1, "PORT=65536 is not a port number from 0 to 65535"]
].freeze

def server_refusal_problem(env, refusal)
  name, extra, args, expected_status, message = refusal
  child = Child.new(env.merge(extra), [RbConfig.ruby, LAUNCHER, "server", *args])
  status = child.wait(READY_TIMEOUT) || child.kill
  return "#{name} timed out" unless status
  unless status.exitstatus == expected_status
    return "#{name} exited #{status.exitstatus.inspect}, expected #{expected_status}:\n#{child.output}"
  end
  return if child.output.include?(message)

  "#{name} printed:\n#{child.output}"
ensure
  child&.close
end

def check(server, host, env, kase)
  status, output = run_client(env, kase)
  problem = if status.nil? then "timed out"
            elsif status.exitstatus != kase.status then "exit #{status.exitstatus.inspect}, expected #{kase.status}"
            elsif (missing = kase.expected.reject { |text| output.include?(text) }).any?
              "missing #{missing.map(&:inspect).join(', ')}"
            elsif kase.evidence then evidence_problem(host, output, kase.evidence)
            end
  [label(server, kase), problem, output]
end

# The server prints its line as the call finishes, possibly after the client exits.
def evidence_problem(host, output, expected)
  id = output[/^request: demo-request-id=(\h+)$/, 1]
  return "printed no demo-request-id" unless id

  line = host.wait_for(/^served \w+ request-id=#{id} .*$/, EVIDENCE_TIMEOUT)&.to_s
  return "server logged no call with demo-request-id=#{id} within #{EVIDENCE_TIMEOUT}s" unless line

  method, facts = expected.split(" ", 2)
  wanted = "served #{method} request-id=#{id} #{facts}"
  "server logged #{line.inspect}, expected #{wanted.inspect}" unless line == wanted
end

# Each worker records its own failures, so every worker finishes and every
# client it started is stopped by run_client.
def run_all(server, host, pairs)
  work = Queue.new
  pairs.each_with_index { |pair, index| work << [index, pair] }
  work.close
  results = []
  workers = Array.new(WORKERS) do
    Thread.new do
      while (item = work.pop)
        index, (env, kase) = item
        results[index] = begin
          check(server, host, env, kase)
        rescue StandardError => e
          [label(server, kase), "verifier error: #{e.class}: #{e.message}", e.full_message(highlight: false)]
        end
      end
    end
  end
  workers.each(&:join)
  results
ensure
  # An interrupt leaves workers mid-call; killing them runs run_client's cleanup.
  workers&.each { |worker| worker.kill.join(STOP_TIMEOUT) }
end

state_dir = "vendor/examples/verify-#{Process.pid}"
failures = []
totals = Hash.new(0)
puts "Runnable examples; Ruby #{RUBY_VERSION}; Rails lane #{ENV.fetch('RAILS_LANE', '8.1')}"
begin
  SERVER_REFUSALS.each do |refusal|
    problem = server_refusal_problem({ "CONNECTRPC_EXAMPLE_STATE" => "#{state_dir}/refused.json" }, refusal)
    puts "#{problem ? 'FAIL' : 'ok  '} #{refusal.first} refused"
    failures << problem if problem
  end
  RUNS.each do |server, signal, mtls|
    name = mtls ? "#{server} --mtls" : server
    # A relative override resolves against the repository root even though runners change directory.
    # The --mtls runs share the demo certificates in #{state_dir}/tls.
    state = "#{state_dir}/#{server}#{'-mtls' if mtls}.json"
    env = { "CONNECTRPC_EXAMPLE_STATE" => state }
    child = Child.new(env.merge("PORT" => "0"), [RbConfig.ruby, LAUNCHER, "server", server, *("--mtls" if mtls)])
    begin
      scheme = mtls ? "https" : "http"
      url = child.wait_for_line(%r{#{server} server listening on (#{scheme}://127\.0\.0\.1:\d+)}, READY_TIMEOUT)[1]
      record = JSON.parse(File.read(File.join(ROOT, state)))
      raise "#{name}: state file records #{record.inspect}" unless record["server"] == server && record["url"] == url

      allowed, refused = cases_for(server, url, mtls)
      pairs = [*allowed, *refused].map { |kase| [env, kase] }
      pairs += discovery_cases(url, state) if server == "rack"
      run_all(name, child, pairs).each do |label, problem, output|
        puts "#{problem ? 'FAIL' : 'ok  '} #{label}"
        failures << "#{label}: #{problem}\n#{output}" if problem
      end
      if mtls
        problem = rotation_problem(env, child, server, url, state)
        puts "#{problem ? 'FAIL' : 'ok  '} #{name} serves renewed certificates without a restart"
        failures << "#{name}: #{problem}\n#{child.output}" if problem
      end
      refusals = pairs.count { |_, kase| kase.status == 2 }
      totals[:allowed] += pairs.size - refusals
      totals[:refused] += refusals
      status = child.stop(signal, STOP_TIMEOUT)
      stopped = status&.success? && child.output.include?("#{server} server stopped")
      raise "#{name} did not stop cleanly on SIG#{signal}:\n#{child.output}" unless stopped
      raise "#{name} left its state file behind" if File.exist?(File.join(ROOT, state))

      puts "ok   #{name} stopped on SIG#{signal}"
      totals[:servers] += 1
    rescue StandardError => e
      failures << "#{name}: #{e.message}"
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
puts "Runnable examples passed: #{totals[:servers]} server runs, #{totals[:allowed]} supported calls, " \
     "#{totals[:refused]} refusals"
