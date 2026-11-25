# frozen_string_literal: true

module HyperHelpers # rubocop:disable Metrics/ModuleLength
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new
  RAW_UNARY = ConnectRPC::Method.new(service: "raw.v1.Raw", name: "Call", request_type: MSG, response_type: MSG)
  RAW_SERVER_STREAM = ConnectRPC::ServerStreamMethod.new(service: "raw.v1.Raw", name: "Stream", request_type: MSG,
                                                         response_type: MSG)
  RAW_CLIENT_STREAM = ConnectRPC::ClientStreamMethod.new(service: "raw.v1.Raw", name: "Upload", request_type: MSG,
                                                         response_type: MSG)
  SHIM_SOURCE = File.expand_path("stall_dns.c", __dir__)
  SHIM = File.join(ROOT, "vendor/hyper/shim/stall_dns.so")
  THREADS_SHIM_SOURCE = File.expand_path("fail_threads.c", __dir__)
  THREADS_SHIM = File.join(ROOT, "vendor/hyper/shim/fail_threads.so")

  def self.included(base)
    base.after do
      @transports&.each { |transport| transport.close(grace: 0) }
    end
  end

  def peer = Peer.start
  def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  def native = ConnectRPC::Hyper.const_get(:Native)

  # A transport closed (grace 0) after the example.
  def hyper(**)
    transport = ConnectRPC::Hyper::Transport.new(**)
    (@transports ||= []) << transport
    transport
  end

  def driver(transport) = transport.instance_variable_get(:@driver)
  def stats(transport) = driver(transport).stats

  def core(origin, transport:, protocol: :grpc, **)
    ConnectRPC::Client.new(base_url: origin, binding: BINDING, transport: transport, protocol: protocol, **)
  end

  def client(origin, transport:, protocol: :grpc, **)
    PEER::Client.new(client: core(origin, transport: transport, protocol: protocol, **))
  end

  def raw_call(origin, scenario, transport:, protocol: :grpc, timeout: 5, **)
    core(origin, transport: transport, protocol: protocol)
      .call(RAW_UNARY, MSG.new, metadata: { "x-scenario" => scenario }, timeout: timeout, **)
  end

  def tls_options(mtls: false)
    options = { ca_file: File.join(peer.certs, "ca.pem") }
    return options unless mtls

    options.merge(certificate_chain: [File.read(File.join(peer.certs, "client.pem"))],
                  private_key: File.read(File.join(peer.certs, "client.key")))
  end

  # ResponseReader yields messages but is not Enumerable.
  def values(reader)
    received = []
    reader.each { |message| received << message.value } # rubocop:disable Style/MapIntoArray
    received
  end

  def error_of
    yield
    nil
  rescue ConnectRPC::Error => e
    e
  end

  # Polls a condition under a bounded deadline; reports the label on timeout.
  def wait_until(label, timeout: 10)
    deadline = monotonic + timeout
    until (result = yield)
      raise "Timed out waiting for #{label}" if monotonic > deadline

      sleep 0.001
    end
    result
  end

  DIAL_COUNTS = %w[dial:started dial:joined].freeze

  # Counts pool dials started and the callers that joined one while the block
  # runs; zero-bound hold points only count. Returns the block's value.
  def count_dial_joins(native_driver)
    DIAL_COUNTS.each { |point| native_driver.hold(point, 0) }
    yield
  ensure
    DIAL_COUNTS.each { |point| native_driver.release(point) }
  end

  # Waits until `count` callers joined one shared pool dial.
  def wait_joined(native_driver, count)
    wait_until("#{count} callers joined the pool dial") { native_driver.reached("dial:joined") >= count }
    expect(DIAL_COUNTS.map { |point| native_driver.reached(point) }).to eq([1, count])
  end

  # A thread parked in a native wait (GVL released) under the given frame.
  # The wait may not have polled its operation yet; a spec that needs a step
  # done (such as joining the pool dial) waits for its hold point instead.
  def wait_parked(thread, frame, timeout: 10)
    wait_until("#{thread.inspect} parked in #{frame}: #{thread.backtrace&.first(3).inspect}", timeout: timeout) do
      trace = thread.backtrace || []
      thread.status == "sleep" && trace.first.to_s.include?("Native::") && trace.any? { |line| line.include?(frame) }
    end
  end

  # A listener whose accept queue is full: SYNs are dropped and connect stays
  # pending. Returns [port, cleanup].
  def full_backlog
    server = TCPServer.new("127.0.0.1", 0)
    server.listen(0)
    fillers = Array.new(8) do
      socket = Socket.new(:INET, :STREAM)
      begin
        socket.connect_nonblock(Socket.sockaddr_in(server.addr[1], "127.0.0.1"))
      rescue IO::WaitWritable, Errno::EINPROGRESS
        nil
      end
      socket
    end
    [server.addr[1], -> { (fillers + [server]).each(&:close) }]
  end

  def syn_sent?(port) = syn_sent(port).positive?

  # Sockets connecting to the loopback port (SYN_SENT), fillers included.
  def syn_sent(port)
    hex = format("%04X", port)
    File.readlines("/proc/net/tcp").count do |line|
      fields = line.split
      fields[2]&.end_with?(":#{hex}") && fields[3] == "02"
    end
  end

  # rubocop:disable-next Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def credentials(name, issuer: nil, authority: false, usage: nil, key: nil, san: nil, version: 2, serial: nil,
                  not_after: Time.now + 3600)
    key ||= OpenSSL::PKey::EC.generate("prime256v1")
    certificate = OpenSSL::X509::Certificate.new
    certificate.version = version
    certificate.serial = serial || rand(1 << 32)
    certificate.subject = OpenSSL::X509::Name.parse("/CN=#{name}")
    certificate.issuer = issuer ? issuer.first.subject : certificate.subject
    certificate.public_key = key
    certificate.not_before = Time.now - 60
    certificate.not_after = not_after
    if version == 2
      factory = OpenSSL::X509::ExtensionFactory.new
      factory.subject_certificate = certificate
      factory.issuer_certificate = issuer ? issuer.first : certificate
      certificate.add_extension(factory.create_extension("basicConstraints", authority ? "CA:TRUE" : "CA:FALSE",
                                                         true))
      usage_bits = authority ? "keyCertSign,cRLSign" : "digitalSignature,keyEncipherment"
      certificate.add_extension(factory.create_extension("keyUsage", usage_bits, true))
      certificate.add_extension(factory.create_extension("extendedKeyUsage", usage)) if usage
      certificate.add_extension(factory.create_extension("subjectAltName", san)) if san
    end
    signer = issuer ? issuer.last : key
    certificate.sign(signer, %w[ED25519 ED448].include?(signer.oid) ? nil : "SHA256")
    [certificate, key]
  end

  # Children log through console (async's task warnings) in its serialized
  # format. Under GitHub Actions console otherwise picks its XTerm output even
  # for a pipe, whose IO#winsize raises Errno::EBADF on Windows; the warning
  # then replaces the task's own error and can leave a reactor running.
  CHILD_ENV = { "CONSOLE_OUTPUT" => "Serialized" }.freeze

  # Runs Ruby code in a child process with this bundle; returns [stdout,
  # status]. The child's stderr is reported when it fails, and what it wrote
  # so far when it times out.
  def ruby_child(code, env: {}, timeout: 60)
    env = CHILD_ENV.merge(env)
    preamble = <<~RUBY
      $LOAD_PATH.unshift(#{File.join(ROOT, 'interop/grpc/ruby').dump})
      require "connectrpc/hyper"
      require "connectrpc/bindings/google_protobuf"
      require "peer_connect"
      $stdout.sync = true
      MSG = Connectrpc::Grpcinterop::V1::Message
      PEER = Connectrpc::Grpcinterop::V1::Peer
      def client(origin, transport, protocol: :grpc)
        PEER::Client.new(client: ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: protocol,
                                                        binding: ConnectRPC::Bindings::GoogleProtobuf.new))
      end
    RUBY
    output = +""
    errors = +""
    status = nil
    Open3.popen3(env, RbConfig.ruby, "-e", preamble + code) do |stdin, stdout, stderr, wait|
      stdin.close
      readers = [[output, stdout], [errors, stderr]].map do |buffer, stream|
        Thread.new do
          Thread.current.report_on_exception = false
          loop { buffer << stream.readpartial(4096) }
        rescue IOError
          nil
        end
      end
      unless wait.join(timeout)
        Process.kill(:KILL, wait.pid)
        readers.each { |reader| reader.join(5) }
        raise "Child Ruby timed out; output so far: #{output}; errors: #{errors}"
      end
      readers.each(&:join)
      status = wait.value
    end
    warn "Child Ruby failed (#{status.inspect}): #{errors}" unless status.success?
    [output, status]
  end

  # The getaddrinfo shim: names ending in ".stall" announce "dns-enter" on the
  # STALL_EVENTS FIFO, then block until a byte arrives on STALL_RELEASE.
  def stall_environment
    skip "The getaddrinfo shim needs Linux and glibc" unless RbConfig::CONFIG["host_os"].include?("linux")
    compile_shim(SHIM_SOURCE, SHIM)
    directory = Dir.mktmpdir("connectrpc-hyper-stall-")
    events = File.join(directory, "events")
    release = File.join(directory, "release")
    File.mkfifo(events)
    File.mkfifo(release)
    { "LD_PRELOAD" => SHIM, "STALL_EVENTS" => events, "STALL_RELEASE" => release }
  end

  # The pthread_create shim (fail_threads.c): refuses the extension threads
  # FAIL_THREADS selects and logs each extension thread to the returned file.
  # Returns [environment, log path].
  def fail_threads_environment(allow, fail)
    skip "The pthread_create shim needs Linux and glibc" unless glibc?
    compile_shim(THREADS_SHIM_SOURCE, THREADS_SHIM)
    log = File.join(Dir.mktmpdir("connectrpc-hyper-threads-"), "threads.log")
    [{ "LD_PRELOAD" => THREADS_SHIM, "FAIL_THREADS" => "#{allow},#{fail}", "FAIL_THREADS_LOG" => log }, log]
  end

  def glibc?
    return false unless RbConfig::CONFIG["host_os"].include?("linux")

    IO.popen(%w[getconf GNU_LIBC_VERSION], err: File::NULL, &:read).start_with?("glibc")
  rescue SystemCallError
    false
  end

  def compile_shim(source, shim)
    return if File.exist?(shim) && File.mtime(shim) >= File.mtime(source)

    FileUtils.mkdir_p(File.dirname(shim))
    system("cc", "-shared", "-fPIC", "-o", shim, source, "-ldl", exception: true)
  end
end
