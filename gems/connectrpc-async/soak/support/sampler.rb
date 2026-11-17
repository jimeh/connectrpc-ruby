# frozen_string_literal: true

module Soak
  # Every read of private library state the campaign makes lives here. Reads
  # of a reactor's objects run on that reactor's thread (see Topology::Peer and
  # #on_owner); problem lists name the object and the unmet condition, and an
  # empty list means the condition holds.
  module Sampler # rubocop:disable Metrics/ModuleLength -- the contract keeps every private read in one place
    INTERNAL = ConnectRPC::Async.const_get(:Internal)
    THREADED = ConnectRPC::Async::ThreadedTransport

    module_function

    def ivar(object, name) = object.instance_variable_get(name)

    # Global counts.

    def fds = Dir.children("/dev/fd").size
    def threads = Thread.list.count(&:alive?)

    # Descriptor targets; TCP sockets also show their local and remote ports and
    # state. The listing's own directory handle is gone by the readlink; skip it.
    def fd_links
      sockets = tcp_sockets
      Dir.children("/dev/fd").sort_by(&:to_i).filter_map do |fd|
        link = File.readlink("/dev/fd/#{fd}")
        inode = link[/\Asocket:\[(\d+)\]\z/, 1]
        [fd.to_i, sockets.key?(inode) ? "#{link} #{sockets[inode]}" : link]
      rescue SystemCallError
        nil
      end.to_h
    end

    TCP_STATES = %w[? ESTABLISHED SYN_SENT SYN_RECV FIN_WAIT1 FIN_WAIT2 TIME_WAIT CLOSE CLOSE_WAIT LAST_ACK LISTEN
                    CLOSING].freeze

    def tcp_sockets
      %w[/proc/net/tcp /proc/net/tcp6].select { |file| File.exist?(file) }.each_with_object({}) do |file, table|
        File.readlines(file).drop(1).each do |line|
          fields = line.split
          ports = fields[1..2].map { |address| address.split(":").last.to_i(16) }
          table[fields[9]] = "tcp #{ports[0]}->#{ports[1]} #{TCP_STATES[fields[3].to_i(16)]}"
        end
      end
    end

    # Unfinished tasks in one reactor's tree. Call it on that reactor's thread.
    def tasks(scheduler)
      count = 0
      scheduler.traverse { |node, _| count += 1 if node.is_a?(::Async::Task) && !node.finished? }
      count
    end

    def hierarchy(scheduler)
      out = StringIO.new
      scheduler.print_hierarchy(out, backtrace: true)
      out.string
    rescue StandardError => e
      "(hierarchy unavailable: #{e.class}: #{e.message})"
    end

    # Resource creation, observed without changing any library class.

    def falcon_http1 = (ConnectRPC::Falcon.const_get(:HTTP1Resource) if defined?(ConnectRPC::Falcon))
    def resource_classes = [INTERNAL::Resource, falcon_http1].compact

    # An owned resource has retired only once its own retirement completed.
    def retired?(resource) = resource.send(:retirement_completed?)

    # Falcon's session replaces the H2 resource it accepted with an HTTP/1
    # resource that takes over the same raw socket; the original never retires
    # itself. True when `other` is that replacement.
    def replacement?(resource, other)
      http1 = falcon_http1
      return false unless http1 && resource.is_a?(INTERNAL::Resource) && other.is_a?(http1)

      resource.connection.nil? && !resource.raw.nil? && other.raw.equal?(resource.raw)
    end

    # A superseded H2 resource has retired once the raw socket its replacement
    # took over is closed and every task it owns finished.
    def superseded_problems(resource)
      problems = []
      problems << "raw socket open" unless resource.raw.closed?
      running = h2_owned_tasks(resource).count { |task| !task.finished? }
      problems << "#{running} running tasks" unless running.zero?
      problems
    end

    # Every task an H2 resource owns: its owner task, tracked tasks, upkeep,
    # control writers, cleanup and drain.
    def h2_owned_tasks(resource)
      [resource.task, *resource.tasks, *ivar(resource, :@maintenance), *ivar(resource, :@controls),
       ivar(resource, :@cleanup), ivar(resource, :@drain_task)].compact
    end

    # HTTP/2 resources (client and server).

    def h2_live_problems(resource, label)
      problems = []
      problems << "#{label} is finished" if resource.finished?
      connection = resource.connection
      return problems << "#{label} has no connection" unless connection

      problems << "#{label} has #{connection.streams.size} open streams" unless connection.streams.empty?
      problems << "#{label} has registered frame writers" unless connection.writers.empty?
      problems << "#{label} has guard waiters" if connection.guard_waiting?
      problems << "#{label} has unfinished stream output" if connection.outputs_pending?
      problems << "#{label} has running tracked tasks" unless resource.tasks.all?(&:finished?)
      problems
    end

    def h2_retired_problems(resource, label)
      problems = []
      problems << "#{label} retirement not completed" unless retired?(resource)
      problems << "#{label} raw socket open" if resource.raw && !resource.raw.closed?
      connection = resource.connection
      problems << "#{label} reader running" if connection&.reader_task && !connection.reader_task.finished?
      problems << "#{label} writers registered" if connection && !connection.writers.empty?
      problems << "#{label} output unfinished" if connection&.outputs_pending?
      tasks = h2_owned_tasks(resource)
      problems << "#{label} has #{tasks.count { |task| !task.finished? }} running tasks" unless tasks.all?(&:finished?)
      problems
    end

    def client_peer_serial(resource)
      socket = resource.socket
      socket.respond_to?(:peer_cert) ? socket.peer_cert&.serial&.to_i : nil
    end

    def calmed?(resource) = resource.calmed?
    def keepalive_time(resource) = resource.keepalive&.time

    # Async Transport, read on its own reactor.

    def transport_resources(transport) = ivar(transport, :@resources)
    def transport_calls(transport) = ivar(transport, :@calls)
    def manager(transport) = ivar(transport, :@manager)
    def live_resources(transport) = transport_resources(transport).reject { |resource| retired?(resource) }
    def live_connections(transport) = manager(transport).connections
    def pending_bytes(transport) = manager(transport).pending_bytes

    def transport_registries(transport)
      manager = manager(transport)
      {
        calls: transport_calls(transport).size, acquiring: ivar(transport, :@acquiring).size,
        usage: manager.usage, pending_bytes: manager.pending_bytes,
        draining: ivar(manager, :@draining).size, reserved: ivar(manager, :@reserved).size,
        waiters: ivar(manager, :@waiters).size, drain_timers: ivar(manager, :@drain_timers).size,
        attempt: ivar(manager, :@attempt) ? 1 : 0, timer: ivar(manager, :@timer) ? 1 : 0
      }
    end

    # Problems with an Async Transport. A closed transport must have retired
    # everything; a live one must be quiescent with only usable connections.
    def transport_problems(transport, label)
      problems = transport_registries(transport).filter_map do |name, value|
        "#{label} #{name}=#{value}" unless value.zero?
      end
      state = ivar(transport, :@state)
      if state == :closed
        problems << "#{label} lifetime running" unless ivar(transport, :@lifetime).finished?
        problems << "#{label} still accounts connections" unless live_connections(transport).empty?
        transport_resources(transport).each_with_index do |resource, index|
          problems.concat(h2_retired_problems(resource, "#{label} resource #{index}"))
        end
      else
        live = live_resources(transport)
        connections = live_connections(transport).keys
        live.each_with_index do |resource, index|
          next problems << "#{label} resource #{index} is retiring" unless connections.include?(resource.connection)

          problems.concat(h2_live_problems(resource, "#{label} resource #{index}"))
        end
      end
      problems
    end

    def acquiring(transport) = ivar(transport, :@acquiring).size
    def usage(transport) = manager(transport).usage
    def guard_waiters(resource) = ivar(resource.connection, :@frame_guard).waiting.size

    # Routes a client connection's writes through a FrameGate.
    def install_gate(resource, gate)
      resource.connection.stream.instance_variable_set(:@io, gate)
      gate.armed = true
      gate
    end

    # Identity of a live transport's usable connections, for reuse checks.
    def connection_identity(transport) = live_connections(transport).keys.map(&:object_id).sort

    # Retained request/response bytes of one client stream's input queue.
    def stream_retained(stream)
      input = ivar(stream, :@input)
      chunks = input && ivar(ivar(input, :@queue), :@chunks)
      chunks ? chunks.sum(&:bytesize) : 0
    end

    def stream_window(stream) = [stream.remote_window.available, stream.local_window.available]

    # The client stream of the transport's only in-flight stream operation with a response.
    def client_streams(transport)
      transport_calls(transport).keys.filter_map { |operation| operation[:response]&.stream }
    end

    def output_maximum(body) = body.maximum_queued_bytes

    # ThreadedTransport. Inner state is read on the owner thread when it runs.

    def engine(transport) = ivar(transport, :@engine)

    def admission(transport)
      admission = ivar(transport, :@admission)
      return [0, 0] unless admission

      admission.lock.synchronize { [admission.count, admission.bytes] }
    end

    # Runs a block inline on a live owner's dispatcher fiber; nil when no owner runs.
    def on_owner(transport, timeout: 10)
      engine = engine(transport)
      return unless engine&.thread&.alive?

      result = Thread::Queue.new
      engine.run_on_owner(lambda {
        result << begin
          [:ok, yield(engine.inner)]
        rescue StandardError => e
          [:error, e]
        end
      })
      tag, value = result.pop(timeout: timeout)
      raise "Timed out reading the ThreadedTransport owner" unless tag
      raise value if tag == :error

      value
    end

    def threaded_problems(transport, label)
      problems = []
      count, bytes = admission(transport)
      problems << "#{label} admission count=#{count} bytes=#{bytes}" unless count.zero? && bytes.zero?
      engine = engine(transport)
      return problems unless engine

      calls = engine.admission.lock.synchronize { engine.calls.size }
      problems << "#{label} engine calls=#{calls}" unless calls.zero?
      closed = %i[closed].include?(ivar(transport, :@state))
      if closed
        problems << "#{label} owner thread alive" if engine.thread&.alive?
        problems.concat(transport_problems(engine.inner, "#{label} inner")) if engine.inner
      else
        problems << "#{label} owner thread stopped" unless engine.thread&.alive?
        inner = on_owner(transport) { |transport_inner| transport_problems(transport_inner, "#{label} inner") }
        problems.concat(inner || [])
      end
      problems
    end

    def owner_tasks(transport) = on_owner(transport) { Sampler.tasks(Fiber.scheduler) }
    def threaded_identity(transport) = on_owner(transport) { |inner| connection_identity(inner) }
    def owner_thread(transport) = engine(transport)&.thread

    # Async HTTP1Transport (client HTTP/1.1).

    def http1_problems(transport, label)
      problems = []
      calls = ivar(transport, :@calls)
      leases = ivar(transport, :@leases)
      idle = ivar(transport, :@idle)
      problems << "#{label} calls=#{calls.size}" unless calls.empty?
      if ivar(transport, :@state) == :closed
        problems << "#{label} leases=#{leases.size} idle=#{idle.size}" unless leases.empty? && idle.empty?
        problems << "#{label} lifetime running" unless ivar(transport, :@lifetime).finished?
        transport_resources(transport).each_with_index do |resource, index|
          problems << "#{label} resource #{index} raw open" if resource.raw && !resource.raw.closed?
        end
      else
        problems << "#{label} leases not idle (#{leases.size} vs #{idle.size})" unless leases.size == idle.size
        transport_resources(transport).each_with_index do |resource, index|
          next if idle.include?(resource)

          problems << "#{label} resource #{index} raw open" if resource.raw && !resource.raw.closed?
        end
      end
      problems
    end

    def http1_idle_identity(transport) = ivar(transport, :@idle).map(&:object_id).sort

    # Server sessions (Async Host and Falcon Server), read on the server reactor.

    def session(server) = ivar(server, :@session)
    def session_resources(server) = ivar(session(server), :@resources)
    def session_live(server) = session_resources(server).reject { |resource| retired?(resource) }
    def tls_configuration(server) = ivar(session(server), :@tls)

    def session_problems(server, label, connections: nil)
      session = session(server)
      problems = %i[@h1_wire @identities].filter_map do |name|
        size = ivar(session, name).size
        "#{label} #{name}=#{size}" unless size.zero?
      end
      admitted = live_admissions(session)
      problems << "#{label} live admissions=#{admitted}" unless admitted.zero?
      tls = tls_configuration(server)
      if ivar(session, :@state) == :closed
        problems << "#{label} lifetime running" unless ivar(session, :@lifetime).finished?
        tasks = [ivar(tls, :@timer), ivar(tls, :@attempt), ivar(tls, :@callback), ivar(tls, :@lifetime)].compact
        problems << "#{label} TLS refresh tasks running" unless tasks.all?(&:finished?)
        session_resources(server).each_with_index do |resource, index|
          problems.concat(server_retired_problems(resource, "#{label} resource #{index}"))
        end
      else
        problems << "#{label} TLS refresh attempt running" if ivar(tls, :@attempt) || ivar(tls, :@callback)
        live = session_live(server)
        if connections && live.size != connections
          problems << "#{label} live connections=#{live.size}, expected #{connections}"
        end
        live.each_with_index do |resource, index|
          problems.concat(server_live_problems(resource, "#{label} live #{index}"))
        end
      end
      problems
    end

    # The session prunes completed admissions of closed HTTP/2 streams at its
    # next dispatch; until then they hold no processor or wire stream.
    def live_admissions(session)
      ivar(session, :@admitted).count { |key, completed| !completed || (key.respond_to?(:closed?) && !key.closed?) }
    end

    def server_live_problems(resource, label)
      return h2_live_problems(resource, label) if resource.is_a?(INTERNAL::Resource)

      problems = []
      problems << "#{label} active" if resource.active
      problems << "#{label} has running tracked tasks" unless ivar(resource, :@tasks).all?(&:finished?)
      problems << "#{label} retains input" if resource.input_retirement
      problems << "#{label} retains an exchange" if resource.exchange
      problems
    end

    def server_retired_problems(resource, label)
      return h2_retired_problems(resource, label) if resource.is_a?(INTERNAL::Resource)

      problems = []
      problems << "#{label} retirement not completed" unless retired?(resource)
      problems << "#{label} raw socket open" if resource.raw && !resource.raw.closed?
      tasks = [*ivar(resource, :@tasks), *ivar(resource, :@retirement_members).keys, ivar(resource, :@cleanup),
               ivar(resource, :@drain_task)].compact
      problems << "#{label} has running tasks" unless tasks.all?(&:finished?)
      problems
    end

    def tls_attempt?(server) = !ivar(tls_configuration(server), :@attempt).nil?

    # The most input bytes any live HTTP/2 server stream retains unread.
    # Live server resources that a drain (drain_tls, close) has marked.
    def draining(server)
      session_live(server).select { |resource| resource.respond_to?(:draining) && resource.draining }
    end

    # The live server stream retaining the most unread input, with its
    # remaining receive credit, or nil when no stream exists.
    def server_ingress(server)
      session_live(server).select { |resource| resource.is_a?(INTERNAL::Resource) && resource.connection }
                          .flat_map { |resource| resource.connection.streams.values }
                          .map { |stream| { retained: stream_retained(stream), credit: stream_window(stream).last } }
                          .max_by { |state| state[:retained] }
    end

    # High-water mark of the payload bytes any one HTTP/2 ingress queue
    # (client response or server request input) retains, observed after every
    # push, which is the only way a queue grows. Covers every thread.
    class IngressMeter
      def initialize
        @lock = Mutex.new
        @peak = 0
        lock = @lock
        meter = self
        @trace = TracePoint.new(:return) do |event|
          bytes = event.self.instance_variable_get(:@chunks).sum(&:bytesize)
          lock.synchronize { meter.raise_peak(bytes) }
        end
        @trace.enable(target: INTERNAL::IngressQueue.instance_method(:push))
      end

      def peak = @lock.synchronize { @peak }
      def stop = @trace.disable

      def raise_peak(bytes)
        @peak = bytes if bytes > @peak
      end
    end

    # Whether a completed peer waits, untaken, in one of the server's listen
    # backlogs: a Host's bound listeners or a Falcon Server's endpoint.
    def listener_queued?(server)
      sockets = server.is_a?(ConnectRPC::Async::Host) ? ivar(server, :@bound).sockets : server.endpoint.sockets
      !IO.select(sockets, nil, nil, 0).nil?
    end

    # High-water mark of a server session's accounted connections, observed
    # after every accept, so admission above `max_connections` cannot hide
    # between samples. Create and read it on the server's peer.
    class AdmissionMeter
      attr_reader :peak

      def initialize(server)
        @server = server
        @peak = 0
        meter = self
        Sampler.session(server).define_singleton_method(:accept) do |raw|
          super(raw).tap { meter.observe }
        end
      end

      def observe
        @peak = [@peak, Sampler.session_live(@server).size].max
      end
    end

    # RST_STREAM codes a client stream receives from now on.
    def watch_resets(stream)
      codes = Thread::Queue.new
      stream.define_singleton_method(:receive_reset_stream) do |frame|
        codes << frame.unpack
        super(frame)
      end
      codes
    end

    # Cancellation: listeners still registered on a shared source.
    def registrations(source) = ivar(ivar(source, :@signal), :@registrations).size
  end
end
