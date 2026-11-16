# frozen_string_literal: true

require "tmpdir"

module Soak
  # Servers, peers, clients and the shapes the scenarios drive.
  module Topology
    ECHO = AsyncHelpers::ECHO
    MESSAGE = AsyncHelpers::MESSAGE
    SS, CS, BIDI = [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod,
                    ConnectRPC::BidiStreamMethod].map do |type|
      type.new(service: "soak.v1.Streams", name: type.name.split("::").last, request_type: MESSAGE,
               response_type: MESSAGE)
    end
    STREAMS = ConnectRPC::Service.new(name: "soak.v1.Streams", methods: [SS, CS, BIDI])
    OP = "x-soak-op"
    COUNT = "x-soak-count"
    SIZE = "x-soak-size"
    FLOOD_MESSAGE = 200_000

    # Test credentials from the fast suite's helpers, plus a CA file that
    # lives for the whole process.
    TLS = Object.new.extend(AsyncHelpers)
    CA_DIRECTORY = Dir.mktmpdir("connectrpc-soak-ca-")
    CA_FILE = File.join(CA_DIRECTORY, "ca.pem")
    File.write(CA_FILE, TLS.tls_material.fetch(:root).first.to_pem)
    at_exit { FileUtils.rm_rf(CA_DIRECTORY) }

    module_function

    def server_tls(mode)
      case mode
      when :tls then { tls: TLS.host_tls }
      when :mtls then { tls: TLS.host_tls(anchors: [TLS.tls_material.fetch(:client_ca)]) }
      else {}
      end
    end

    def client_tls(mode, identity: TLS.tls_material.fetch(:identity))
      case mode
      when :h2c, :tcp then {}
      when :tls then { ca_file: CA_FILE }
      else { ca_file: CA_FILE, **TLS.host_tls(identity).except(:client_ca) }
      end
    end

    def scheme(mode) = %i[h2c tcp].include?(mode) ? "http" : "https"
    def message(size) = MESSAGE.new(name: "x" * size)

    # A peer reactor on its own thread (ThreadedHelpers::PeerThread) that runs
    # commands one at a time on one long-lived task, which therefore owns every
    # server created through it.
    class Peer
      attr_reader :label, :scheduler

      def initialize(label)
        @label = label
        inbox = @inbox = Thread::Queue.new
        @thread = ThreadedHelpers::PeerThread.new do |task|
          runner = task.async do
            while (command = inbox.pop)
              command.call
            end
          end
          [Fiber.scheduler, lambda {
            inbox.close
            runner.wait
          }]
        end
        @scheduler = @thread.peer
        Diagnostics.register("peer #{label}", @scheduler)
      end

      def run(timeout: 60)
        result = Thread::Queue.new
        @inbox << lambda {
          result << begin
            [:ok, yield]
          rescue Exception => e # rubocop:disable Lint/RescueException -- the harness re-raises it
            [:error, e]
          end
        }
        tag, value = result.pop(timeout: timeout)
        raise Failure, "Timed out after #{timeout}s waiting for a #{label} peer command" unless tag
        raise value if tag == :error

        value
      end

      def stop
        Diagnostics.unregister("peer #{label}")
        @thread.stop
      end
    end

    # Unary, stream and identity handlers that record every execution in the
    # ledger and hold on the shared gates. Both protocols are served.
    class Handlers
      attr_accessor :host

      def initialize(ledger, gates)
        @ledger = ledger
        @gates = gates
      end

      def server(**)
        handlers = ECHO.handlers(echo: unary, fail: unary).merge(SS.path => server_stream, CS.path => client_stream,
                                                                 BIDI.path => bidi)
        ConnectRPC::Server.new(services: [ECHO::SERVICE, STREAMS], binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                               handlers: handlers, protocol: :auto, **)
      end

      def unary
        lambda do |request, context|
          observe(context) do |id, op|
            @gates.wait(id, context) if op == "hold"
            op == "identity" ? MESSAGE.new(name: host.peer_certificate&.serial.to_s) : request
          end
        end
      end

      def server_stream
        lambda do |_request, writer, context|
          observe(context) do |id, op|
            count = Integer(context.request_metadata[COUNT]&.first || (op == "flood" ? 20 : 3))
            size = Integer(context.request_metadata[SIZE]&.first || 16)
            case op
            when "flood" then count.times { writer.send_message(Topology.message(FLOOD_MESSAGE)) }
            when "hold"
              writer.send_message(Topology.message(size))
              @gates.wait(id, context)
            else count.times { writer.send_message(Topology.message(size)) }
            end
          end
        end
      end

      def client_stream
        lambda do |reader, context|
          observe(context) do |id, op|
            case op
            when "ignore"
              @gates.wait(id, context)
              MESSAGE.new(name: "ignored")
            when "first" then reader.receive
            else
              count = 0
              reader.each { count += 1 }
              MESSAGE.new(name: count.to_s)
            end
          end
        end
      end

      def bidi
        lambda do |duplex, context|
          observe(context) do
            duplex.each { |message| duplex.send_message(message) }
          end
        end
      end

      private

      def observe(context)
        id = context.request_metadata[Ledger::ID]&.first
        op = context.request_metadata[OP]&.first
        @ledger.enter(id) if id
        reason = :ok
        begin
          yield id, op
        rescue Exception => e # rubocop:disable Lint/RescueException -- recorded, then re-raised
          reason = e.class.name
          raise
        ensure
          @ledger.finish(id, context.cancellation_reason || reason) if id
        end
      end
    end

    # A server living on a peer: an Async Host or a Falcon Server.
    class Server
      attr_reader :peer, :host, :port, :mode, :kind, :label

      def initialize(peer, kind:, mode:, handlers:, label:, **)
        @peer = peer
        @kind = kind
        @mode = mode
        @label = label
        @host, @port, @bound = peer.run { start(handlers, **) }
        handlers.host = @host
      end

      def origin = "#{Topology.scheme(@mode)}://127.0.0.1:#{@port}"

      # Runs the block on the peer with the host.
      def run(timeout: 60) = @peer.run(timeout: timeout) { yield @host }

      def close(grace: 0)
        run do |host|
          host.close(grace: grace)
        ensure
          @bound&.close
        end
      end

      def closed? = run { Sampler.ivar(Sampler.session(@host), :@state) == :closed }

      def problems(label, connections: nil)
        run do
          problems = Sampler.session_problems(@host, label, connections: connections)
          if @bound && Sampler.ivar(Sampler.session(@host), :@state) == :closed && !@bound.sockets.all?(&:closed?)
            problems << "#{label} listener open after close"
          end
          [problems, Sampler.session_live(@host)]
        end
      end

      private

      def start(handlers, tls_provider: nil, **)
        tls = tls_provider ? { tls_provider: tls_provider } : Topology.server_tls(@mode)
        target = handlers.server
        if @kind == :falcon
          bound = ConnectRPC::Falcon::Endpoint.parse("#{Topology.scheme(@mode)}://127.0.0.1:0").bound
          host = ConnectRPC::Falcon::Server.new(server: target, endpoint: bound, **tls, **)
          host.run
          [host, bound.sockets.first.local_address.ip_port, bound]
        else
          host = ConnectRPC::Async::Host.new(server: target, **tls, **).start
          [host, host.port, nil]
        end
      end
    end

    # Typed clients over one transport, cached per protocol and GET use.
    class Clients
      attr_reader :transport, :origin

      def initialize(transport, origin)
        @transport = transport
        @origin = origin
        @cores = {}
      end

      def core(protocol: :connect, use_get: false)
        @cores[[protocol, use_get]] ||= ConnectRPC::Client.new(
          base_url: @origin, transport: @transport, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
          protocol: protocol, use_get: use_get
        )
      end
    end
  end

  # Builders and call shapes, mixed into Campaign.
  module Builders
    H2_SHAPES = %i[unary get server_stream client_stream bidi grpc_unary grpc_server_stream].freeze
    THREADED_SHAPES = %i[unary get server_stream grpc_unary grpc_server_stream].freeze
    H1_SHAPES = %i[unary get server_stream client_stream].freeze

    def peer_thread(label)
      peer(Topology::Peer.new(label)).tap { |created| cleanup(:stop) { created.stop } }
    end

    # A tracked server; persistent ones close at teardown.
    # Objects built without `persistent` must close by the end of their round
    # unless `outlives_round` says the scenario hands them to a later round.
    def server(peer, kind: :host, mode: :h2c, persistent: true, connections: nil, label: nil, **)
      handlers = Topology::Handlers.new(ledger, gates)
      label ||= "#{kind} #{mode}"
      created = Topology::Server.new(peer, kind: kind, mode: mode, handlers: handlers, label: label, **)
      track(:server, created, label, connections: connections, round: !persistent)
      cleanup { created.close } if persistent
      created
    end

    def transport(origin, mode: :h2c, label: "transport", persistent: false, reuse: false, identity: nil,
                  connections: nil, outlives_round: false, **)
      tls = identity ? Topology.client_tls(mode, identity: identity) : Topology.client_tls(mode)
      created = ConnectRPC::Async::Transport.new(origin: origin, **tls, **)
      track(:transport, created, label, reuse: reuse, connections: connections, round: !persistent && !outlives_round)
      cleanup { created.close(grace: 0) } if persistent
      Topology::Clients.new(created, origin)
    end

    def threaded(origin, mode: :h2c, label: "threaded", persistent: false, reuse: false, **)
      created = ConnectRPC::Async::ThreadedTransport.new(origin: origin, **Topology.client_tls(mode), **)
      track(:threaded, created, label, reuse: reuse, round: !persistent)
      cleanup { created.close(grace: 0) } if persistent
      Topology::Clients.new(created, origin)
    end

    def http1(origin, mode: :tcp, label: "http1", persistent: false, **)
      created = ConnectRPC::Async::HTTP1Transport.new(origin: origin, **Topology.client_tls(mode), **)
      track(:http1, created, label, round: !persistent)
      cleanup { created.close(grace: 0) } if persistent
      Topology::Clients.new(created, origin)
    end

    def relay(port, label: "relay")
      created = FaultRelay.new(task, port)
      track(:relay, created, label)
      cleanup { created.close }
      created
    end

    # A never-cancelled source shared by non-cancelled calls; its listeners
    # must all unregister by the end of each round.
    def shared_source(label = "shared cancellation source")
      track(:source, ConnectRPC::CancellationSource.new, label)
    end

    # Performs one logical call and records its outcome against an allowlist.
    # A block replaces the default stream body; it receives the reader,
    # writer or duplex.
    def perform(clients, shape, id: ledger.next_id(shape.to_s), action: nil, size: 16, count: 3, timeout: 30,
                cancellation: nil, expect: [:ok], metadata: {}, &body)
      headers = ledger.metadata(id, **metadata)
      headers[Topology::OP] = action if action
      headers[Topology::COUNT] = count.to_s
      headers[Topology::SIZE] = size.to_s
      options = { metadata: headers, timeout: timeout, cancellation: cancellation }
      invoke(clients, shape, Topology.message(size), options, body)
      ledger.outcome(id, :ok, expect: expect)
    rescue ConnectRPC::Error => e
      ledger.outcome(id, e.code.to_s, expect: expect)
    rescue StandardError => e
      ledger.outcome(id, e.class.name, expect: expect)
    end

    # Runs each block concurrently on the harness reactor and waits for all.
    def concurrently(*blocks)
      tasks = blocks.map { |block| task.async(finished: false) { block.call } }
      tasks.map(&:wait)
    ensure
      tasks&.each { |child| Soak.stop(child) }
    end

    # Runs each block on its own plain thread, as ThreadedTransport callers
    # and Net::HTTP do, and joins them within a bound.
    def on_threads(*blocks, timeout: 30)
      threads = blocks.map { |block| Thread.new(&block) }
      threads.each { |thread| thread.report_on_exception = false }
      threads.map do |thread|
        raise Failure, "Caller thread did not finish within #{timeout}s" unless thread.join(timeout)

        thread.value
      end
    ensure
      threads&.each { |thread| thread.kill.join(5) if thread.alive? }
    end

    private

    def invoke(clients, shape, message, options, body)
      case shape
      when :unary then clients.core.call(Topology::ECHO::ECHO, message, **options)
      when :get then clients.core(use_get: true).call(Topology::ECHO::ECHO, message, **options)
      when :grpc_unary then clients.core(protocol: :grpc).call(Topology::ECHO::ECHO, message, **options)
      when :server_stream, :grpc_server_stream
        core = clients.core(protocol: shape == :server_stream ? :connect : :grpc)
        core.server_stream(Topology::SS, message, **options) { |reader| body ? body.call(reader) : reader.each { nil } }
      when :client_stream
        clients.core.client_stream(Topology::CS, **options) do |writer|
          body ? body.call(writer) : 3.times { writer.send_message(message) }
        end
      when :bidi
        clients.core.bidi_stream(Topology::BIDI, **options) do |duplex|
          next body.call(duplex) if body

          duplex.send_message(message)
          duplex.close_send
          duplex.each { nil }
        end
      else raise ArgumentError, "Unknown shape #{shape}"
      end
    end
  end

  Campaign.include(Builders)
end
