# frozen_string_literal: true

require_relative "support/stream_interceptor_helpers"

RSpec.describe "Async stream interceptors" do
  include StreamInterceptorHelpers

  let(:outcomes) { [] }
  let(:completions) { [] }
  let(:bus) do
    outcomes = self.outcomes
    completions = self.completions
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |call|
      Object.new.tap do |observer|
        observer.define_singleton_method(:processing_finished) { |outcome| outcomes << [call.role, outcome] }
        observer.define_singleton_method(:finished) { |completion| completions << [call.role, completion] }
      end
    end
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end

  def role_outcomes(role) = outcomes.filter_map { |current, outcome| outcome if current == role }
  def role_completions(role) = completions.filter_map { |current, completion| completion if current == role }

  # Serves +handlers+ on a fresh Async Host; transports from stream_client
  # close before the host.
  def with_stream_host(handlers, interceptors: [], tls: nil)
    reactor do |task|
      @transports = []
      target = stream_target(handlers, protocol: %i[connect grpc grpc_web], instrumentation: bus,
                                       interceptors: interceptors)
      host = ConnectRPC::Async::Host.new(server: target, tls: tls).start
      yield host, task
    ensure
      @transports.each { |transport| transport.close(grace: 0) }
      host&.close(grace: 0)
    end
  end

  def stream_client(host, protocol: :connect, interceptors: [], scheme: "http", **)
    origin = "#{scheme}://127.0.0.1:#{host.port}"
    transport = ConnectRPC::Async::Transport.new(origin: origin, **)
    @transports << transport
    ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: protocol, instrumentation: bus,
                           binding: ConnectRPC::Bindings::GoogleProtobuf.new, interceptors: interceptors)
  end

  def canceled = raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }

  describe "order over h2c" do
    %i[connect grpc].product(StreamInterceptorHelpers::SHAPES.keys).each do |protocol, shape|
      it "runs two client and two server layers and their wrappers in order for #{shape} over #{protocol}" do
        client_log = []
        server_log = []
        client_calls = []
        server_calls = []
        headers = []
        handlers = echo_handlers(server_log)
        handler = handlers.fetch(stream_rpc(shape).path)
        handlers[stream_rpc(shape).path] = lambda do |*args|
          headers << args.last.request_metadata.to_h.slice("x-c1", "x-c2")
          handler.call(*args)
        end
        server_layers = %w[s1 s2].map { |name| tracing_layer(:server_stream, name, server_log, calls: server_calls) }
        client_layers = %w[c1 c2].map { |name| tracing_layer(:client_stream, name, client_log, calls: client_calls) }
        with_stream_host(handlers, interceptors: server_layers) do |host, task|
          result = drive_stream(stream_client(host, protocol: protocol, interceptors: client_layers), shape)
          case shape
          when :server_stream then expect(result).to eq(%w[q-1 q-2])
          when :client_stream
            expect(result.message.name).to eq("a,b")
            expect(result).to be_frozen
          else expect(result).to eq(["a", "b", nil])
          end
          wait_until(task, "server outcome") { role_outcomes(:server).size == 1 }
        end
        expect(client_log).to eq(traced(%w[c1 c2], client_operations(shape)))
        expect(server_log).to eq(traced(%w[s1 s2], server_operations(shape), ["handler"]))
        expect(headers).to eq([{ "x-c1" => ["sent"], "x-c2" => ["sent"] }])
        server_class = shape == :server_stream ? ConnectRPC::ServerStreamCall::ServerStreaming : ConnectRPC::ServerStreamCall
        client_class = shape == :server_stream ? ConnectRPC::ClientStreamCall::ServerStreaming : ConnectRPC::ClientStreamCall
        expect(server_calls.map { |call| [call.class, call.protocol, call.method.cardinality] }.uniq)
          .to eq([[server_class, protocol, shape]])
        expect(client_calls.map { |call| [call.class, call.protocol, call.method.cardinality] }.uniq)
          .to eq([[client_class, protocol, shape]])
        expect(server_calls.uniq.size).to eq(1)
        expect(client_calls.uniq.size).to eq(1)
        expect(role_outcomes(:server).map(&:code)).to eq(["OK"])
        expect(role_completions(:client).map(&:delivery)).to eq([:completed])
      end
    end
  end

  describe "gRPC-Web server streaming over h2c" do
    def web_stream(host, name)
      raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
      connection = Async::HTTP::Protocol::HTTP2.client(raw)
      bytes = AsyncHelpers::MESSAGE.encode(stream_message(name))
      response = connection.call(Protocol::HTTP::Request["POST", stream_rpc(:server_stream).path,
                                                         { "content-type" => "application/grpc-web+proto" },
                                                         [[0, bytes.bytesize].pack("CN") + bytes],
                                                         scheme: "http", authority: "127.0.0.1:#{host.port}"])
      [response.status, response.read]
    ensure
      response&.close
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw) if raw
    end

    it "runs the server chain with the request on the call and wrappers in order" do
      log = []
      calls = []
      requests = []
      peek = interceptor(:server_stream) do |call, &continuation|
        requests << call.request.name
        continuation.call
      end
      layers = [*%w[s1 s2].map { |name| tracing_layer(:server_stream, name, log, calls: calls) }, peek]
      with_stream_host(echo_handlers(log), interceptors: layers) do |host, task|
        status, body = web_stream(host, "web")
        expect(status).to eq(200)
        expect(body).to include("web-1", "web-2", "grpc-status: 0\r\n")
        wait_until(task, "server outcome") { role_outcomes(:server).size == 1 }
      end
      expect(log).to eq(traced(%w[s1 s2], server_operations(:server_stream), ["handler"]))
      expect(calls.uniq.size).to eq(1)
      expect(calls.first).to be_a(ConnectRPC::ServerStreamCall::ServerStreaming)
      expect(calls.first.protocol).to eq(:grpc_web)
      expect(requests).to eq(["web"])
    end
  end

  describe "ThreadedTransport server streaming" do
    %i[connect grpc].each do |protocol|
      it "runs client layers and wrappers on the caller thread over #{protocol}" do
        log = []
        threads = []
        recorder = Object.new
        recorder.define_singleton_method(:<<) do |entry|
          threads << Thread.current
          log << entry
        end
        peer = threaded_host(server: stream_target(echo_handlers, protocol: %i[connect grpc]))
        origin = "http://127.0.0.1:#{peer.peer.port}"
        transport = threaded(origin: origin)
        layers = %w[c1 c2].map { |name| tracing_layer(:client_stream, name, recorder) }
        core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: protocol, interceptors: layers,
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new)
        caller = Thread.new { drive_stream(core, :server_stream) }
        expect(value_within(caller, "the intercepted stream")).to eq(%w[q-1 q-2])
        expect(log).to eq(traced(%w[c1 c2], client_operations(:server_stream)))
        expect(threads.uniq).to eq([caller])
      ensure
        transport&.close(grace: 0)
        peer&.stop
      end
    end
  end

  describe "original processing task over mTLS" do
    def with_mtls(handlers, interceptors)
      tls = host_tls(anchors: [tls_material.fetch(:client_ca)])
      with_stream_host(handlers, interceptors: interceptors, tls: tls) do |host, task|
        with_ca do |ca_file|
          certificate, key = tls_material.fetch(:identity)
          options = { scheme: "https", ca_file: ca_file, certificate_chain: [certificate.to_pem],
                      private_key: key.private_to_pem }
          yield host, task, options
        end
      end
    end

    it "returns the same client certificate in the interceptor, its wrapper and the handler" do
      host = nil
      log = []
      recorder = Object.new
      recorder.define_singleton_method(:<<) { |entry| log << [entry, host.peer_certificate&.to_der] }
      with_mtls(echo_handlers(recorder), [tracing_layer(:server_stream, "s1", recorder)]) do |current, task, options|
        host = current
        expect(drive_stream(stream_client(host, **options), :server_stream)).to eq(%w[q-1 q-2])
        wait_until(task, "server outcome") { role_outcomes(:server).size == 1 }
      end
      expect(log.map(&:first)).to eq(traced(%w[s1], server_operations(:server_stream), ["handler"]))
      expect(log.map(&:last).uniq).to eq([tls_material.fetch(:identity).first.to_der])
    end

    it "rejects a stream continuation run in an Async child task on both roles" do
      host = nil
      handled = []
      child_certificates = []
      child = lambda do |role|
        interceptor(role) do |_call, &continuation|
          error = Async do
            child_certificates << host.peer_certificate if role == :server_stream
            continuation.call
            nil
          rescue ConnectRPC::InterceptorStateError => e
            e
          end.wait
          raise error
        end
      end
      handlers = echo_handlers(handled)
      with_mtls(handlers, [child.call(:server_stream)]) do |current, task, options|
        host = current
        expect { drive_stream(stream_client(host, **options), :bidi_stream) }
          .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("internal") }
        wait_until(task, "server outcome") { role_outcomes(:server).size == 1 }
        intercepted = stream_client(host, interceptors: [child.call(:client_stream)], **options)
        expect { drive_stream(intercepted, :bidi_stream) }
          .to raise_error(ConnectRPC::InterceptorStateError, /another fiber or thread/)
      end
      expect(handled).to be_empty
      expect(child_certificates).to eq([nil])
      error = role_outcomes(:server).first.error
      expect(error).to be_a(ConnectRPC::InterceptorStateError)
      expect(error.message).to match(/another fiber or thread/)
      expect(role_outcomes(:client).last.error).to be_a(ConnectRPC::InterceptorStateError)
      expect(role_completions(:client).last.delivery).to eq(:unobserved)
    end
  end

  it "isolates two concurrent bidi streams on one connection" do
    counting = Class.new(SimpleDelegator) do
      attr_reader :call, :counts

      def initialize(stream, call, counts, check = nil)
        super(stream)
        @call = call
        @counts = counts
        @check = check
      end

      def send_message(message)
        @counts[:sent] += 1
        __getobj__.send_message(message)
      end

      def receive
        message = __getobj__.receive
        if message
          @counts[:received] += 1
          @check&.call(message)
        end
        message
      end

      def each
        while (message = receive)
          yield message
        end
        nil
      end
    end
    server_seen = []
    client_seen = []
    foreign = []
    results = {}
    server_layer = interceptor(:server_stream) do |call, &continuation|
      id = call.context.request_metadata["x-stream"].first
      counts = Hash.new(0)
      check = ->(message) { foreign << [id, message.name] unless message.name.start_with?("#{id}-") }
      call.wrap_stream { |stream| counting.new(stream, call, counts, check) }
      continuation.call
      server_seen << [id, call, counts]
    end
    client_layer = interceptor(:client_stream) do |call, &continuation|
      counts = Hash.new(0)
      call.wrap_stream { |stream| counting.new(stream, call, counts) }
      continuation.call
      client_seen << [call.request_metadata["x-stream"].first, call, counts]
    end
    handlers = {
      stream_rpc(:bidi_stream).path => lambda { |duplex, context|
        id = context.request_metadata["x-stream"].first
        duplex.each { |message| duplex.send_message(stream_message("#{id}:#{message.name}")) }
      }
    }
    with_stream_host(handlers, interceptors: [server_layer]) do |host, task|
      core = stream_client(host, interceptors: [client_layer])
      progress = Async::Queue.new
      gates = { "a" => Async::Queue.new, "b" => Async::Queue.new }
      streams = gates.keys.map do |id|
        task.async do
          core.bidi_stream(stream_rpc(:bidi_stream), metadata: { "x-stream" => id }) do |duplex|
            replies = []
            2.times do |round|
              duplex.send_message(stream_message("#{id}-#{round}"))
              replies << duplex.receive.name
              progress.enqueue(id)
              gates.fetch(id).dequeue
            end
            duplex.close_send
            replies << duplex.receive
            results[id] = [duplex.call, duplex.call.request_metadata["x-stream"], duplex.counts.dup, replies]
          end
        end
      end
      # Each round waits until both streams exchanged a message, so both are
      # open together before either moves on.
      2.times do
        expect([progress.dequeue, progress.dequeue].sort).to eq(%w[a b])
        gates.each_value { |gate| gate.enqueue(true) }
      end
      streams.each(&:wait)
      wait_until(task, "both server layers") { server_seen.size == 2 }
      expect(host_session(host).instance_variable_get(:@resources).size).to eq(1)
    end
    %w[a b].each do |id|
      call, metadata, counts, replies = results.fetch(id)
      expect(metadata).to eq([id])
      expect(counts).to eq(sent: 2, received: 2)
      expect(replies).to eq(["#{id}:#{id}-0", "#{id}:#{id}-1", nil])
      expect(client_seen.assoc(id)[1]).to equal(call)
      expect(client_seen.assoc(id)[2]).to eq(sent: 2, received: 2)
      expect(server_seen.assoc(id)[2]).to eq(sent: 2, received: 2)
    end
    expect(results.fetch("a").first).not_to equal(results.fetch("b").first)
    expect(server_seen.assoc("a")[1]).not_to equal(server_seen.assoc("b")[1])
    expect(server_seen.assoc("a")[1].context).not_to equal(server_seen.assoc("b")[1].context)
    expect(foreign).to be_empty
  end

  describe "cancellation" do
    %i[connect grpc].each do |protocol|
      it "ends a server interceptor blocked before yield on a peer CANCEL over #{protocol}" do
        handled = []
        reasons = []
        entered = Async::Queue.new
        woken = Async::Queue.new
        blocking = interceptor(:server_stream) do |call, &continuation|
          call.context.on_cancel { woken.enqueue(true) }
          entered.enqueue(true)
          woken.dequeue
          reasons << call.context.cancellation_reason
          call.context.check!
          continuation.call
        end
        with_stream_host(echo_handlers(handled), interceptors: [blocking]) do |host, task|
          source = ConnectRPC::CancellationSource.new
          core = stream_client(host, protocol: protocol)
          expect do
            core.bidi_stream(stream_rpc(:bidi_stream), cancellation: source.token) do |duplex|
              # Wire progress: the server received the stream and entered the interceptor.
              entered.dequeue
              source.cancel
              duplex.receive
            end
          end.to canceled
          wait_until(task, "server outcome") { role_outcomes(:server).size == 1 }
        end
        expect(handled).to be_empty
        expect(reasons).to eq([:canceled])
        expect(role_outcomes(:server).first.code).to eq("canceled")
      end

      it "reports a peer CANCEL while the handler runs through a server interceptor over #{protocol}" do
        log = []
        reasons = []
        entered = Async::Queue.new
        woken = Async::Queue.new
        observing = interceptor(:server_stream) do |_call, &continuation|
          log << "s1>"
          continuation.call
        rescue ConnectRPC::Error => e
          log << "s1 saw #{e.code}"
          raise
        end
        handlers = {
          stream_rpc(:server_stream).path => lambda { |request, writer, context|
            context.on_cancel { woken.enqueue(true) }
            writer.send_message(request)
            entered.enqueue(true)
            woken.dequeue
            reasons << context.cancellation_reason
            context.check!
          }
        }
        with_stream_host(handlers, interceptors: [observing]) do |host, task|
          source = ConnectRPC::CancellationSource.new
          core = stream_client(host, protocol: protocol)
          expect do
            core.server_stream(stream_rpc(:server_stream), stream_message("hold"),
                               cancellation: source.token) do |reader|
              # Wire progress: the handler's first message reached the client.
              expect(reader.receive.name).to eq("hold")
              entered.dequeue
              source.cancel
              reader.receive
            end
          end.to canceled
          wait_until(task, "server outcome") { role_outcomes(:server).size == 1 }
        end
        expect(reasons).to eq([:canceled])
        expect(log).to eq(["s1>", "s1 saw canceled"])
        expect(role_outcomes(:server).first.code).to eq("canceled")
      end
    end

    it "abandons the raw client session once when the application abandons through a wrapper" do
      log = []
      abandons = []
      contexts = []
      wrapping = interceptor(:client_stream) do |call, &continuation|
        call.wrap_stream do |stream|
          raw = stream.instance_variable_get(:@session)
          allow(raw).to receive(:abandon).and_wrap_original do |original|
            abandons << raw
            original.call
          end
          StreamInterceptorHelpers::TracingStream.new(stream, "c1", log)
        end
        continuation.call
      end
      handlers = {
        stream_rpc(:bidi_stream).path => lambda { |duplex, context|
          contexts << context
          duplex.each { |message| duplex.send_message(message) }
        }
      }
      with_stream_host(handlers) do |host, task|
        result = stream_client(host, interceptors: [wrapping]).bidi_stream(stream_rpc(:bidi_stream)) do |duplex|
          duplex.send_message(stream_message("before"))
          # Wire progress: the handler echoed before the abandonment.
          expect(duplex.receive.name).to eq("before")
          duplex.abandon
        end
        expect(result).to be_nil
        wait_until(task, "server outcome") { role_outcomes(:server).size == 1 }
      end
      expect(log).to eq(%w[c1.send c1.receive])
      expect(abandons.size).to eq(1)
      expect(contexts.first.cancellation_reason).to eq(:canceled)
      expect(role_outcomes(:client).map(&:code)).to eq(["canceled"])
      expect(role_completions(:client).map(&:delivery)).to eq([:aborted])
      expect(role_outcomes(:server).map(&:code)).to eq(["canceled"])
    end
  end
end
