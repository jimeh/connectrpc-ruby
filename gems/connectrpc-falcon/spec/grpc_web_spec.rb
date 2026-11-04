# frozen_string_literal: true

RSpec.describe "Falcon native gRPC-Web" do
  def web_frame(bytes) = [0, bytes.bytesize].pack("CN") + bytes

  def web_request(connection, origin, bytes, text: false, route: AsyncHelpers::ECHO::ECHO.path)
    media = text ? "application/grpc-web-text+proto" : "application/grpc-web+proto"
    connection.call(Protocol::HTTP::Request["POST", route, { "content-type" => media },
                                            [text ? [bytes].pack("m0") : bytes], authority: URI(origin).authority])
  end

  it "keeps a real H2 sibling healthy after Web first-data progress and actual CANCEL" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      stream = ConnectRPC::ServerStreamMethod.new(service: "web.Siblings", name: "Watch",
                                                  request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
      unary = ConnectRPC::Method.new(service: stream.service, name: "Call",
                                     request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
      target = ConnectRPC::Server.new(
        services: [ConnectRPC::Service.new(name: stream.service, methods: [stream, unary])],
        binding: ConnectRPC::Bindings::GoogleProtobuf.new, protocol: :grpc_web,
        handlers: { unary.path => ->(message, _) { message }, stream.path => lambda { |message, output, context|
          output.send_message(message)
          entered.enqueue(context)
          release.dequeue
          context.check!
        } }
      )
      falcon(server: target) do |_host, origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP2.client(raw)
        bytes = web_frame(AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "progress")))
        response = web_request(connection, origin, bytes, route: stream.path)
        first = +"".b
        first << response.body.read while first.bytesize < bytes.bytesize
        expect(first).to eq(bytes)
        context = entered.dequeue
        expect(context).not_to be_cancelled
        response.stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL)
        wait_until(task, "actual Web peer CANCEL reaches original Context") { context.cancelled? }
        healthy = web_request(connection, origin, bytes, route: unary.path)
        expect(healthy.status).to eq(200)
        expect(healthy.read).to start_with(bytes)
        expect(context.cancellation_reason).to eq(:canceled)
        release.enqueue(true)
      ensure
        release.enqueue(true)
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
    end
  end

  it "checks the H1 parsed Web field block at 64KiB before copies, lifecycle or invocation and retires resources" do
    reactor do |task|
      calls = []
      completions = []
      seen = []
      subscriber = Object.new
      subscriber.define_singleton_method(:start) do |call|
        calls << call
        Object.new.tap do |observer|
          observer.define_singleton_method(:finished) do |completion|
            completions << completion
          end
        end
      end
      copies = []
      allow(AsyncHelpers::INTERNAL).to receive(:headers).and_wrap_original do |original, incoming|
        copies << incoming
        original.call(incoming)
      end
      target = server(protocol: :grpc_web, instrumentation: ConnectRPC::Instrumentation.new(subscribers: [subscriber]),
                      echo: lambda { |message, _|
                        seen << message
                        message
                      })
      falcon(server: target) do |host, origin|
        uri = URI(origin)
        fields = [["content-type", "application/grpc-web"]] + (1..16).map { |i| ["x-fill-#{i}", "x" * 3800] }
        size = fields.sum { |key, value| key.bytesize + value.bytesize + 32 }
        socket = nil
        [0, 1].each do |delta|
          block = fields + [["x-pad", "x" * (65_536 + delta - size - 37)]]
          expect(block.sum { |key, value| key.bytesize + value.bytesize + 32 }).to eq(65_536 + delta)
          if delta.zero?
            result = http1(origin, AsyncHelpers::ECHO::ECHO.path, method: "POST", body: [0, 0].pack("CN"),
                                                                  headers: block)
            expect(result.first).to eq(200)
            # The parsed block fits the host guard. Core separately accounts for
            # the reconstructed Content-Length, so its own field cap rejects it.
            expect(result.last).to include("grpc-status: 8\r\n")
            expect(copies).not_to be_empty
            expect(calls.size).to eq(1)
          else
            socket = IO::Endpoint.tcp(uri.hostname, uri.port).connect
            header_lines = block.map { |key, value| "#{key}: #{value}\r\n" }.join
            socket.write("POST #{AsyncHelpers::ECHO::ECHO.path} HTTP/1.1\r\nHost: #{uri.authority}\r\n" \
                         "Content-Length: 5\r\n#{header_lines}\r\n" + [0, 0].pack("CN"))
            expect(task.with_timeout(2) { socket.gets }).to be_nil
            expect(calls.size).to eq(1)
            expect(copies).to be_empty
            socket.close
          end
          expect(seen).to be_empty
          wait_until(task, "H1 Web owned resource and admission retirement") do
            session = host.instance_variable_get(:@session)
            session.instance_variable_get(:@admitted).empty? &&
              session.instance_variable_get(:@resources).all? { |resource| resource.finished? && resource.raw.closed? }
          end
          copies.clear
        end
        healthy = http1(origin, AsyncHelpers::ECHO::ECHO.path, method: "POST", body: [0, 0].pack("CN"),
                                                               headers: { "content-type" => "application/grpc-web" })
        expect(healthy.first).to eq(200)
        expect(healthy.last).to include("grpc-status: 0\r\n")
        wait_until(task, "healthy H1 Web completion") { completions.size == 2 }
        expect(seen.size).to eq(1)
        expect(calls.size).to eq(2)
      ensure
        socket&.close unless socket&.closed?
      end
    end
  end

  it "retains real H1 admission and metrics through a failing body callback" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      registry = Async::Utilization::Registry.new
      body_type = Class.new(Protocol::HTTP::Body::Wrapper) do
        define_method(:close) do |error = nil|
          entered.enqueue(true)
          release.dequeue
          raise "owned response callback failed"
        ensure
          super(error)
        end
      end
      host_type = Class.new(ConnectRPC::Falcon::Server) do
        define_method(:call_with_publication) do |request, publication:, **options|
          publication.on_publish { |head| head.body = body_type.new(head.body) }
          super(request, publication: publication, **options)
        end
      end
      bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
      policy = ConnectRPC::GRPCWebCORS.new(origins: ["https://browser.example"])
      target = server(protocol: :grpc_web, cors: policy)
      host = host_type.new(server: target, endpoint: bound, max_requests: 1, utilization_registry: registry)
      host.run
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
      connection = Async::HTTP::Protocol::HTTP1.client(raw)
      bytes = web_frame(AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "callback")))
      response = web_request(connection, origin, bytes)
      expect(response.read).to end_with("grpc-status: 0\r\n")
      entered.dequeue
      expect(registry.metric(:requests_active).value).to eq(1)
      rejected = http1(origin, AsyncHelpers::ECHO::ECHO.path, method: "POST", body: bytes, headers: {
                         "content-type" => "application/grpc-web", "origin" => "https://browser.example"
                       })
      expect(rejected.first).to eq(503)
      expect(rejected[1]["access-control-allow-origin"]).to eq(["https://browser.example"])
      tunnel = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
      tunnel.write("CONNECT localhost:443 HTTP/1.1\r\nHost: localhost\r\n\r\n")
      expect(tunnel.gets).to start_with("HTTP/1.1 503")
      release.enqueue(true)
      wait_until(task, "Web callback failure retires admission and metrics") do
        registry.metric(:requests_active).value.zero? && host.instance_variable_get(:@session)
                                                             .instance_variable_get(:@admitted).empty?
      end
    ensure
      release&.enqueue(true)
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw)
      AsyncHelpers::INTERNAL.raw_close(tunnel)
      host&.close(grace: 0)
      bound&.close
    end
  end

  %i[http1 h2].product([false, true]).each do |protocol, text|
    it "serves #{protocol} #{text ? 'text' : 'binary'} unary through the real wire and retires admission" do
      reactor do |task|
        registry = Async::Utilization::Registry.new
        target = server(protocol: :grpc_web)
        falcon(server: target, utilization_registry: registry) do |host, origin|
          raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
          implementation = protocol == :http1 ? Async::HTTP::Protocol::HTTP1 : Async::HTTP::Protocol::HTTP2
          connection = implementation.client(raw)
          bytes = web_frame(AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "web")))
          response = web_request(connection, origin, bytes, text: text)
          body = response.read
          body = body.scan(/.{4}/).map { |group| group.unpack1("m0") }.join if text
          expect(response.status).to eq(200)
          expect(body).to start_with(bytes)
          expect(body).to end_with("x-tail: three\r\nx-tail: four\r\ngrpc-status: 0\r\n")
          wait_until(task, "Web admission and metrics released") do
            host.instance_variable_get(:@session).instance_variable_get(:@admitted).all? do |key, done|
              done && key.respond_to?(:closed?) && key.closed?
            end &&
              registry.metric(:requests_active).value.zero?
          end
          if protocol == :h2
            streams = host.instance_variable_get(:@session).instance_variable_get(:@admitted).keys
            expect(streams).not_to be_empty
            expect(streams).to all(have_attributes(grpc?: false, grpc_error: nil))
          end
        ensure
          connection&.close
          AsyncHelpers::INTERNAL.raw_close(raw)
        end
      end
    end
  end

  %i[http1 h2].product([false, true]).each do |protocol, late_failure| # rubocop:disable Style/CombinableLoops -- distinct scenarios
    it "holds ALL #{protocol} terminal bytes behind original application cleanup, failure=#{late_failure}" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        observed = []
        outcomes = []
        completions = []
        observer = Object.new
        observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
        observer.define_singleton_method(:finished) { |completion| completions << completion }
        subscriber = Object.new
        subscriber.define_singleton_method(:start) { |_| observer }
        instrumentation = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
        failure = RuntimeError.new("private outer cleanup")
        target = server(protocol: :grpc_web, instrumentation: instrumentation, echo: lambda { |message, _|
          observed << Async::Task.current
          message
        })
        host_type = Class.new(ConnectRPC::Falcon::Server) do
          define_method(:call_with_publication) do |*args, **options|
            super(*args, **options)
          ensure
            observed << Async::Task.current
            entered.enqueue(true)
            release.dequeue
            observed << Async::Task.current
            raise failure if late_failure
          end
        end
        bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
        host = host_type.new(server: target, endpoint: bound)
        host.run
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        implementation = protocol == :http1 ? Async::HTTP::Protocol::HTTP1 : Async::HTTP::Protocol::HTTP2
        connection = implementation.client(raw)
        bytes = web_frame(AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "first")))
        response = web_request(connection, origin, bytes)
        incoming = +"".b
        incoming << response.body.read while incoming.bytesize < bytes.bytesize
        expect(incoming).to eq(bytes)
        entered.dequeue
        reader_started = Async::Queue.new
        terminal_events = Async::Queue.new
        terminal_chunks = []
        terminal_read = task.async(finished: false) do
          reader_started.enqueue(true)
          while (chunk = response.body.read)
            terminal_chunks << chunk
            terminal_events.enqueue([:bytes, chunk])
          end
          terminal_events.enqueue([:eof])
          terminal_chunks.join
        rescue StandardError => e
          terminal_events.enqueue([:error, e])
          e
        end
        reader_started.dequeue
        # This is a bounded real network read, after first DATA and next-reader
        # progress. EOF alone is insufficient: even one terminal prefix byte
        # during the original cleanup gate must fail this assertion.
        boundary = begin
          task.with_timeout(0.1) { terminal_events.dequeue }
        rescue Async::TimeoutError
          :no_terminal_bytes
        end
        expect(boundary).to eq(:no_terminal_bytes), "terminal event before cleanup release: #{boundary.inspect}"
        expect(terminal_chunks).to be_empty
        expect(terminal_read).not_to be_finished
        expect(host.instance_variable_get(:@session).instance_variable_get(:@admitted).size).to eq(1)
        release.enqueue(true)
        result = terminal_read.wait
        if late_failure
          expect(result).to be_a(StandardError)
          expect(terminal_chunks).to be_empty
        else
          expect(result).to include("grpc-status: 0\r\n")
        end
        expect(observed.map(&:object_id).uniq.size).to eq(1)
        wait_until(task, "Web original processing and delivery outcomes") { !completions.empty? }
        expect(outcomes.first.error).to equal(late_failure ? failure : nil)
        expect(completions.size).to eq(1)
        expect(completions.first.delivery).to eq(late_failure ? :aborted : :completed)
        expect(completions.first.error).to equal(late_failure ? failure : nil)
        healthy = http1(origin, "/healthy")
        expect(healthy.first).to eq(404)
      ensure
        release&.enqueue(true)
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
        host&.close(grace: 0)
        bound&.close
      end
    end
  end
end
