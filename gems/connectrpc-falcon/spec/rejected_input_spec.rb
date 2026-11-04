# frozen_string_literal: true

RSpec.describe "Falcon rejected Web HTTP/1 input" do
  def rejected_server(events, instrument: true, stream: false, **)
    observer = Object.new
    observer.define_singleton_method(:processing_finished) { |outcome| events[:outcomes] << outcome }
    observer.define_singleton_method(:finished) { |completion| events[:completions] << completion }
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    stream_method = ConnectRPC::ServerStreamMethod.new(service: "web.Rejection", name: "Watch",
                                                       request_type: AsyncHelpers::MESSAGE,
                                                       response_type: AsyncHelpers::MESSAGE)
    options = if stream
                { services: [ConnectRPC::Service.new(name: stream_method.service, methods: [stream_method])],
                  binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                  handlers: { stream_method.path => ->(*) { raise "Oversized request reached handler" } } }
              else
                {}
              end
    instrumentation = ConnectRPC::Instrumentation.new(subscribers: [subscriber]) if instrument
    target = server(**options, protocol: :grpc_web, request_limit: 100, instrumentation: instrumentation, **)
    # Core has prepared the original error; the host still owns input cleanup.
    original = target.method(:call_grpc_web)
    target.define_singleton_method(:call_grpc_web) do |**arguments|
      native_body = arguments[:input].instance_variable_get(:@body)
      events[:body] = native_body
      original_read = native_body.method(:read)
      native_body.define_singleton_method(:read) do
        events[:cleanup_started] = true if events[:rejected]
        chunk = original_read.call
        (events[:reads] ||= []) << [chunk&.bytesize, Async::Task.current, events[:rejected]]
        chunk
      end
      original.call(**arguments)
    ensure
      events[:rejected] = true
      events[:prepared].enqueue(Async::Task.current)
    end
    target
  end

  def rejected_socket(origin, tls: false)
    raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
    socket = raw
    if tls
      context = OpenSSL::SSL::SSLContext.new
      context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, verify_hostname: true)
      store = OpenSSL::X509::Store.new
      store.add_cert(tls_material[:root].first)
      context.cert_store = store
      context.alpn_protocols = ["http/1.1"]
      socket = OpenSSL::SSL::SSLSocket.new(raw, context)
      socket.hostname = "localhost"
      socket.connect
    end
    yield socket
  ensure
    AsyncHelpers::INTERNAL.raw_close(raw)
    socket&.close unless socket.equal?(raw)
  end

  def rejection_prefix(socket, chunked: true, text: false, timeout: nil, route: AsyncHelpers::ECHO::ECHO.path)
    prefix = [0, 200_000].pack("CN")
    prefix = [prefix].pack("m0") if text
    framing = chunked ? "Transfer-Encoding: chunked" : "Content-Length: 200005"
    media = text ? "application/grpc-web-text+proto" : "application/grpc-web+proto"
    fields = timeout ? "grpc-timeout: #{timeout}\r\n" : ""
    socket.write("POST #{route} HTTP/1.1\r\nHost: localhost\r\n" \
                 "Content-Type: #{media}\r\n#{fields}#{framing}\r\n\r\n")
    socket.write(chunked ? "#{prefix.bytesize.to_s(16)}\r\n#{prefix}\r\n" : prefix)
  end

  def read_http1_head(socket)
    status = socket.gets("\r\n")
    headers = {}
    while (line = socket.gets("\r\n")) && line != "\r\n"
      key, value = line.strip.split(":", 2)
      headers[key.downcase] = value.strip
    end
    [status, headers]
  end

  def read_http1(socket)
    status, headers = read_http1_head(socket)
    body = +"".b
    if headers["transfer-encoding"] == "chunked"
      loop do
        length = socket.gets("\r\n").to_i(16)
        break if length.zero?

        body << socket.read(length)
        expect(socket.read(2)).to eq("\r\n")
      end
      expect(socket.gets("\r\n")).to eq("\r\n")
    elsif headers["content-length"]
      body << socket.read(headers["content-length"].to_i)
    end
    [status, body]
  end

  [false, true].product([false, true]).each do |tls, chunked|
    it "defers the rejection head for incomplete #{chunked ? 'chunked' : 'fixed'} input, TLS=#{tls}" do
      reactor do |task|
        events = { prepared: Async::Queue.new, outcomes: [], completions: [] }
        target = rejected_server(events, stream: true)
        falcon(server: target, scheme: tls ? "https" : "http", tls: tls ? host_tls : nil) do |host, origin|
          boundary = Async::Queue.new
          session = host.instance_variable_get(:@session)
          trace = TracePoint.new(:call) do |event|
            next unless %i[wait write_response].include?(event.method_id)

            resource = session.instance_variable_get(:@resources).find do |owned|
              owned.respond_to?(:active) && owned.active && owned.task.equal?(Async::Task.current)
            end
            next unless resource

            if event.defined_class == AsyncHelpers::INTERNAL::Publication && event.method_id == :wait
              boundary.enqueue(:dispatch_wait)
            elsif event.self.equal?(resource.connection) && event.method_id == :write_response
              boundary.enqueue(:response_head)
            end
          end
          trace.enable
          rejected_socket(origin, tls: tls) do |socket|
            rejection_prefix(socket, chunked: chunked, route: "/web.Rejection/Watch")
            task.with_timeout(2) { events[:prepared].dequeue }
            ordering = task.with_timeout(2) { boundary.dequeue }
            trace.disable
            expect(events[:cleanup_started]).to be(true)
            expect(events[:body]).not_to be_empty
            # Resume upload on either boundary so baseline also proves that
            # terminal and HTTP completion alone cannot detect the early head.
            socket.write("30d40\r\n") if chunked
            remaining = 200_000
            while remaining.positive?
              amount = [remaining, 16_384].min
              socket.write("x" * amount)
              remaining -= amount
              task.yield
            end
            socket.write("\r\n0\r\n\r\n") if chunked
            socket.write("GET /healthy HTTP/1.1\r\nHost: localhost\r\n\r\n")
            status, body = task.with_timeout(2) { read_http1(socket) }
            expect(status).to start_with("HTTP/1.1 200")
            expect(body).to include("grpc-status: 8\r\n")
            expect(body.getbyte(0)).to eq(0x80)
            expect(body.bytesize).to eq(5 + body.byteslice(1, 4).unpack1("N"))
            expect(task.with_timeout(2) { read_http1(socket) }.first).to start_with("HTTP/1.1 404")
            wait_until(task, "completed rejection") { !events[:completions].empty? }
            expect(events[:outcomes].map(&:code)).to eq(["resource_exhausted"])
            expect(events[:completions].map(&:delivery)).to eq([:completed])
            expect(ordering).to eq(:dispatch_wait), "response head preceded dispatch completion wait"
          ensure
            trace&.disable
          end
        end
      end
    end

    it "streams the first Web server message before handler completion, #{chunked ? 'chunked' : 'fixed'}, TLS=#{tls}" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        progress = Async::Queue.new
        stream = ConnectRPC::ServerStreamMethod.new(service: "web.Progress", name: "Watch",
                                                    request_type: AsyncHelpers::MESSAGE,
                                                    response_type: AsyncHelpers::MESSAGE)
        target = ConnectRPC::Server.new(
          services: [ConnectRPC::Service.new(name: stream.service, methods: [stream])],
          binding: ConnectRPC::Bindings::GoogleProtobuf.new, protocol: :grpc_web,
          handlers: { stream.path => lambda { |message, output, _|
            entered.enqueue(true)
            output.send_message(message)
            release.dequeue
          } }
        )
        falcon(server: target, scheme: tls ? "https" : "http", tls: tls ? host_tls : nil) do |host, origin|
          rejected_socket(origin, tls: tls) do |socket|
            bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "incremental"))
            frame = [0, bytes.bytesize].pack("CN") + bytes
            framing = chunked ? "Transfer-Encoding: chunked" : "Content-Length: #{frame.bytesize}"
            socket.write("POST #{stream.path} HTTP/1.1\r\nHost: localhost\r\n" \
                         "Content-Type: application/grpc-web+proto\r\n#{framing}\r\n\r\n")
            socket.write(chunked ? "#{frame.bytesize.to_s(16)}\r\n#{frame}\r\n0\r\n\r\n" : frame)
            entered.dequeue
            reader = task.async(finished: false) do
              status, headers = read_http1_head(socket)
              expect(status).to eq("HTTP/1.1 200 OK\r\n")
              expect(headers["transfer-encoding"]).to eq("chunked")
              body = +"".b
              loop do
                length = socket.gets("\r\n").to_i(16)
                break if length.zero?

                body << socket.read(length)
                expect(socket.read(2)).to eq("\r\n")
                progress.enqueue(body.dup) if body.bytesize == frame.bytesize
              end
              expect(socket.gets("\r\n")).to eq("\r\n")
              body
            end
            first = begin
              task.with_timeout(0.3) { progress.dequeue }
            rescue Async::TimeoutError
              :no_incremental_message
            end
            expect(first).to eq(frame), "first Web server message blocked behind handler completion"
            expect(host.instance_variable_get(:@session).instance_variable_get(:@admitted).size).to eq(1)
            release.enqueue(true)
            expect(task.with_timeout(2) { reader.wait }).to end_with("grpc-status: 0\r\n")
          ensure
            release.enqueue(true)
            AsyncHelpers::INTERNAL.cancel(reader)
          end
        end
      end
    end
  end

  [false, true].each do |text|
    it "finishes readahead framing without instrumentation, text=#{text}" do
      reactor do |_task|
        events = { prepared: Async::Queue.new, outcomes: [], completions: [] }
        falcon(server: rejected_server(events, instrument: false)) do |_host, origin|
          rejected_socket(origin) do |socket|
            bytes = [0, 200_000].pack("CN") + ("x" * 200_000)
            bytes = [bytes].pack("m0") if text
            media = text ? "application/grpc-web-text+proto" : "application/grpc-web+proto"
            socket.write("POST #{AsyncHelpers::ECHO::ECHO.path} HTTP/1.1\r\nHost: localhost\r\n" \
                         "Content-Type: #{media}\r\nTransfer-Encoding: chunked\r\n\r\n" \
                         "#{bytes.bytesize.to_s(16)}\r\n#{bytes}\r\n0\r\n\r\n" \
                         "GET /next HTTP/1.1\r\nHost: localhost\r\n\r\n")
            processor = events[:prepared].dequeue
            status, body = read_http1(socket)
            body = body.unpack1("m0") if text
            expect(status).to start_with("HTTP/1.1 200")
            expect(body).to include("grpc-status: 8\r\n")
            expect(read_http1(socket).first).to start_with("HTTP/1.1 404")
            expect(events[:reads].filter_map(&:first).sum).to eq(bytes.bytesize)
            expect(events[:reads].filter_map(&:first).max).to be <= 65_536
            tail_readers = events[:reads].filter_map { |_, reader, rejected| reader if rejected }.uniq
            expect(tail_readers).to all(eq(processor))
          end
        end
      end
    end
  end

  [false, true].product(%i[stalled malformed truncated progress deadline drain abort cancel]).each do |tls, failure|
    it "retires only owned input raw-first on #{failure}, TLS=#{tls}" do
      reactor do |task|
        events = { prepared: Async::Queue.new, outcomes: [], completions: [] }
        registry = Async::Utilization::Registry.new
        falcon(server: rejected_server(events), scheme: tls ? "https" : "http", tls: tls ? host_tls : nil,
               utilization_registry: registry) do |host, origin|
          rejected_socket(origin, tls: tls) do |socket|
            # This peer establishes wire progress before we observe its cleanup.
            started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rejection_prefix(socket, timeout: failure == :deadline ? "30m" : nil)
            processor = events[:prepared].dequeue
            resource = host.instance_variable_get(:@session).instance_variable_get(:@resources).find(&:active)
            closed_states = []
            backend_closes = []
            trace = TracePoint.new(:call) do |event|
              if event.self.equal?(resource.connection) && event.method_id == :close_read &&
                 event.defined_class == Protocol::HTTP1::Connection
                backend_closes << resource.raw.closed?
              end
              if event.self.equal?(events[:body]) && event.method_id == :close
                closed_states << [resource.raw.closed?, Async::Task.current]
              end
            end
            trace.enable
            case failure
            when :malformed then socket.write("invalid-chunk\r\n")
            when :truncated
              socket.write("100\r\npartial")
              # Deliberately truncate the HTTP entity at the real TCP boundary.
              socket.is_a?(OpenSSL::SSL::SSLSocket) ? socket.to_io.shutdown(Socket::SHUT_WR) : socket.shutdown(Socket::SHUT_WR)
            when :progress
              # Real paced network input tests elapsed-time policy itself. Each
              # short pause spaces writes, rather than assuming a race began.
              writer = task.async(finished: false) do
                loop do
                  socket.write("1\r\nx\r\n")
                  task.sleep(0.015)
                end
              rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
                nil
              end
            when :drain then closing = task.async(finished: false) { host.close(grace: 0.015) }
            when :abort then host.abort
            when :cancel then processor.cancel
            end
            wait_until(task, "rejected input delivery completion") { !events[:completions].empty? }
            elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
            expect(events[:outcomes].map(&:code)).to eq(["resource_exhausted"])
            expect(events[:completions].map(&:delivery)).to eq([:aborted])
            expect(events[:completions].map(&:http_status)).to eq([200])
            expect(events[:outcomes].first.error.code).to eq("resource_exhausted")
            if %i[stalled progress deadline malformed].include?(failure)
              # The writer can observe raw retirement before publication reports
              # the cleanup error. Preserve that actual delivery failure too.
              expect(events[:completions].first.error).to be_a(StandardError)
              expect(events[:completions].first.error).not_to be_a(ConnectRPC::Error)
            end
            expect(closed_states).not_to be_empty
            expect(closed_states.map(&:first)).to all(be(true))
            expect(backend_closes).not_to be_empty if failure == :truncated
            expect(backend_closes).to all(be(true))
            expect(closed_states.first.last).to equal(processor)
            wire = +"".b
            begin
              task.with_timeout(2) { loop { wire << socket.readpartial(16_384) } }
            rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
              # Abrupt raw-first TLS retirement need not send close_notify.
            end
            expect(wire).not_to include("grpc-status: 8")
            tail_readers = events[:reads].filter_map { |_, reader, rejected| reader if rejected }.uniq
            expect(tail_readers).to all(eq(processor))
            limit = %i[deadline drain abort cancel].include?(failure) ? 0.09 : 0.5
            expect(elapsed).to be < limit
            if failure == :progress
              expect(events[:reads].count { |size, _| size == 1 }).to be >= 3
              expect(elapsed).to be >= 0.08
            end
            unless %i[drain abort].include?(failure)
              expect(http1(origin, "/healthy", context: tls ? verified_h1_context : nil).first).to eq(404)
              wait_until(task, "all rejection and healthy peer metrics retired") do
                registry.metric(:connections_active).value.zero? && registry.metric(:requests_active).value.zero?
              end
            end
            closing&.wait
            wait_until(task, "failed cleanup retires admission and request metrics") do
              host.instance_variable_get(:@session).instance_variable_get(:@admitted).empty? &&
                registry.metric(:requests_active).value.zero?
            end
          ensure
            trace&.disable
            AsyncHelpers::INTERNAL.cancel(writer)
          end
        end
      end
    end
  end

  it "preserves an already established other connection after rejected input expires" do
    reactor do |task|
      events = { prepared: Async::Queue.new, outcomes: [], completions: [] }
      falcon(server: rejected_server(events)) do |_host, origin|
        rejected_socket(origin) do |healthy|
          healthy.write("GET /before HTTP/1.1\r\nHost: localhost\r\n\r\n")
          expect(read_http1(healthy).first).to start_with("HTTP/1.1 404")
          rejected_socket(origin) do |socket|
            rejection_prefix(socket)
            events[:prepared].dequeue
            wait_until(task, "stalled connection retired") { !events[:completions].empty? }
            healthy.write("GET /after HTTP/1.1\r\nHost: localhost\r\n\r\n")
            expect(read_http1(healthy).first).to start_with("HTTP/1.1 404")
          end
        end
      end
    end
  end

  it "keeps core validation order when a timeout is invalid" do
    reactor do
      falcon(server: server(protocol: :grpc_web)) do |_host, origin|
        headers = { "content-type" => "application/grpc-web+proto",
                    "grpc-timeout" => "invalid", "grpc-encoding" => "gzip" }
        status, _, body = http1(origin, AsyncHelpers::ECHO::ECHO.path, method: "POST", headers: headers)
        expect(status).to eq(200)
        expect(body).to include("grpc-status: 12\r\n")
      end
    end
  end

  [false, true].each do |stalled|
    it "owns early CORS rejection cleanup without RPC observation, stalled=#{stalled}" do
      reactor do |task|
        starts = []
        subscriber = Object.new
        subscriber.define_singleton_method(:start) { |call| starts << call }
        cors = ConnectRPC::GRPCWebCORS.new(origins: ["https://allowed.example"])
        target = server(protocol: :grpc_web, cors: cors,
                        instrumentation: ConnectRPC::Instrumentation.new(subscribers: [subscriber]))
        registry = Async::Utilization::Registry.new
        falcon(server: target, utilization_registry: registry) do |_host, origin|
          rejected_socket(origin) do |socket|
            socket.write("POST #{AsyncHelpers::ECHO::ECHO.path} HTTP/1.1\r\nHost: localhost\r\n" \
                         "Content-Type: application/grpc-web+proto\r\nOrigin: https://denied.example\r\n" \
                         "Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n")
            if stalled
              wait_until(task, "CORS native cleanup started") { registry.metric(:requests_active).value == 1 }
              wait_until(task, "CORS unfinished input retired") { registry.metric(:connections_active).value.zero? }
              expect(registry.metric(:requests_active).value).to eq(0)
              expect(http1(origin, "/healthy").first).to eq(404)
            else
              socket.write("0\r\n\r\nGET /next HTTP/1.1\r\nHost: localhost\r\n\r\n")
              expect(read_http1(socket).first).to start_with("HTTP/1.1 403")
              expect(read_http1(socket).first).to start_with("HTTP/1.1 404")
            end
            expect(starts).to be_empty
          end
        end
      end
    end
  end

  def verified_h1_context
    context = OpenSSL::SSL::SSLContext.new
    context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, verify_hostname: true)
    store = OpenSSL::X509::Store.new
    store.add_cert(tls_material[:root].first)
    context.cert_store = store
    context.alpn_protocols = ["http/1.1"]
    context
  end
end
