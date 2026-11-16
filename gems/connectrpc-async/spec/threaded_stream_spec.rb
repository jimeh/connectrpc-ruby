# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"

RSpec.describe "ThreadedTransport streams from plain threads" do
  include ThreadedStreamHelpers

  %i[h2c tls mtls].each do |mode|
    it "runs generated Connect SS (proto, JSON) and gRPC unary/SS on threads over one #{mode} connection" do
      with_ca do |ca_file|
        tls, scheme, options = threaded_mode(mode, ca_file)
        entered = Thread::Queue.new
        release = Thread::Queue.new
        peer = threaded_host(tls: tls, server: mixed_server(watch: lambda { |message, writer, context|
          context.send_headers(metadata: { "x-head" => %w[one two] })
          writer.send_message(message)
          entered << message.name
          release.pop
          writer.send_message(item("#{message.name}-last"))
          context.response_trailers["x-tail"] = %w[three four]
        }))
        port = peer.peer.port
        origin = "#{scheme}://127.0.0.1:#{port}"
        transport = threaded(origin: origin, **options)
        callers = { "proto" => mixed_client(transport, origin), "json" => mixed_client(transport, origin, codec: :json),
                    "grpc" => mixed_client(transport, origin, protocol: :grpc) }.map do |name, typed|
          Thread.new do
            seen = []
            headers = trailers = nil
            typed.watch(item(name)) do |reader|
              headers = reader.headers
              reader.each { |item| seen << item.name }
              trailers = reader.trailers
            end
            [seen, headers["x-head"], trailers["x-tail"]]
          end
        end
        expect(3.times.map { pop_within(entered, "three streams in the handler") }.sort).to eq(%w[grpc json proto])
        expect([peer.peer.accepts.size, owner_threads(port).size, admitted(transport)]).to eq([1, 1, 3])
        unary = Thread.new { mixed_client(transport, origin, protocol: :grpc).lookup(item("unary")) }
        expect(value_within(unary, "gRPC unary beside open streams").message.name).to eq("unary")
        3.times { release << true }
        expect(callers.map { |caller| value_within(caller, "a streaming caller") }).to eq(
          %w[proto json grpc].map { |name| [[name, "#{name}-last"], %w[one two], %w[three four]] }
        )
        expect([peer.peer.accepts.size, transport.connection_state, admitted(transport)]).to eq([1, :ready, 0])
        expect(transport.close).to be(true)
        expect(owner_threads(port)).to be_empty
      ensure
        3.times { release&.<<(true) }
        transport&.close(grace: 0)
        peer&.stop
      end
    end
  end

  it "forwards early headers, bounded reads and finish through the exchange proxy" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: mixed_server(watch: lambda { |message, writer, context|
      context.send_headers(metadata: { "x-early" => %w[a b] })
      entered << true
      release.pop
      writer.send_message(message)
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    result = Thread.new do
      transport.open_stream(stream_request(origin, watch_method)) do |exchange|
        exchange.write(envelope(item("bounded")))
        exchange.close_send
        pop_within(entered, "the handler to publish headers")
        head = exchange.response_head
        early = [head.status, head.headers.fetch("x-early"), head.headers.frozen?, head.headers["x-early"].frozen?]
        release << true
        chunks = []
        while (chunk = exchange.read(3))
          chunks << chunk
        end
        exchange.finish
        [early, chunks.map(&:bytesize).max, chunks.join]
      end
    end
    early, largest, bytes = value_within(result, "the raw stream")
    expect(early).to eq([200, %w[a b], true, true])
    expect(largest).to eq(3)
    expect(bytes).to start_with(envelope(item("bounded")))
    expect(admitted(transport)).to eq(0)
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "rejects an overlapping operation from another thread and a retained proxy after completion" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: mixed_server(watch: lambda { |message, writer, _|
      entered << true
      release.pop
      writer.send_message(message)
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    retained = Thread::Queue.new
    reading = Thread::Queue.new
    stream = Thread.new do
      transport.open_stream(stream_request(origin, watch_method)) do |exchange|
        retained << exchange
        exchange.write(envelope(item("overlap")))
        exchange.close_send
        reading << true
        exchange.read(1024)
        exchange.read(1024) while exchange.read(1024)
        exchange.finish
        :done
      end
    end
    exchange = pop_within(retained, "the proxy")
    pop_within(reading, "the first read")
    pop_within(entered, "the handler")
    eventually("the read waiting on the owner") { blocked?(stream) && stream_mailboxes(transport).any?(&:busy) }
    expect { exchange.read(1) }.to raise_error(ConnectRPC::StreamStateError, "Overlapping stream operation")
    release << true
    expect(value_within(stream, "the stream")).to eq(:done)
    expect(admitted(transport)).to eq(0)
    # A retained proxy must refuse at once; a queued command would never be served.
    error = value_within(Thread.new { capture_error { exchange.read(1) } }, "the retained proxy to refuse", timeout: 2)
    expect(error).to be_a(ConnectRPC::Error)
    expect([error.code, error.message]).to eq(["unavailable", "HTTP/2 exchange is retired"])
    # The head was cached by the block's reads; the cache must not outlive the scope.
    head_error = capture_error { exchange.response_head }
    expect([head_error.class, head_error.code, head_error.message])
      .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 exchange is retired"])
    expect(exchange.abort).to be_nil
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "raises the open failure before the block and requires a block" do
    peer = threaded_host(server: mixed_server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    expect { transport.open_stream(stream_request(origin, watch_method)) }
      .to raise_error(ArgumentError, "A streaming block is required")
    expect { transport.open_grpc_stream(stream_request(origin, watch_method)) }
      .to raise_error(ArgumentError, "A gRPC block is required")
    expect { transport.open_stream(stream_request("http://127.0.0.1:1", watch_method)) { nil } }
      .to raise_error(ArgumentError, "Request URL must match the transport origin")
    expect(mixed_client(transport, origin).lookup(item("warm")).message.name).to eq("warm")
    expect(on_owner(transport) { transport.open_stream(stream_request(origin, watch_method)) { nil } }.last)
      .to be_a(AsyncHelpers::LIFECYCLE_ERROR)
    refused = refusing_socket
    closed = "http://127.0.0.1:#{refused.local_address.ip_port}"
    unreachable = threaded(origin: closed)
    entered = []
    error = capture_error { unreachable.open_grpc_stream(stream_request(closed, watch_method)) { entered << true } }
    expect([error.class, error.code, entered]).to eq([ConnectRPC::Error, "unavailable", []])
    expect(admitted(unreachable)).to eq(0)
  ensure
    unreachable&.close(grace: 0)
    refused&.close
    transport&.close(grace: 0)
    peer&.stop
  end

  it "refuses an inherited stream proxy in a forked child without touching the parent's stream" do
    peer = threaded_host(server: mixed_server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    result = Thread.new do
      transport.open_stream(stream_request(origin, watch_method)) do |exchange|
        exchange.write(envelope(item("forked")))
        exchange.close_send
        head = exchange.response_head
        child = fork do
          errors = [capture_error { exchange.read(1) }, capture_error { exchange.response_head }]
          refused = errors.all? do |error|
            [error.class, error.code, error.message] == [ConnectRPC::Error, "unavailable", "HTTP/2 exchange is retired"]
          end
          exit!(refused && exchange.abort.nil? ? 0 : 1)
        end
        _, status = Process.wait2(child)
        bytes = +"".b
        while (chunk = exchange.read(1024))
          bytes << chunk
        end
        [status.exitstatus, head.status, bytes.start_with?(envelope(item("forked")))]
      end
    end
    expect(value_within(result, "the parent's stream")).to eq([0, 200, true])
    expect(admitted(transport)).to eq(0)
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end

  # The child leaves the inherited block itself, so the transport's exit path
  # and ensure run in the child; it reports over a pipe once open_stream ends.
  %i[return raise throw].each do |kind|
    it "ends a forked child's #{kind} from an inherited stream block without touching the parent's stream" do
      skip "Process.fork is unavailable" unless Process.respond_to?(:fork)
      peer = threaded_host(server: mixed_server)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = threaded(origin: origin)
      failure = Class.new(StandardError)
      parent = Process.pid
      reader, writer = IO.pipe
      result = Thread.new do
        started = value = nil
        outcome = capture_error do
          value = catch(:out) do
            transport.open_stream(stream_request(origin, watch_method)) do |exchange|
              exchange.write(envelope(item("forked")))
              exchange.close_send
              exchange.response_head
              child = fork
              if child.nil?
                started = monotonic
                throw :out if kind == :throw
                raise failure, "child" if kind == :raise

                next :child
              end
              status = Process.detach(child).join(10)&.value
              Process.kill("KILL", child) unless status
              bytes = +"".b
              while (chunk = exchange.read(1024))
                bytes << chunk
              end
              exchange.finish
              [status&.success?, bytes.start_with?(envelope(item("forked")))]
            end
          end
        end
        if Process.pid != parent
          inherited = stream_mailboxes(transport).map do |stream|
            [stream.broken, stream.caller_done, stream.commands.size]
          end
          error = [outcome.class, outcome.respond_to?(:code) ? outcome.code : nil, outcome&.message]
          error = [:own] if outcome.is_a?(failure) && outcome.message == "child"
          writer.write(Marshal.dump([error, monotonic - started, inherited, admitted(transport)]))
          writer.close
          exit!(0)
        end
        outcome || value
      end
      expect(value_within(result, "the parent's stream")).to eq([true, true])
      writer.close
      error, elapsed, inherited, held = Marshal.load(reader.read) # rubocop:disable Security/MarshalLoad -- this test's own child
      expected = { return: [ConnectRPC::Error, "unavailable", "HTTP/2 exchange is retired"], raise: [:own],
                   throw: [NilClass, nil, nil] }.fetch(kind)
      expect([error, inherited, held]).to eq([expected, [[false, false, 0]], 1])
      expect(elapsed).to be < 1
      expect(admitted(transport)).to eq(0)
      expect(mixed_client(transport, origin).lookup(item("after")).message.name).to eq("after")
    ensure
      reader&.close
      writer&.close unless writer&.closed?
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  it "resets the stream when the block leaves with break or throw and returns that value" do
    contexts = {}
    release = Thread::Queue.new
    peer = threaded_host(server: mixed_server(watch: lambda { |message, writer, context|
      contexts[message.name] = context
      writer.send_message(message)
      release.pop
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    start = lambda do |exchange, name|
      exchange.write(envelope(item(name)))
      exchange.close_send
      exchange.read(5)
    end
    broken = Thread.new do
      transport.open_stream(stream_request(origin, watch_method)) do |exchange|
        start.call(exchange, "break")
        break :left
      end
    end
    thrown = Thread.new do
      catch(:out) do
        transport.open_stream(stream_request(origin, watch_method)) do |exchange|
          start.call(exchange, "throw")
          throw :out, :thrown
        end
      end
    end
    expect([value_within(broken, "the break"), value_within(thrown, "the throw")]).to eq(%i[left thrown])
    eventually("actual peer CANCEL reached both handlers") { contexts.values_at("break", "throw").all?(&:cancelled?) }
    eventually("admission released") { admitted(transport).zero? }
  ensure
    2.times { release&.<<(true) }
    transport&.close(grace: 0)
    peer&.stop
  end
end
