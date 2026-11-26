# frozen_string_literal: true

require "delegate"
require "json"
require "stringio"

RSpec.describe "Stream interceptors" do
  shapes = {
    server_stream: ConnectRPC::ServerStreamMethod,
    client_stream: ConnectRPC::ClientStreamMethod,
    bidi_stream: ConnectRPC::BidiStreamMethod
  }.freeze

  let(:binding) do
    Object.new.tap do |codec|
      codec.define_singleton_method(:encode) { |message, **| message.b }
      codec.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end
  let(:status_codec) do
    require "connectrpc/bindings/google_protobuf"
    ConnectRPC::Bindings::GoogleProtobuf.new
  end
  let(:outcomes) { [] }
  let(:completions) { [] }
  let(:bus) do
    outcomes = self.outcomes
    completions = self.completions
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |_call|
      Object.new.tap do |observer|
        observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
        observer.define_singleton_method(:finished) { |completion| completions << completion }
      end
    end
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end
  # Exchange and transport counters for one client call.
  let(:counts) { Hash.new(0) }
  let(:opened) { [] }
  let(:written) { [] }

  # Logs each operation, then delegates. Read-side wrappers override both
  # receive and each, so reads through each pass through the wrapper too.
  let(:tracing) do
    Class.new(SimpleDelegator) do
      def initialize(stream, name, log)
        super(stream)
        @name = name
        @log = log
      end

      def send_message(message)
        @log << "#{@name}.send"
        __getobj__.send_message(message)
      end

      def receive
        @log << "#{@name}.receive"
        __getobj__.receive
      end

      def each
        while (message = receive)
          yield message
        end
        nil
      end
    end
  end

  # Rejects incoming messages that start with "bad", through receive and each.
  let(:validating) do
    Class.new(SimpleDelegator) do
      def receive
        message = __getobj__.receive
        raise ConnectRPC::Error.new(:invalid_argument, "Invalid message") if message&.start_with?("bad")

        message
      end

      def each
        while (message = receive)
          yield message
        end
        nil
      end
    end
  end

  define_method(:rpc) do |shape|
    shapes.fetch(shape).new(service: "intercept.Streams", name: "Run", request_type: String, response_type: String)
  end

  def frame(bytes, flags = 0) = [flags, bytes.bytesize].pack("CN") + bytes

  def frames(bytes)
    offset = 0
    parsed = []
    while offset < bytes.bytesize
      flags, length = bytes.byteslice(offset, 5).unpack("CN")
      parsed << [flags, bytes.byteslice(offset + 5, length)]
      offset += 5 + length
    end
    parsed
  end

  # Bodies run with the example as self; the continuation arrives as a block.
  def layer(role, &body)
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(role) { |call, &continuation| body.call(call, &continuation) }
    end
  end

  def only(list)
    expect(list.size).to eq(1)
    list.first
  end

  def expect_frozen(response)
    expect(response).to be_frozen
    expect(response.metadata).to be_frozen
    expect(response.trailers).to be_frozen
    expect { response.trailers["x-late"] = "value" }.to raise_error(FrozenError)
  end

  # Counts every Session#close and StreamingContext#close in the example.
  def count_closes
    allow_any_instance_of(ConnectRPC.const_get(:Streaming)::Session).to receive(:close).and_wrap_original do |original|
      counts[:session_close] += 1
      original.call
    end
    allow_any_instance_of(ConnectRPC::StreamingContext).to receive(:close).and_wrap_original do |original|
      counts[:context_close] += 1
      original.call
    end
  end

  # A scripted peer that answers with `messages` and an OK terminal carrying
  # x-tail. It counts the operations that core cleanup reaches.
  def peer(protocol, messages, on_abort: nil, on_write: nil)
    counts = self.counts
    written = self.written
    body = messages.map { |message| frame(message) }.join
    if protocol == :grpc
      head = ConnectRPC::GRPCHead.new(status: 200, end_stream: false,
                                      headers: { "content-type" => ["application/grpc"], "x-head" => ["a"] })
    else
      head = ConnectRPC::StreamHead.new(status: 200,
                                        headers: { "content-type" => ["application/connect+proto"],
                                                   "x-head" => ["a"] })
      body += frame('{"metadata":{"x-tail":["b"]}}', 2)
    end
    source = StringIO.new(body)
    exchange = Object.new
    exchange.define_singleton_method(:response_head) { |**| head }
    exchange.define_singleton_method(:response_trailers) { { "grpc-status" => ["0"], "x-tail" => ["b"] } }
    exchange.define_singleton_method(:read) { |length, **| source.read([length, 64].min) }
    exchange.define_singleton_method(:write) do |bytes|
      on_write&.call(bytes)
      written << bytes
      nil
    end
    exchange.define_singleton_method(:close_send) { (counts[:close_send] += 1) && nil }
    exchange.define_singleton_method(:finish) { |**| (counts[:finish] += 1) && nil }
    exchange.define_singleton_method(:abort) do
      counts[:abort] += 1
      on_abort&.call
      nil
    end
    exchange
  end

  def stream_client(interceptors, protocol: :connect, messages: ["reply"], exchange: nil, before_open: nil,
                    instrumentation: bus)
    exchange ||= peer(protocol, messages)
    opened = self.opened
    open = lambda do |request, &work|
      opened << request
      before_open&.call(request)
      work.call(exchange)
    end
    transport = Object.new
    if protocol == :grpc
      transport.define_singleton_method(:grpc_capabilities) { %i[unary server_stream client_stream bidi_stream] }
      transport.define_singleton_method(:open_grpc_stream, &open)
    else
      transport.define_singleton_method(:streaming_capabilities) { %i[server_stream client_stream bidi_stream] }
      transport.define_singleton_method(:open_stream, &open)
    end
    options = protocol == :grpc ? { protocol: :grpc, status_codec: status_codec } : {}
    ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                           instrumentation: instrumentation, interceptors: interceptors, **options)
  end

  def invoke(client, shape, request: "q", **, &)
    method = rpc(shape)
    return client.server_stream(method, request, **, &) if shape == :server_stream

    client.public_send(shape, method, **, &)
  end

  def stream_server(interceptors, shape, handler)
    method = rpc(shape)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: method.service, rpcs: [method])],
                           binding: binding, handlers: { method.path => handler }, instrumentation: bus,
                           interceptors: interceptors, protocol: %i[connect grpc grpc_web],
                           status_codec: status_codec)
  end

  # Serves one stream in process and returns the terminal code, the response
  # messages, the trailers and the number of terminals emitted.
  def serve(target, shape, protocol, requests: ["q"], heads: [])
    writes = []
    output = Object.new
    output.define_singleton_method(:write) { |bytes, **| (writes << bytes) && nil }
    output.define_singleton_method(:abort) { nil }
    input = StringIO.new(requests.map { |message| frame(message) }.join)
    input.define_singleton_method(:read) { |length, **| super(length) }
    options = { path: rpc(shape).path, http_method: "POST", input: input, output: output,
                publish: ->(head) { heads << head } }
    code, trailers, terminals = public_send(:"serve_#{protocol}", target, output, writes, options)
    messages = frames(writes.join).select { |flags, _| flags.zero? }.map(&:last)
    { code: code, messages: messages, trailers: trailers, terminals: terminals }
  end

  def serve_connect(target, _output, writes, options)
    target.call_stream(headers: { "content-type" => ["application/connect+proto"] }, http2: true, **options)
    ends = frames(writes.join).select { |flags, _| flags == 2 }
    terminal = JSON.parse(ends.last.last)
    [terminal.dig("error", "code") || "OK", terminal.fetch("metadata", {}), ends.size]
  end

  def serve_grpc(target, output, _writes, options)
    terminals = []
    output.define_singleton_method(:finish) { |headers:| (terminals << headers) && nil }
    target.call_grpc(headers: { "content-type" => ["application/grpc"] }, http2: true, **options)
    fields = terminals.last
    [grpc_code(fields.fetch("grpc-status").first), fields.except("grpc-status", "grpc-message"), terminals.size]
  end

  def serve_grpc_web(target, output, _writes, options)
    terminals = []
    output.define_singleton_method(:finish) { |terminal:| (terminals << terminal) && nil }
    target.call_grpc_web(headers: { "content-type" => ["application/grpc-web+proto"] },
                         capabilities: %i[unary server_stream], **options)
    fields = frames(terminals.last).first.last.split("\r\n").to_h { |line| line.split(": ", 2) }
    code = grpc_code(fields.delete("grpc-status"))
    [code, fields.except("grpc-message").transform_values { |value| [value] }, terminals.size]
  end

  def grpc_code(status) = ConnectRPC.const_get(:GRPCProtocol)::CODES.fetch(Integer(status))

  # Runs one shape's application operations through the endpoint it receives.
  def drive_client(shape, stream)
    case shape
    when :server_stream then 2.times { stream.receive }
    when :client_stream then stream.send_message("one")
    else
      stream.send_message("one")
      stream.close_send
      2.times { stream.receive }
    end
  end

  # A handler for each shape that uses its endpoint and echoes requests.
  def echo_handler(shape, log = [])
    case shape
    when :server_stream then ->(request, writer, _) { (log << "handler") && writer.send_message("re:#{request}") }
    when :client_stream
      lambda do |reader, _|
        log << "handler"
        "re:#{reader.receive}"
      end
    else
      lambda do |duplex, _|
        log << "handler"
        duplex.send_message("re:#{duplex.receive}")
      end
    end
  end

  def wait_until(seconds = 5)
    limit = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    until yield
      raise "Condition not observed within #{seconds}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > limit

      Thread.pass
    end
  end

  describe "construction" do
    it "runs a mixed interceptor's unary method on unary calls and its stream method on streams" do
      log = []
      mixed = layer(:client_unary) { |_call, &continuation| (log << :unary) && continuation.call }
      mixed.define_singleton_method(:client_stream) { |_call, &continuation| (log << :stream) && continuation.call }
      client = stream_client([mixed], protocol: :grpc)
      expect(client.call(ConnectRPC::Method.new(service: "intercept.Streams", name: "Run", request_type: String,
                                                response_type: String), "q").message).to eq("reply")
      invoke(client, :server_stream) { |reader| reader.each { nil } }
      expect(log).to eq(%i[unary stream])
    end
  end

  describe "ordering" do
    shapes.each_key do |shape|
      %i[connect grpc].each do |protocol|
        it "runs client layers outermost first and wrappers outer first for #{shape} over #{protocol}" do
          log = []
          calls = []
          interceptors = %w[c1 c2 c3].map do |name|
            layer(:client_stream) do |call, &continuation|
              calls << [call.class, call.protocol, call.method.cardinality]
              log << "#{name}>"
              call.wrap_stream { |stream| tracing.new(stream, name, log) }
              continuation.call.tap { log << "<#{name}" }
            end
          end
          invoke(stream_client(interceptors, protocol: protocol), shape) do |stream|
            log << "app"
            drive_client(shape, stream)
          end
          operations = { server_stream: %w[receive receive], client_stream: %w[send],
                         bidi_stream: %w[send receive receive] }.fetch(shape)
          expect(log).to eq(%w[c1> c2> c3> app] + operations.flat_map { |op| %w[c1 c2 c3].map { "#{it}.#{op}" } } +
                            %w[<c3 <c2 <c1])
          expected = shape == :server_stream ? ConnectRPC::ClientStreamCall::ServerStreaming : ConnectRPC::ClientStreamCall
          expect(calls.uniq).to eq([[expected, protocol, shape]])
          expect(only(completions).delivery).to eq(:completed)
        end
      end

      protocols = shape == :server_stream ? %i[connect grpc grpc_web] : %i[connect grpc]
      protocols.each do |protocol|
        it "runs server layers outermost first and wrappers outer first for #{shape} over #{protocol}" do
          log = []
          calls = []
          interceptors = %w[s1 s2 s3].map do |name|
            layer(:server_stream) do |call, &continuation|
              calls << [call.class, call.protocol, call.context.class]
              log << "#{name}>"
              call.wrap_stream { |stream| tracing.new(stream, name, log) }
              continuation.call.tap { log << "<#{name}" }
            end
          end
          result = serve(stream_server(interceptors, shape, echo_handler(shape, log)), shape, protocol)
          expect(result).to include(code: "OK", messages: ["re:q"], terminals: 1)
          operation = { server_stream: %w[send], client_stream: %w[receive], bidi_stream: %w[receive send] }
                      .fetch(shape).flat_map { |op| %w[s1 s2 s3].map { "#{it}.#{op}" } }
          expect(log).to eq(%w[s1> s2> s3> handler] + operation + %w[<s3 <s2 <s1])
          expected = shape == :server_stream ? ConnectRPC::ServerStreamCall::ServerStreaming : ConnectRPC::ServerStreamCall
          expect(calls.uniq).to eq([[expected, protocol, ConnectRPC::StreamingContext]])
          expect(only(outcomes).code).to eq("OK")
        end
      end
    end

    it "sends layer metadata, the replaced request and closes the call at dispatch" do
      late = []
      replacing = layer(:client_stream) do |call, &continuation|
        call.request_metadata["x-layer"] = call.request
        call.request = "replaced"
        continuation.call.tap do
          late << (begin
            call.request = "late"
          rescue ConnectRPC::InterceptorStateError => e
            e
          end)
          late << (begin
            call.wrap_stream { |stream| stream }
          rescue ConnectRPC::InterceptorStateError => e
            e
          end)
          late << (begin
            call.request_metadata["x-late"] = "late"
          rescue FrozenError => e
            e
          end)
        end
      end
      invoke(stream_client([replacing]), :server_stream, request: "original") { |reader| reader.each { nil } }
      expect(only(opened).headers).to include("x-layer" => ["original"])
      expect(frames(written.join)).to eq([[0, "replaced"]])
      expect(late.map(&:class)).to eq([ConnectRPC::InterceptorStateError, ConnectRPC::InterceptorStateError,
                                       FrozenError])
    end

    it "requires a block for wrap_stream" do
      missing = layer(:client_stream) { |call, &_continuation| call.wrap_stream }
      expect { invoke(stream_client([missing]), :bidi_stream) { nil } }.to raise_error(ArgumentError, /block/)
      expect(opened).to be_empty
    end

    it "hands the handler the request a server layer replaced and rejects late replacement" do
      seen = []
      late = []
      replacing = layer(:server_stream) do |call, &continuation|
        seen << call.request
        call.request = "replaced"
        continuation.call.tap do
          late << (begin
            call.request = "late"
          rescue ConnectRPC::InterceptorStateError => e
            e
          end)
        end
      end
      result = serve(stream_server([replacing], :server_stream, echo_handler(:server_stream)), :server_stream, :connect)
      expect(result).to include(code: "OK", messages: ["re:replaced"])
      expect(seen).to eq(["q"])
      expect(only(late)).to be_a(ConnectRPC::InterceptorStateError)
    end
  end

  describe "read-side wrappers" do
    def validator(role)
      layer(role) do |call, &continuation|
        call.wrap_stream { |stream| validating.new(stream) }
        continuation.call
      end
    end

    %i[receive each].each do |style|
      read = lambda do |stream, seen|
        if style == :receive
          while (message = stream.receive)
            seen << message
          end
        else
          stream.each { |message| seen << message }
        end
      end

      %i[server_stream bidi_stream].each do |shape|
        it "rejects an invalid #{shape} response read with #{style} on the client" do
          seen = []
          client = stream_client([validator(:client_stream)], messages: %w[good bad])
          expect do
            invoke(client, shape) do |stream|
              stream.close_send if shape == :bidi_stream
              read.call(stream, seen)
            end
          end.to raise_error(ConnectRPC::Error) { |error| expect(error.message).to eq("Invalid message") }
          expect(seen).to eq(["good"])
          expect(only(outcomes).code).to eq("invalid_argument")
        end
      end

      %i[client_stream bidi_stream].each do |shape|
        it "rejects an invalid #{shape} request read with #{style} on the server" do
          seen = []
          handler = lambda do |stream, _context|
            read.call(stream, seen)
            "done"
          end
          result = serve(stream_server([validator(:server_stream)], shape, handler), shape, :connect,
                         requests: %w[good bad])
          expect(result[:code]).to eq("invalid_argument")
          expect(seen).to eq(["good"])
        end
      end
    end
  end

  describe "wrapper boundaries" do
    it "raises StreamStateError from an escaped client wrapper and an escaped server wrapper" do
      escaped = []
      capture = lambda do |role|
        layer(role) do |call, &continuation|
          call.wrap_stream { |stream| tracing.new(stream, "w", []).tap { escaped << it } }
          continuation.call
        end
      end
      invoke(stream_client([capture.call(:client_stream)]), :server_stream) { |reader| reader.each { nil } }
      serve(stream_server([capture.call(:server_stream)], :bidi_stream, echo_handler(:bidi_stream)), :bidi_stream,
            :connect)
      expect { escaped.first.receive }.to raise_error(ConnectRPC::StreamStateError, /closed/)
      expect { escaped.last.send_message("late") }.to raise_error(ConnectRPC::StreamStateError, /closed/)
    end

    %i[client_stream bidi_stream].each do |shape|
      it "reaches the exchange close_send once after a wrapper close_send for #{shape}" do
        closing = layer(:client_stream) do |call, &continuation|
          call.wrap_stream do |stream|
            SimpleDelegator.new(stream).tap do |wrapper|
              wrapper.define_singleton_method(:send_message) do |message|
                __getobj__.send_message(message)
                __getobj__.close_send
              end
            end
          end
          continuation.call
        end
        invoke(stream_client([closing]), shape) do |stream|
          stream.send_message("one")
          stream.each { nil } if shape == :bidi_stream
        end
        expect(counts[:close_send]).to eq(1)
        expect(only(outcomes).code).to eq("OK")
      end
    end

    it "fails at wrap time when a duplex wrapper drops an inherited reader method" do
      ran = false
      dropping = layer(:client_stream) do |call, &continuation|
        call.wrap_stream do |stream|
          Object.new.tap do |wrapper|
            %i[send_message close_send receive headers trailers abandon].each do |name|
              wrapper.define_singleton_method(name) { |*args| stream.public_send(name, *args) }
            end
          end
        end
        continuation.call
      end
      expect { invoke(stream_client([dropping]), :bidi_stream) { ran = true } }
        .to raise_error(ConnectRPC::InterceptorStateError, "Stream wrapper does not respond to each")
      expect(ran).to be(false)
      expect(counts[:abort]).to eq(1)
    end

    it "fails internal at wrap time when a server wrapper drops send_headers" do
      ran = false
      dropping = layer(:server_stream) do |call, &continuation|
        call.wrap_stream do |stream|
          Object.new.tap do |wrapper|
            wrapper.define_singleton_method(:send_message) do |message|
              stream.send_message(message)
            end
          end
        end
        continuation.call
      end
      handler = ->(_request, _writer, _context) { ran = true }
      expect(serve(stream_server([dropping], :server_stream, handler), :server_stream, :grpc)[:code]).to eq("internal")
      expect(ran).to be(false)
      expect(only(outcomes).error).to be_a(ConnectRPC::InterceptorStateError)
      expect(only(outcomes).error.message).to eq("Stream wrapper does not respond to send_headers")
    end
  end

  describe "exactly-once cleanup" do
    failure = RuntimeError.new("raise point")
    raise_points = {
      "an interceptor before yield" => [lambda { |_call, &_continuation|
        raise failure
      }, :before],
      "an interceptor after yield" => [lambda { |_call, &continuation|
        continuation.call
        raise failure
      }, :after],
      "a wrap_stream block" => [lambda { |call, &continuation|
        call.wrap_stream { |_stream| raise failure }
        continuation.call
      }, :factory],
      "a wrapper method" => [lambda { |call, &continuation|
        call.wrap_stream do |stream|
          SimpleDelegator.new(stream).tap do |wrapper|
            wrapper.define_singleton_method(:send_message) { |_message| raise failure }
          end
        end
        continuation.call
      }, :wrapper],
      "a return without yielding" => [->(_call, &_continuation) { :skipped }, :before]
    }

    raise_points.each do |label, (body, point)|
      it "cleans up a server stream once for #{label}" do
        count_closes
        ran = []
        handler = lambda do |duplex, _context|
          ran << :handler
          duplex.send_message("re:#{duplex.receive}")
        end
        result = serve(stream_server([layer(:server_stream, &body)], :bidi_stream, handler), :bidi_stream, :connect)
        expect(result).to include(code: "internal", terminals: 1)
        expect(counts.values_at(:context_close, :session_close)).to eq([1, 1])
        recorded = label.start_with?("a return") ? be_a(ConnectRPC::InterceptorStateError) : equal(failure)
        expect(only(outcomes).error).to(recorded)
        expect(ran).to eq(%i[before factory].include?(point) ? [] : [:handler])
      end

      it "cleans up a client stream once for #{label}" do
        count_closes
        source = ConnectRPC::CancellationSource.new
        fired = []
        listening = layer(:client_stream) do |call, &continuation|
          call.cancellation.on_cancel { fired << :listener }
          continuation.call
        end
        client = stream_client([listening, layer(:client_stream, &body)])
        expected = label.start_with?("a return") ? ConnectRPC::InterceptorStateError : failure
        expect do
          invoke(client, :bidi_stream, cancellation: source.token) do |duplex|
            duplex.send_message("one")
            duplex.close_send
            duplex.each { nil }
          end
        end.to(raise_error do |error|
                 expected.is_a?(Class) ? expect(error).to(be_a(expected)) : expect(error).to(equal(expected))
               end)
        completion = only(completions)
        if point == :before
          expect(opened).to be_empty
          expect(counts.values_at(:abort, :finish, :session_close)).to eq([0, 0, 0])
          expect(completion.delivery).to eq(:unobserved)
        else
          expect(opened.size).to eq(1)
          # A completed exchange finishes; an abandoned one aborts. Never both.
          expect(counts.values_at(:abort, :finish)).to eq(point == :after ? [0, 1] : [1, 0])
          expect(counts[:session_close]).to eq(1)
          expect(completion.delivery).to eq(point == :after ? :completed : :aborted)
        end
        source.cancel
        expect(fired).to be_empty
      end
    end
  end

  describe "settled rows" do
    describe "abandonment precedence" do
      translation = ConnectRPC::Error.new(:failed_precondition, "Translated")
      after_abandon = ConnectRPC::Error.new(:aborted, "After abandon")
      cases = {
        "explicit abandonment" => [:bidi_stream, :abandon.to_proc, nil,
                                   { result: nil, code: "canceled", delivery: :aborted }],
        "unconsumed completion" => [:server_stream, ->(_stream) {}, nil,
                                    { error: ConnectRPC::StreamNotConsumedError, code: "canceled",
                                      delivery: :aborted }],
        "a translated StreamNotConsumedError" => [
          :server_stream, ->(_stream) {},
          lambda do |_call, &continuation|
            continuation.call
          rescue ConnectRPC::StreamNotConsumedError
            raise translation
          end, { error: translation, code: "failed_precondition", delivery: :aborted }
        ],
        "an interceptor raising after abandon" => [
          :bidi_stream, :abandon.to_proc,
          lambda { |_call, &continuation|
            continuation.call
            raise after_abandon
          }, { error: after_abandon, code: "aborted", delivery: :aborted }
        ]
      }
      cases.each do |label, (shape, application, body, expected)|
        [false, true].each do |cancelled|
          it "reports #{label}#{' with an accepted cancellation' if cancelled}" do
            source = ConnectRPC::CancellationSource.new
            interceptors = [layer(:client_stream) { |_call, &continuation| continuation.call }]
            interceptors << layer(:client_stream, &body) if body
            client = stream_client(interceptors, messages: %w[one two])
            run = lambda do
              invoke(client, shape, cancellation: source.token) do |stream|
                if cancelled
                  source.cancel
                  expect { stream.receive }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
                end
                application.call(stream)
              end
            end
            if expected.key?(:result)
              expect(run.call).to eq(expected[:result])
            elsif expected[:error].is_a?(Class)
              expect(&run).to raise_error(expected[:error])
            else
              expect(&run).to(raise_error { |error| expect(error).to equal(expected[:error]) })
            end
            outcome = only(outcomes)
            expect(outcome.code).to eq(expected[:code])
            expect(outcome.normalized_error.code).to eq(expected[:code])
            expect(outcome.normalized_error).to equal(expected[:error]) if expected[:error].is_a?(ConnectRPC::Error)
            expect(only(completions).delivery).to eq(expected[:delivery])
          end
        end
      end
    end

    it "keeps a caught send failure on an OK outcome, as without interceptors" do
      catching = layer(:client_stream) do |call, &continuation|
        call.wrap_stream do |stream|
          SimpleDelegator.new(stream).tap do |wrapper|
            wrapper.define_singleton_method(:send_message) do |message|
              __getobj__.send_message(message)
            rescue ConnectRPC::Error
              nil
            end
          end
        end
        continuation.call
      end
      results = [[catching], []].map do |interceptors|
        outcomes.clear
        response = invoke(stream_client(interceptors), :client_stream) do |writer|
          if interceptors.empty?
            begin
              writer.send_message(nil)
            rescue ConnectRPC::Error
              nil
            end
          else
            writer.send_message(nil)
          end
          writer.send_message("one")
        end
        outcome = only(outcomes)
        [response.message, outcome.code, outcome.error&.code, outcome.normalized_error]
      end
      expect(results.first).to eq(["reply", "OK", "invalid_argument", nil])
      expect(results.first).to eq(results.last)
    end

    %i[connect grpc].each do |protocol|
      it "reports a forgiven request send failure as OK, as without interceptors, over #{protocol}" do
        passing = [layer(:client_stream) { |_call, &continuation| continuation.call },
                   layer(:client_unary) { |_call, &continuation| continuation.call }]
        passing.last.define_singleton_method(:client_stream) { |_call, &continuation| continuation.call }
        results = [passing, []].map do |interceptors|
          outcomes.clear
          completions.clear
          exchange = peer(protocol, %w[one two], on_write: ->(_) { raise IOError, "incidental send I/O" })
          client = stream_client(interceptors, protocol: protocol, exchange: exchange)
          received = []
          invoke(client, :server_stream) { |reader| reader.each { |message| received << message } }
          if protocol == :grpc
            exchange = peer(protocol, ["reply"], on_write: ->(_) { raise IOError, "incidental send I/O" })
            client = stream_client(interceptors, protocol: protocol, exchange: exchange)
            unary = ConnectRPC::Method.new(service: "intercept.Streams", name: "Run", request_type: String,
                                           response_type: String)
            received << client.call(unary, "q").message
          end
          [received, outcomes.map { |outcome| [outcome.code, outcome.error, outcome.normalized_error] },
           completions.map(&:delivery)]
        end
        calls = protocol == :grpc ? 2 : 1
        expect(results.first).to eq([protocol == :grpc ? %w[one two reply] : %w[one two],
                                     [["OK", nil, nil]] * calls, [:completed] * calls])
        expect(results.first).to eq(results.last)
      end
    end

    it "classifies a raising wrap_stream factory as an independent interceptor exception" do
      count_closes
      failure = ConnectRPC::Error.new(:permission_denied, "Factory")
      ran = false
      factory = layer(:client_stream) do |call, &continuation|
        call.wrap_stream { |_stream| raise failure }
        continuation.call
      end
      source = ConnectRPC::CancellationSource.new
      noisy = peer(:connect, ["reply"], on_abort: -> { source.cancel })
      expect do
        invoke(stream_client([factory], exchange: noisy), :bidi_stream, cancellation: source.token) do
          ran = true
        end
      end
        .to(raise_error { |error| expect(error).to equal(failure) })
      expect(ran).to be(false)
      expect(only(outcomes)).to have_attributes(code: "permission_denied", error: failure, normalized_error: failure)
      expect(counts.values_at(:abort, :session_close)).to eq([1, 1])
      expect(only(completions).delivery).to eq(:aborted)
    end

    describe "failure provenance under an accepted cancellation" do
      let(:source) { ConnectRPC::CancellationSource.new }
      let(:failure) { ConnectRPC::Error.new(:unavailable, "Open failed") }

      def run(interceptor, shape = :server_stream)
        source = self.source
        failure = self.failure
        cancel_then_fail = lambda do |request|
          source.cancel
          request.cancellation.cancelled?
          raise failure
        end
        invoke(stream_client([interceptor], before_open: cancel_then_fail), shape, cancellation: source.token) { nil }
      end

      {
        "a pass-through layer" => ->(_call, &continuation) { continuation.call },
        "a layer that re-raises by identity" => lambda do |_call, &continuation|
          continuation.call
        rescue StandardError => e # rubocop:disable Lint/UselessRescue -- re-raises by identity on purpose
          raise e
        end
      }.each do |label, body|
        it "replaces a core-owned open failure with canceled through #{label}" do
          expect { run(layer(:client_stream, &body)) }
            .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
          expect(only(outcomes)).to have_attributes(code: "canceled", error: failure)
          expect(only(completions).delivery).to eq(:aborted)
        end
      end

      it "keeps a translated core failure by identity, with its own code" do
        translation = ConnectRPC::Error.new(:resource_exhausted, "Translated")
        translating = layer(:client_stream) do |_call, &continuation|
          continuation.call
        rescue ConnectRPC::Error
          raise translation
        end
        expect { run(translating, :bidi_stream) }.to(raise_error { |error| expect(error).to equal(translation) })
        expect(only(outcomes)).to have_attributes(code: "resource_exhausted", error: failure,
                                                  normalized_error: translation)
      end

      it "keeps an exception an interceptor raises after an accepted cancellation" do
        own = ConnectRPC::Error.new(:aborted, "Interceptor")
        raising = layer(:client_stream) do |call, &continuation|
          continuation.call
          source.cancel
          raise own if call.cancellation.cancelled?
        end
        expect do
          invoke(stream_client([raising]), :client_stream, cancellation: source.token) do |w|
            w.send_message("one")
          end
        end
          .to(raise_error { |error| expect(error).to equal(own) })
        expect(only(outcomes)).to have_attributes(code: "aborted", error: own, normalized_error: own)
        expect(only(completions).delivery).to eq(:completed)
      end
    end
  end

  describe "client identity" do
    it "returns an application exception translated by an interceptor as the translation" do
      original = RuntimeError.new("application")
      translation = ConnectRPC::Error.new(:internal, "Translated")
      seen = []
      translating = layer(:client_stream) do |_call, &continuation|
        continuation.call
      rescue RuntimeError => e
        seen << e
        raise translation
      end
      expect { invoke(stream_client([translating]), :bidi_stream) { raise original } }
        .to(raise_error { |error| expect(error).to equal(translation) })
      expect(only(seen)).to equal(original)
      expect(only(outcomes)).to have_attributes(code: "internal", error: original, normalized_error: translation)
    end

    it "fails with the application exception an interceptor swallows" do
      original = RuntimeError.new("application")
      swallowing = layer(:client_stream) do |_call, &continuation|
        continuation.call
      rescue RuntimeError
        :fallback
      end
      expect { invoke(stream_client([swallowing]), :bidi_stream) { raise original } }
        .to(raise_error { |error| expect(error).to equal(original) })
      expect(only(outcomes)).to have_attributes(code: nil, error: original)
    end

    it "keeps an application error over cleanup noise for interceptors and the caller" do
      original = RuntimeError.new("application")
      original.set_backtrace(["application.rb:42"])
      noise = IOError.new("cleanup noise")
      translation = ConnectRPC::Error.new(:aborted, "Translated")
      seen = []
      client_with = lambda do |body|
        client = stream_client([layer(:client_stream, &body)])
        transport = client.instance_variable_get(:@transport)
        exchange = peer(:connect, [])
        transport.define_singleton_method(:open_stream) do |_request, &work|
          work.call(exchange)
        ensure
          raise noise
        end
        client
      end
      translating = lambda do |_call, &continuation|
        continuation.call
      rescue StandardError => e
        seen << e
        raise translation
      end
      expect { invoke(client_with.call(translating), :bidi_stream) { raise original } }
        .to(raise_error { |error| expect(error).to equal(translation) })
      expect(only(seen)).to equal(original)
      passing = ->(_call, &continuation) { continuation.call }
      expect { invoke(client_with.call(passing), :bidi_stream) { raise original } }
        .to(raise_error do |error|
          expect(error).to equal(original)
          expect(error.backtrace).to eq(["application.rb:42"])
        end)
    end

    it "keeps canceled for a transport that cancels and fails to open, with an interceptor present" do
      source = ConnectRPC::CancellationSource.new
      failing = lambda do |_request|
        source.cancel
        raise ConnectRPC::Error.new(:unavailable, "Open failed")
      end
      client = stream_client([layer(:client_stream) { |_call, &continuation| continuation.call }], before_open: failing)
      expect { invoke(client, :client_stream, cancellation: source.token) { nil } }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
      expect(only(outcomes).code).to eq("canceled")
    end
  end

  describe "results" do
    %i[connect grpc].each do |protocol|
      it "freezes the client-streaming Response without interceptors over #{protocol}" do
        response = invoke(stream_client([], protocol: protocol), :client_stream) { |writer| writer.send_message("one") }
        expect(response.message).to eq("reply")
        expect(response.trailers["x-tail"]).to eq(["b"])
        expect_frozen(response)
      end
    end

    it "returns a replacement Response frozen to outer layers and the caller" do
      seen = []
      outer = layer(:client_stream) do |_call, &continuation|
        continuation.call.tap { |response| seen << response.frozen? << response.trailers.frozen? }
      end
      inner = layer(:client_stream) do |_call, &continuation|
        ConnectRPC::Response.new(message: "re:#{continuation.call.message}", metadata: ConnectRPC::Metadata.new,
                                 trailers: ConnectRPC::Metadata.new)
      end
      response = invoke(stream_client([outer, inner]), :client_stream) { |writer| writer.send_message("one") }
      expect(response.message).to eq("re:reply")
      expect_frozen(response)
      expect(seen).to eq([true, true])
      expect(only(completions).delivery).to eq(:completed)
    end

    it "rejects an invalid client-streaming layer result after a completed exchange" do
      invalid = layer(:client_stream) { |_call, &continuation| continuation.call && "invalid" }
      expect { invoke(stream_client([invalid]), :client_stream) { |writer| writer.send_message("one") } }
        .to raise_error(ConnectRPC::InterceptorStateError, /must return a ConnectRPC::Response/)
      expect(only(outcomes).code).to be_nil
      expect(only(completions).delivery).to eq(:completed)
    end

    %i[server_stream bidi_stream].each do |shape|
      it "ignores #{shape} layer results on both roles" do
        yields = []
        ignoring = lambda do |role|
          layer(role) do |_call, &continuation|
            yields << continuation.call
            :ignored
          end
        end
        expect(invoke(stream_client([ignoring.call(:client_stream), ignoring.call(:client_stream)]), shape) do |stream|
          stream.close_send if shape == :bidi_stream
          stream.each { nil }
          :block_result
        end).to be_nil
        handler = if shape == :server_stream
                    ->(_q, writer, _) { writer.send_message("a") && :handled }
                  else
                    lambda { |_d, _|
                      :handled
                    }
                  end
        result = serve(stream_server([ignoring.call(:server_stream), ignoring.call(:server_stream)], shape, handler),
                       shape, :connect)
        expect(result[:code]).to eq("OK")
        expect(yields).to eq([nil, nil, nil, nil])
      end
    end

    it "sends the outermost server client-streaming result and fails internal for nil" do
      replacing = layer(:server_stream) { |_call, &continuation| "outer:#{continuation.call}" }
      handler = ->(reader, _context) { "re:#{reader.receive}" }
      expect(serve(stream_server([replacing], :client_stream, handler), :client_stream, :connect))
        .to include(code: "OK", messages: ["outer:re:q"])
      nil_result = layer(:server_stream) { |_call, &continuation| continuation.call && nil }
      expect(serve(stream_server([nil_result], :client_stream, handler), :client_stream, :grpc)[:code])
        .to eq("internal")
    end

    describe "server rule 6" do
      let(:original) { RuntimeError.new("handler") }
      let(:handler) do
        original = self.original
        ->(_duplex, _context) { raise original }
      end

      it "renders the handler's failure when a layer rescues it and returns" do
        rescuing = layer(:server_stream) do |_call, &continuation|
          continuation.call
        rescue RuntimeError
          :recovered
        end
        expect(serve(stream_server([rescuing], :bidi_stream, handler), :bidi_stream, :grpc)[:code]).to eq("internal")
        expect(only(outcomes).error).to equal(original)
      end

      it "renders an inner translation when the outer layer rescues it and returns" do
        translation = ConnectRPC::Error.new(:invalid_argument, "Translated")
        inner = layer(:server_stream) do |_call, &continuation|
          continuation.call
        rescue RuntimeError
          raise translation
        end
        outer = layer(:server_stream) do |_call, &continuation|
          continuation.call
        rescue ConnectRPC::Error
          :recovered
        end
        expect(serve(stream_server([outer, inner], :bidi_stream, handler), :bidi_stream, :connect)[:code])
          .to eq("invalid_argument")
        expect(only(outcomes)).to have_attributes(error: original, normalized_error: translation)
      end
    end
  end

  describe "server headers and trailers" do
    it "rejects response metadata writes after publication and publishes writes made before it" do
      late = []
      heads = []
      writing = layer(:server_stream) do |call, &continuation|
        call.wrap_stream do |stream|
          SimpleDelegator.new(stream).tap do |wrapper|
            wrapper.define_singleton_method(:send_message) do |message|
              call.context.response_metadata["x-wrapper"] = "early"
              __getobj__.send_message(message)
            end
          end
        end
        continuation.call.tap do
          late << (begin
            call.context.response_metadata["x-late"] = "late"
          rescue FrozenError => e
            e
          end)
        end
      end
      result = serve(stream_server([writing], :server_stream, echo_handler(:server_stream)), :server_stream,
                     :connect, heads: heads)
      expect(result[:code]).to eq("OK")
      expect(only(late)).to be_a(FrozenError)
      expect(only(heads).headers).to include("x-wrapper" => ["early"])
      expect(only(heads).headers).not_to have_key("x-late")
    end

    %i[connect grpc].each do |protocol|
      it "sends trailers added after yield, including after a handler error, over #{protocol}" do
        trailing = layer(:server_stream) do |call, &continuation|
          continuation.call
        ensure
          call.context.response_trailers["x-after"] = "yes"
        end
        ok = serve(stream_server([trailing], :bidi_stream, echo_handler(:bidi_stream)), :bidi_stream, protocol)
        failing = serve(stream_server([trailing], :bidi_stream, ->(_duplex, _context) { raise "handler" }),
                        :bidi_stream, protocol)
        expect([ok[:code], ok[:trailers]["x-after"]]).to eq(["OK", ["yes"]])
        expect([failing[:code], failing[:trailers]["x-after"]]).to eq(["internal", ["yes"]])
      end
    end
  end

  describe "non-local exits in stream session cleanup" do
    # The exchange's abort runs inside session abandonment, after the
    # application error was recorded as independent.
    it "lets a thread killed during abandonment die without raising the application error" do
      count_closes
      rescued = false
      exchange = peer(:connect, [], on_abort: -> { Thread.current.kill })
      client = stream_client([], exchange: exchange)
      worker = Thread.new do
        invoke(client, :bidi_stream) { raise "application" }
      rescue StandardError
        rescued = true
      end
      expect(worker.join(5)).to equal(worker)
      expect(worker.status).to be(false)
      expect(rescued).to be(false)
      expect(counts.values_at(:abort, :session_close)).to eq([1, 1])
      expect(only(completions).delivery).to eq(:aborted)
    end

    it "lets a fiber killed during abandonment die without raising the application error" do
      count_closes
      rescued = false
      exchange = peer(:connect, [], on_abort: -> { Fiber.yield(:aborting) })
      client = stream_client([layer(:client_stream) { |_call, &continuation| continuation.call }], exchange: exchange)
      fiber = Fiber.new do
        invoke(client, :bidi_stream) { raise "application" }
      rescue StandardError
        rescued = true
      end
      expect(fiber.resume).to eq(:aborting)
      fiber.kill
      expect(fiber.alive?).to be(false)
      expect(rescued).to be(false)
      expect(counts[:session_close]).to eq(1)
    end
  end

  describe "continuation misuse and non-local exits" do
    describe "on the client" do
      it "rejects a second yield, including one the layer swallows" do
        swallowed = []
        twice = layer(:client_stream) do |_call, &continuation|
          continuation.call
          continuation.call
        rescue ConnectRPC::InterceptorStateError => e
          swallowed << e
          :swallowed
        end
        expect { invoke(stream_client([twice]), :server_stream) { |reader| reader.each { nil } } }
          .to(raise_error { |error| expect(error).to equal(only(swallowed)) })
        expect(only(opened)).to be_a(ConnectRPC::StreamRequest)
        expect(counts.values_at(:finish, :abort)).to eq([1, 0])
        expect(only(completions).delivery).to eq(:completed)
      end

      it "rejects a continuation from another thread and the layer's later yield, without opening" do
        foreign = layer(:client_stream) do |_call, &continuation|
          Thread.new { continuation.call }.join
        rescue ConnectRPC::InterceptorStateError
          continuation.call
        end
        expect { invoke(stream_client([foreign]), :bidi_stream) { nil } }
          .to raise_error(ConnectRPC::InterceptorStateError, /another fiber or thread/)
        expect(opened).to be_empty
        expect(only(completions).delivery).to eq(:unobserved)
      end

      it "rejects a captured continuation after its interceptor returned" do
        captured = nil
        capturing = layer(:client_stream) do |_call, &continuation|
          captured = continuation
          continuation.call
        end
        invoke(stream_client([capturing]), :server_stream) { |reader| reader.each { nil } }
        expect { captured.call }.to raise_error(ConnectRPC::InterceptorStateError, /after its interceptor returned/)
        expect(opened.size).to eq(1)
        expect(only(completions).delivery).to eq(:completed)
      end

      it "fails with InterceptorStateError when an application throw is caught by the layer" do
        count_closes
        catching = layer(:client_stream) do |_call, &continuation|
          catch(:skip) { continuation.call }
          nil
        end
        expect { invoke(stream_client([catching]), :bidi_stream) { throw :skip } }
          .to raise_error(ConnectRPC::InterceptorStateError, /without returning or raising/)
        expect(counts.values_at(:abort, :finish, :session_close)).to eq([1, 0, 1])
        expect(only(completions).delivery).to eq(:aborted)
      end

      it "lets a thread killed in a layer after yield die with single cleanup" do
        count_closes
        resumed = false
        killing = layer(:client_stream) do |_call, &continuation|
          continuation.call
          Thread.current.kill
        end
        client = stream_client([killing])
        worker = Thread.new do
          invoke(client, :server_stream) { |reader| reader.each { nil } }
          resumed = true
        rescue StandardError
          resumed = true
        end
        expect(worker.join(5)).to equal(worker)
        expect(resumed).to be(false)
        expect(counts.values_at(:finish, :abort, :session_close)).to eq([1, 0, 1])
        expect(only(completions).delivery).to eq(:completed)
      end
    end

    describe "on the server" do
      misuse = {
        "a second yield the layer swallows" => lambda { |_call, &continuation|
          continuation.call
          begin
            continuation.call
          rescue ConnectRPC::InterceptorStateError
            nil
          end
        },
        "a foreign-thread continuation" => lambda do |_call, &continuation|
          Thread.new { continuation.call }.join
        rescue ConnectRPC::InterceptorStateError
          continuation.call
        end,
        "an application throw caught by the layer" => lambda { |_call, &continuation|
          catch(:skip) { continuation.call }
        }
      }
      misuse.each do |label, body|
        it "fails internal with single cleanup for #{label}" do
          count_closes
          handler = lambda do |duplex, _context|
            duplex.send_message("re:#{duplex.receive}")
            throw :skip if label.include?("throw")
          end
          result = serve(stream_server([layer(:server_stream, &body)], :bidi_stream, handler), :bidi_stream, :grpc)
          expect(result).to include(code: "internal", terminals: 1)
          expect(counts.values_at(:context_close, :session_close)).to eq([1, 1])
          expect(only(outcomes).error).to be_a(ConnectRPC::InterceptorStateError)
        end
      end

      it "rejects a captured continuation after the interceptor returned" do
        captured = nil
        capturing = layer(:server_stream) do |_call, &continuation|
          captured = continuation
          continuation.call
        end
        expect(serve(stream_server([capturing], :bidi_stream, echo_handler(:bidi_stream)), :bidi_stream,
                     :connect)[:code]).to eq("OK")
        expect { captured.call }.to raise_error(ConnectRPC::InterceptorStateError, /after its interceptor returned/)
      end

      it "lets a fiber killed inside a continuation die with single cleanup" do
        count_closes
        rescued = false
        handler = lambda do |duplex, _context|
          duplex.send_message("re:#{duplex.receive}")
          Fiber.yield(:handling)
        end
        target = stream_server([layer(:server_stream) { |_call, &continuation| continuation.call }], :bidi_stream,
                               handler)
        fiber = Fiber.new do
          serve(target, :bidi_stream, :connect)
        rescue StandardError
          rescued = true
        end
        expect(fiber.resume).to eq(:handling)
        fiber.kill
        expect(fiber.alive?).to be(false)
        expect(rescued).to be(false)
        expect(counts.values_at(:context_close, :session_close)).to eq([1, 1])
        expect(outcomes.size).to be <= 1
      end
    end
  end
end
