# frozen_string_literal: true

require "stringio"

RSpec.describe "Streaming exception ownership and completion" do
  let(:observer) do
    Object.new.tap do |value|
      value.define_singleton_method(:start) { |_| self }
      value.define_singleton_method(:processing_finished) { |outcome| @outcome = outcome }
      value.define_singleton_method(:finished) { |completion| @completion = completion }
      value.define_singleton_method(:outcome) { @outcome }
      value.define_singleton_method(:completion) { @completion }
    end
  end
  let(:bus) { ConnectRPC::Instrumentation.new(subscribers: [observer]) }
  let(:binding) do
    Object.new.tap do |value|
      value.define_singleton_method(:encode) { |message, **| message }
      value.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end

  def frame(bytes, flags = 0) = [flags, bytes.bytesize].pack("CN") + bytes

  def rpc(shape)
    type = { server_stream: ConnectRPC::ServerStreamMethod, client_stream: ConnectRPC::ClientStreamMethod,
             bidi_stream: ConnectRPC::BidiStreamMethod }.fetch(shape)
    type.new(service: "correction.Streams", name: "Call", request_type: String, response_type: String)
  end

  def core(body = frame("reply") + frame("{}", 2), coding: nil, cleanup: nil, during_cleanup: nil)
    input = StringIO.new(body)
    exchange = Object.new
    fields = { "content-type" => ["application/connect+proto"] }
    fields["connect-content-encoding"] = [coding] if coding
    exchange.define_singleton_method(:response_head) { |**| ConnectRPC::StreamHead.new(status: 200, headers: fields) }
    exchange.define_singleton_method(:read) { |length, **| input.read(length) }
    exchange.define_singleton_method(:write) { |_| nil }
    exchange.define_singleton_method(:close_send) { nil }
    exchange.define_singleton_method(:finish) { |**| nil }
    exchange.define_singleton_method(:abort) { nil }
    transport = Object.new
    transport.define_singleton_method(:streaming_capabilities) { %i[server_stream client_stream bidi_stream] }
    transport.define_singleton_method(:open_stream) do |_, &work|
      work.call(exchange)
    ensure
      during_cleanup&.call
      raise cleanup if cleanup
    end
    client = ConnectRPC::Client.new(base_url: "https://correction.invalid", binding: binding, transport: transport,
                                    instrumentation: bus, accept_encoding: [:gzip])
    [client, exchange]
  end

  def invoke(client, shape, **, &)
    args = shape == :server_stream ? [rpc(shape), "request"] : [rpc(shape)]
    client.public_send(shape, *args, timeout: nil, **, &)
  end

  %i[server_stream client_stream bidi_stream].each do |shape|
    [nil, IOError.new("cleanup failure")].each do |cleanup|
      it "uses actual #{shape} escape state inside ambient rescue, cleanup #{!cleanup.nil?}" do
        client, = core(cleanup: cleanup)
        actual = nil
        begin
          begin
            raise "unrelated outer exception"
          rescue RuntimeError
            invoke(client, shape) do |endpoint|
              endpoint.each { |_| nil } unless shape == :client_stream
            end
          end
        rescue StandardError => e
          actual = e
        end
        expect(actual).to equal(cleanup)
        expect(observer.outcome.error).to equal(cleanup)
        expect(observer.outcome.code).to eq(cleanup ? nil : "OK")
        expect(observer.completion.delivery).to eq(cleanup ? :aborted : :completed)
      end
    end

    it "retains actual #{shape} cleanup failure before cancellation arbitration inside ambient rescue" do
      source = ConnectRPC::CancellationSource.new
      cleanup = IOError.new("original cleanup failure")
      client, = core(cleanup: cleanup, during_cleanup: -> { source.cancel })
      begin
        raise "unrelated outer error"
      rescue RuntimeError
        expect do
          invoke(client, shape, cancellation: source.token) do |endpoint|
            endpoint.each { |_| nil } unless shape == :client_stream
          end
        end.to raise_error(ConnectRPC::Error) { |error|
          expect(error.code).to eq("canceled")
          expect(error.cause).to be_nil
          expect(observer.outcome.normalized_error).to equal(error)
        }
      end
      expect(observer.outcome.error).to equal(cleanup)
    end
  end

  %i[server_stream bidi_stream].each do |shape|
    [false, true].each do |repeat|
      it "retains caught #{shape} remote error at scope completion, repeated receive #{repeat}" do
        terminal = '{"error":{"code":"not_found"},"metadata":{"x-tail":["retained"]}}'
        client, = core(frame(terminal, 2))
        first = nil
        expect do
          invoke(client, shape) do |reader|
            expect { reader.receive }.to raise_error(ConnectRPC::Error) { |error| first = error }
            expect(reader.trailers["x-tail"]).to eq(["retained"])
            expect { reader.receive }.to(raise_error { |error| expect(error).to equal(first) }) if repeat
          end
        end.to(raise_error { |error| expect(error).to equal(first) })
        expect(observer.outcome.code).to eq("not_found")
        expect(observer.completion.delivery).to eq(:aborted)
      end
    end
  end

  [StopIteration, ConnectRPC::StreamStateError, ConnectRPC::Error, Interrupt].each do |type|
    it "preserves independently raised #{type} identity and backtrace despite cancellation and cleanup" do
      source = ConnectRPC::CancellationSource.new
      client, = core(cleanup: IOError.new("cleanup noise"))
      original = type == ConnectRPC::Error ? type.new(:internal, "user") : type.new("user")
      original.set_backtrace(["user.rb:42"])
      expect do
        invoke(client, :bidi_stream, cancellation: source.token) do |reader|
          reader.each do |message|
            source.cancel
            raise original if message == "reply"
          end
        end
      end.to(raise_error do |error|
        expect(error).to equal(original)
        expect(error.backtrace).to eq(["user.rb:42"])
      end)
    end
  end

  %i[direct enumerator].each do |origin|
    it "propagates client #{origin} StopIteration from each" do
      client, = core
      original = StopIteration.new("user iteration")
      original.set_backtrace(["iteration.rb:7"])
      exhausted = [].each
      escaped = nil
      trace = nil
      expect do
        invoke(client, :bidi_stream) do |reader|
          reader.each do |_|
            origin == :direct ? (raise original) : exhausted.next
          rescue StopIteration => e
            escaped = e
            trace = e.backtrace.dup
            raise
          end
        end
      end.to raise_error(StopIteration) { |error|
        expect(error).to equal(escaped)
        expect(error.backtrace).to eq(trace)
      }
      expect(observer.outcome.code).not_to eq("OK")
    end

    it "reports server #{origin} StopIteration as original failure and an internal terminal" do
      original = StopIteration.new("user iteration")
      original.set_backtrace(["iteration.rb:9"])
      exhausted = [].each
      escaped = nil
      trace = nil
      method = rpc(:bidi_stream)
      target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
                                      binding: binding, instrumentation: bus,
                                      handlers: { method.path => lambda { |duplex, _|
                                        duplex.each do |_|
                                          origin == :direct ? (raise original) : exhausted.next
                                        rescue StopIteration => e
                                          escaped = e
                                          trace = e.backtrace.dup
                                          raise
                                        end
                                      } })
      input = StringIO.new(frame("request"))
      input.define_singleton_method(:read) { |length, **| super(length) }
      output = StringIO.new
      output.define_singleton_method(:write) do |bytes, **|
        super(bytes)
        nil
      end
      output.define_singleton_method(:abort) { nil }
      target.call_stream(path: method.path, http_method: "POST",
                         headers: { "content-type" => ["application/connect+proto"] },
                         input: input, output: output, publish: ->(_) {}, http2: true)
      expect(JSON.parse(output.string.byteslice(5..)).dig("error", "code")).to eq("internal")
      expect(observer.outcome.error).to be_a(StopIteration)
      expect(observer.outcome.error).to equal(escaped)
      expect(observer.outcome.error.backtrace).to eq(trace)
    end
  end

  it "keeps outer-scope receive ownership after an ordinary each break" do
    client, = core(frame("one") + frame("two") + frame("{}", 2))
    invoke(client, :bidi_stream) do |reader|
      value = reader.each { |message| break message if message == "one" }
      expect(value).to eq("one")
      expect(reader.receive).to eq("two")
      expect(reader.receive).to be_nil
    end
    expect(observer.outcome.code).to eq("OK")
  end

  def return_from_each(client)
    invoke(client, :bidi_stream) do |reader|
      reader.each { |message| return message if message == "reply" }
    end
  end

  it "preserves nonlocal return out of each and reports bounded local abandonment" do
    client, = core
    expect(return_from_each(client)).to eq("reply")
    expect(observer.outcome.normalized_error.code).to eq("canceled")
    expect(observer.completion.delivery).to eq(:aborted)
  end

  %i[closed affinity].each do |entry|
    it "arbitrates SDK #{entry} entry failure without codecs or I/O" do
      source = ConnectRPC::CancellationSource.new
      client, exchange = core
      expect(binding).not_to receive(:decode)
      expect(binding).not_to receive(:encode)
      expect(exchange).not_to receive(:read)
      expect do
        invoke(client, :bidi_stream, cancellation: source.token) do |duplex|
          duplex.abandon if entry == :closed
          source.cancel
          if entry == :affinity
            failure = Thread.new do
              duplex.receive
            rescue StandardError => e
              e
            end.value
            raise failure
          else
            duplex.receive
          end
        end
      end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
    end
  end

  it "arbitrates the actual SDK failure inside an ambient rescue" do
    client, = core(frame('{"error":{"code":"not_found"}}', 2))
    begin
      raise "unrelated outer error"
    rescue RuntimeError
      expect { invoke(client, :bidi_stream, &:receive) }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("not_found")
        expect(observer.outcome.error).to equal(error)
      }
    end
    expect(observer.outcome.code).to eq("not_found")
  end

  %i[read write].each do |direction|
    %i[canceled deadline_exceeded].each do |reason|
      it "arbitrates SDK #{direction} overlap with #{reason} before more binding or I/O" do
        source = ConnectRPC::CancellationSource.new
        clock = 0.0
        allow(Process).to receive(:clock_gettime).and_call_original
        allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { clock }
        calls = []
        binding.define_singleton_method(direction == :write ? :encode : :decode) do |message, **|
          calls << message
          Fiber.yield(:entered)
          message
        end
        client, = core
        active = nil
        expect do
          client.bidi_stream(rpc(:bidi_stream), timeout: 1, cancellation: source.token) do |duplex|
            active = Fiber.new { direction == :write ? duplex.send_message("first") : duplex.receive }
            expect(active.resume).to eq(:entered)
            reason == :canceled ? source.cancel : clock = 2.0
            direction == :write ? duplex.send_message("overlap") : duplex.receive
          end
        end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(reason.to_s) }
        expect(calls.size).to eq(1)
      ensure
        if active&.alive?
          begin
            active.resume
          rescue ConnectRPC::Error, ConnectRPC::StreamStateError
            nil
          end
        end
      end
    end

    it "keeps the original #{direction} guard busy after two rejected overlaps" do
      calls = []
      binding.define_singleton_method(direction == :write ? :encode : :decode) do |message, **|
        calls << message
        Fiber.yield(:entered) if calls.size == 1
        message
      end
      client, = core
      invoke(client, :bidi_stream) do |duplex|
        active = Fiber.new { direction == :write ? duplex.send_message("first") : duplex.receive }
        expect(active.resume).to eq(:entered)
        2.times do
          expect { direction == :write ? duplex.send_message("overlap") : duplex.receive }
            .to raise_error(ConnectRPC::StreamStateError)
        end
        expect(calls.size).to eq(1)
        active.resume
        duplex.each { |_| nil }
      end
    end
  end
  %i[reading writing].each do |first|
    %i[canceled deadline_exceeded].each do |reason|
      %i[ensure reraise].each do |unwind|
        it "preserves SDK provenance across #{first} then the opposite direction, #{unwind}, #{reason}" do
          source = ConnectRPC::CancellationSource.new
          clock = 0.0
          allow(Process).to receive(:clock_gettime).and_call_original
          allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { clock }
          binding.define_singleton_method(:decode) { |*, **| raise ConnectRPC::CodecError, "original decode failure" }
          client, = core
          expect do
            client.bidi_stream(rpc(:bidi_stream), timeout: 1, cancellation: source.token) do |duplex|
              fail_direction = ->(direction) { direction == :reading ? duplex.receive : duplex.send_message(nil) }
              fail_other = lambda do
                expect { fail_direction.call(first == :reading ? :writing : :reading) }
                  .to raise_error(ConnectRPC::Error)
                reason == :canceled ? source.cancel : clock = 2.0
              end
              if unwind == :ensure
                begin
                  fail_direction.call(first)
                ensure
                  fail_other.call
                end
              else
                primary = begin
                  fail_direction.call(first)
                rescue ConnectRPC::Error => e
                  e
                end
                fail_other.call
                raise primary
              end
            end
          end.to raise_error(ConnectRPC::Error) { |error|
            expect(error.code).to eq(reason.to_s)
            expect(error.cause).to be_nil
          }
          expect(observer.outcome.code).to eq(reason.to_s)
        end
      end
    end

    it "preserves SDK provenance across #{first} and a handled abandon failure" do
      source = ConnectRPC::CancellationSource.new
      binding.define_singleton_method(:decode) { |*, **| raise ConnectRPC::CodecError, "original decode failure" }
      client, exchange = core
      exchange.define_singleton_method(:abort) { raise IOError, "abort failure" }
      expect do
        invoke(client, :bidi_stream, cancellation: source.token) do |duplex|
          first == :reading ? duplex.receive : duplex.send_message(nil)
        ensure
          expect { duplex.abandon }.to raise_error(IOError, "abort failure")
          source.cancel
        end
      end.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("canceled")
        expect(error.cause).to be_nil
      }
    end
  end

  it "retains an independent user error after both SDK directions fail" do
    source = ConnectRPC::CancellationSource.new
    binding.define_singleton_method(:decode) { |*, **| raise ConnectRPC::CodecError, "original decode failure" }
    client, = core
    original = ConnectRPC::Error.new(:unknown, "independent application failure")
    expect do
      invoke(client, :bidi_stream, cancellation: source.token) do |duplex|
        expect { duplex.receive }.to raise_error(ConnectRPC::Error)
        expect { duplex.send_message(nil) }.to raise_error(ConnectRPC::Error)
        source.cancel
        raise original
      end
    end.to raise_error(ConnectRPC::Error) { |error| expect(error).to equal(original) }
    expect(observer.outcome.code).to eq("unknown")
  end

  { gzip_data: ["malformed", 1, "gzip", ConnectRPC::Compression::Malformed],
    gzip_terminal: ["malformed", 3, "gzip", ConnectRPC::Compression::Malformed],
    json: ["{", 2, nil, JSON::ParserError],
    root: ["[]", 2, nil, ArgumentError],
    metadata: ['{"metadata":[]}', 2, nil, ArgumentError],
    details: ['{"error":{"details":[{"type":"valid.Name","value":"!"}]}}', 2, nil,
              ArgumentError] }.each do |name, (payload, flags, coding, original_type)|
    it "retains original response #{name} diagnostic before safe mapping" do
      client, = core(frame(payload, flags), coding: coding)
      expect { invoke(client, :bidi_stream, &:receive) }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("internal")
        expect(error.cause).to be_nil
        expect(observer.outcome.normalized_error).to equal(error)
      }
      expect(observer.outcome.error).to be_a(original_type)
    end
  end

  it "retains malformed request gzip before invalid_argument mapping" do
    method = rpc(:bidi_stream)
    target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
                                    binding: binding, instrumentation: bus, encoding: :gzip,
                                    handlers: { method.path => ->(duplex, _) { duplex.receive } })
    input = StringIO.new(frame("malformed", 1))
    input.define_singleton_method(:read) { |length, **| super(length) }
    output = StringIO.new
    output.define_singleton_method(:write) do |bytes, **|
      super(bytes)
      nil
    end
    output.define_singleton_method(:abort) { nil }
    target.call_stream(path: method.path, http_method: "POST",
                       headers: { "content-type" => ["application/connect+proto"],
                                  "connect-content-encoding" => ["gzip"] },
                       input: input, output: output, publish: ->(_) {}, http2: true)
    expect(JSON.parse(output.string.byteslice(5..)).dig("error", "code")).to eq("invalid_argument")
    expect(observer.outcome.error).to be_a(ConnectRPC::Compression::Malformed)
    expect(observer.outcome.normalized_error.code).to eq("invalid_argument")
    expect(observer.outcome.normalized_error.cause).to be_nil
  end

  %i[failure_check cleanup].each do |phase|
    it "records the original inflater failure when cancellation wins during #{phase}" do
      source = ConnectRPC::CancellationSource.new
      original = Zlib::DataError.new("private decompressor detail")
      inflater = Object.new
      inflater.define_singleton_method(:total_in) { 0 }
      inflater.define_singleton_method(:finished?) { false }
      inflater.define_singleton_method(:inflate) do |_|
        source.cancel if phase == :failure_check
        raise original
      end
      inflater.define_singleton_method(:close) do
        source.cancel if phase == :cleanup
        nil
      end
      allow(Zlib::Inflate).to receive(:new).and_return(inflater)
      client, = core(frame("malformed", 1), coding: "gzip")
      expect { invoke(client, :bidi_stream, cancellation: source.token, &:receive) }
        .to raise_error(ConnectRPC::Error) { |error|
          expect(error.code).to eq("canceled")
          expect(error.cause).to be_nil
        }
      if phase == :failure_check
        expect(observer.outcome.error).to equal(original)
      else
        expect(observer.outcome.error).to be_a(ConnectRPC::Compression::Malformed)
      end
      expect(observer.outcome.normalized_error.code).to eq("canceled")
    end
  end
end
