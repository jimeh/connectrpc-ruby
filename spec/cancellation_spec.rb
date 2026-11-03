# frozen_string_literal: true

require "connectrpc/transports/net_http"

RSpec.describe "Explicit unary cancellation" do
  it "does not expose the original transport failure through the public cause" do
    request = ConnectRPC::TransportRequest.new(url: URI("http://example.invalid"), http_method: "POST",
                                               headers: {}, body: "", response_limit: 100, deadline: nil)
    transport = ConnectRPC::Transports::NetHTTP.new
    allow(transport).to receive(:connection).and_raise(SocketError, "private network information")
    expect { transport.call(request) }.to raise_error(ConnectRPC::Error) do |error|
      expect(error.code).to eq("unavailable")
      expect(error.cause).to be_nil
    end
  end
end

RSpec.describe ConnectRPC::CancellationSource do
  it "owns one permanent read-only shareable token and transitions only once" do
    source = described_class.new
    expect(source.token).to equal(source.token)
    expect(source.token.reason).to be_nil
    expect(source.token).not_to respond_to(:cancel)
    expect(source.cancel).to be(true)
    expect(source.cancel).to be(false)
    expect(source.token).to be_cancelled
    expect(source.token.reason).to eq(:canceled)
    expect { source.token.check! }.to raise_error(ConnectRPC::Error) do |e|
      expect([e.code, e.message, e.cause]).to eq(["canceled", "Call canceled", nil])
    end
  end

  it "invokes already-cancelled listeners synchronously and isolates failing listeners" do
    source = described_class.new
    calls = []
    source.token.on_cancel { raise "application failure" }
    source.token.on_cancel { calls << :first }
    expect(source.cancel).to be(true)
    registration = source.token.on_cancel { calls << :immediate }
    expect(calls).to eq(%i[first immediate])
    expect(registration.unregister).to be(false)
  end

  it "suppresses a snapshotted listener unregistered before its invocation claim" do
    source = described_class.new
    pending = nil
    calls = []
    source.token.on_cancel { expect(pending.unregister).to be(true) }
    pending = source.token.on_cancel { calls << :unregistered }
    expect(source.cancel).to be(true)
    expect(calls).to be_empty
    expect(pending.unregister).to be(false)
    expect(pending.instance_variable_get(:@callback)).to be_nil
  end

  it "never waits for a running callback and permits reentrant unregister/cancel" do
    source = described_class.new
    started = Queue.new
    release = Queue.new
    registration = nil
    registration = source.token.on_cancel do
      expect(registration.unregister).to be(false)
      expect(source.cancel).to be(false)
      started << true
      release.pop
    end
    worker = Thread.new { source.cancel }
    expect(started.pop(timeout: 1)).to be(true)
    expect(registration.unregister).to be(false)
    release << true
    expect(worker.join(1)).to equal(worker)
    expect(worker.value).to be(true)
    expect(registration.instance_variable_get(:@callback)).to be_nil
  ensure
    release&.push(true)
    worker&.join(1)
  end

  it "releases a Context registration if metadata construction fails" do
    source = described_class.new
    expect do
      ConnectRPC::Context.new(request_metadata: { "x-invalid" => "line\nfeed" }, cancellation: source.token)
    end.to raise_error(ArgumentError)
    expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
  end

  it "releases registrations before cancellation and after invocation" do
    source = described_class.new
    100.times do
      registration = source.token.on_cancel { nil }
      expect(registration.unregister).to be(true)
    end
    expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
    source.cancel
    expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
  end
end

RSpec.describe "Call cancellation arbitration" do
  let(:method) do
    ConnectRPC::Method.new(service: "example.Test", name: "Call", request_type: String, response_type: String,
                           idempotency: :no_side_effects)
  end
  let(:source) { ConnectRPC::CancellationSource.new }
  let(:binding) do
    Class.new do
      def encode(message, **) = message
      def decode(bytes, **) = bytes
    end.new
  end
  let(:reply) do
    ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] }, body: "ok")
  end

  def call_client(transport: ->(_) { reply }, **)
    ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport, **)
  end

  [nil, 30].each do |timeout|
    it "never encodes or dispatches a pre-cancelled call with timeout #{timeout.inspect}" do
      source.cancel
      expect(binding).not_to receive(:encode)
      transport = double("transport")
      expect(transport).not_to receive(:call)
      expect do
        call_client(transport: transport).call(method, "request", timeout: timeout, cancellation: source.token)
      end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
    end
  end

  it "checks cancellation after encoding without mutating the request or metadata" do
    request = "request"
    metadata = { "x-request" => ["caller"].freeze }.freeze
    allow(binding).to receive(:encode) {
      source.cancel
      request
    }
    transport = double("transport")
    expect(transport).not_to receive(:call)
    expect do
      call_client(transport: transport).call(method, request, metadata: metadata, cancellation: source.token)
    end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
    expect(metadata).to eq("x-request" => ["caller"])
    expect(request).to eq("request")
  end

  it "rechecks source directly at final decode commit even while an earlier listener is blocked" do
    entered = Queue.new
    release = Queue.new
    source.token.on_cancel do
      entered << true
      release.pop
    end
    worker = nil
    allow(binding).to receive(:decode) do
      worker = Thread.new { source.cancel }
      expect(entered.pop(timeout: 1)).to be(true)
      "decoded"
    end
    expect do
      call_client.call(method, "request", timeout: nil, cancellation: source.token)
    end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
  ensure
    release&.push(true)
    worker&.join(1)
  end

  %i[decode transport].each do |phase|
    it "accepts cancellation over an escaping #{phase} failure and removes sensitive causes" do
      failure = ConnectRPC::CodecError.new("private detail")
      transport = if phase == :transport
                    lambda { |_|
                      source.cancel
                      raise failure
                    }
                  else
                    allow(binding).to receive(:decode) {
                      source.cancel
                      raise failure
                    }
                    ->(_) { reply }
                  end
      invoke = -> { call_client(transport: transport).call(method, "request", cancellation: source.token) }
      expect(&invoke).to raise_error(ConnectRPC::Error) do |e|
        expect([e.code, e.cause]).to eq(["canceled", nil])
      end
    end
  end

  it "leaves committed success final and keeps shared-source subsequent calls independent" do
    client = call_client
    expect(client.call(method, "first", timeout: nil, cancellation: source.token).message).to eq("ok")
    expect(client.call(method, "second", timeout: nil, cancellation: source.token).message).to eq("ok")
    source.cancel
    expect { client.call(method, "third", cancellation: source.token) }.to raise_error(ConnectRPC::Error) do |e|
      expect(e.code).to eq("canceled")
    end
    expect(client.call(method, "unrelated").message).to eq("ok")
  end

  it "rejects a source or arbitrary object in the public Client keyword" do
    [source, Object.new, false].each do |invalid|
      expect { call_client.call(method, "request", cancellation: invalid) }.to raise_error(ArgumentError)
    end
  end

  it "keeps accepted cancellation through later expiry and gives already-expired deadlines precedence" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    context = ConnectRPC::Context.new(deadline: deadline, cancellation: source.token)
    source.cancel
    now = 2.0
    expect(context.cancellation_reason).to eq(:canceled)
    expect { context.check! }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
    second = ConnectRPC::CancellationSource.new
    expired = ConnectRPC::Context.new(deadline: deadline, cancellation: second.token)
    second.cancel
    expect(expired.cancellation_reason).to eq(:deadline_exceeded)
  ensure
    context&.close
    expired&.close
  end

  it "does not let Context processing commit complete its input cancellation state" do
    state = ConnectRPC::CallCancellation.new(cancellation: source.token)
    context = ConnectRPC::Context.new(cancellation: state)
    context.commit!
    source.cancel
    expect(state.reason).to eq(:canceled)
    expect(context.cancellation_reason).to be_nil
  ensure
    context&.close
    state&.close
  end

  it "checks real server input failures for cancellation before classifying internal" do
    input = Object.new
    cancellation_source = source
    input.define_singleton_method(:read) do |_|
      cancellation_source.cancel
      raise IOError, "reset body"
    end
    service = ConnectRPC::Service.new(name: method.service, methods: [method])
    server = ConnectRPC::Server.new(services: [service], binding: binding,
                                    handlers: { method.path => ->(*) { raise "No invocation" } })
    result = server.call(path: method.path, http_method: "POST", headers: { "content-type" => ["application/proto"] },
                         input: input, cancellation: source.token)
    expect(JSON.parse(result.body).fetch("code")).to eq("canceled")
  end
end

RSpec.describe "Ordered Client cancellation and deadline outcomes" do
  let(:method) do
    ConnectRPC::Method.new(service: "example.Order", name: "Call", request_type: String, response_type: String)
  end
  let(:source) { ConnectRPC::CancellationSource.new }
  let(:binding) do
    Class.new do
      def encode(message, **) = message
      def decode(bytes, **) = bytes
    end.new
  end
  let(:reply) { ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] }, body: "ok") }

  it "keeps accepted source cancellation through later expiry and decoding" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    allow(ConnectRPC::Deadline).to receive(:new).with(1).and_return(deadline)
    transport = lambda do |_request|
      source.cancel
      now = 2.0
      reply
    end
    client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport)
    expect { client.call(method, "request", timeout: 1, cancellation: source.token) }
      .to raise_error(ConnectRPC::Error) { |e| expect([e.code, e.cause]).to eq(["canceled", nil]) }
  end

  it "accepts real already-expired deadline before a later source request and transport failure" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    allow(ConnectRPC::Deadline).to receive(:new).with(1).and_return(deadline)
    transport = lambda do |_request|
      now = 2.0
      source.cancel
      raise IOError, "private transport"
    end
    client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport)
    expect { client.call(method, "request", timeout: 1, cancellation: source.token) }
      .to raise_error(ConnectRPC::Error) { |e| expect([e.code, e.cause]).to eq(["deadline_exceeded", nil]) }
  end

  it "rechecks actual expiry after a decoding failure and retains the original failure for lifecycle only" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    allow(ConnectRPC::Deadline).to receive(:new).with(1).and_return(deadline)
    original = ConnectRPC::CodecError.new("private codec")
    allow(binding).to receive(:decode) {
      now = 2.0
      raise original
    }
    outcomes = []
    observer = Object.new
    observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding,
                                    transport: ->(_) { reply }, instrumentation: bus)
    expect { client.call(method, "request", timeout: 1, cancellation: source.token) }
      .to raise_error(ConnectRPC::Error) { |e| expect([e.code, e.cause]).to eq(["deadline_exceeded", nil]) }
    expect(outcomes.size).to eq(1)
    expect(outcomes.first.error).to equal(original)
    expect(outcomes.first.normalized_error.code).to eq("deadline_exceeded")
  end

  [true, false].each do |success|
    it "keeps a committed #{success ? 'response' : 'error'} final through reentrant instrumentation cancellation" do
      outcomes = []
      observer = Object.new
      cancellation_source = source
      observer.define_singleton_method(:processing_finished) do |outcome|
        outcomes << outcome
        cancellation_source.cancel
        outcome.code
      end
      subscriber = Object.new
      subscriber.define_singleton_method(:start) { |_| observer }
      bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
      failure = ConnectRPC::Error.new(:unavailable, "safe")
      transport = ->(_) { success ? reply : (raise failure) }
      client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport,
                                      instrumentation: bus)
      if success
        expect(client.call(method, "request", cancellation: source.token).message).to eq("ok")
      else
        expect { client.call(method, "request", cancellation: source.token) }.to(raise_error do |e|
          expect(e).to equal(failure)
        end)
      end
      expect(source.token).to be_cancelled
      expect(outcomes.map(&:code)).to eq([success ? "OK" : "unavailable"])
    end
  end
end

RSpec.describe "Shared cancellation across concurrent actual Client calls" do
  it "cancels every open call without completing the source or changing an unrelated call" do
    source = ConnectRPC::CancellationSource.new
    entered = Queue.new
    release = Queue.new
    binding = Class.new do
      def encode(message, **) = message
      def decode(bytes, **) = bytes
    end.new
    method = ConnectRPC::Method.new(service: "example.Shared", name: "Call", request_type: String,
                                    response_type: String)
    transport = lambda do |request|
      entered << request.body
      release.pop
      ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                        body: request.body)
    end
    client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport)
    workers = %w[first second].map do |message|
      Thread.new do
        client.call(method, message, timeout: nil, cancellation: source.token)
      rescue ConnectRPC::Error => e
        e
      end
    end
    expect(2.times.map { entered.pop(timeout: 1) }.sort).to eq(%w[first second])
    expect(source.cancel).to be(true)
    expect(source.cancel).to be(false)
    2.times { release << true }
    workers.each do |worker|
      expect(worker.join(1)).to equal(worker)
      expect(worker.value.code).to eq("canceled")
    end
    expect(source.token.reason).to eq(:canceled)
    release << true
    expect(client.call(method, "unrelated", timeout: nil).message).to eq("unrelated")
  ensure
    2.times { release&.push(true) }
    workers&.each { |worker| worker.join(1) }
  end
end

RSpec.describe "GET processing cancellation ownership" do
  it "keeps cancellation accepted before expiry through metadata extraction into the handler Context" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    source = ConnectRPC::CancellationSource.new
    binding = Class.new do
      def encode(message, **) = message
      def decode(bytes, **) = bytes
    end.new
    method = ConnectRPC::Method.new(service: "example.Get", name: "Call", request_type: String, response_type: String,
                                    idempotency: :no_side_effects)
    service = ConnectRPC::Service.new(name: method.service, methods: [method])
    target = ConnectRPC::Server.new(services: [service], binding: binding,
                                    handlers: { method.path => ->(*) { raise "Must not invoke handler" } })
    allow(target).to receive(:request_deadline).and_return(deadline)
    allow(ConnectRPC::Protocol).to receive(:metadata).and_wrap_original do |original, *args|
      value = original.call(*args)
      source.cancel
      now = 2.0
      value
    end
    response = target.call(path: method.path, http_method: "GET", headers: {}, input: nil,
                           query: "connect=v1&encoding=json&message=hello", cancellation: source.token)
    expect(JSON.parse(response.body).fetch("code")).to eq("canceled")
  end
end

RSpec.describe "Deadline observation across the read-only cancellation SPI" do
  it "rechecks real expiry after a source observation before accepting cancellation" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    source = ConnectRPC::CancellationSource.new
    entered = Queue.new
    release = Queue.new
    source.token.on_cancel do
      entered << true
      release.pop
    end
    spi = Object.new
    spi.define_singleton_method(:on_cancel) { |&listener| source.token.on_cancel(&listener) }
    spi.define_singleton_method(:cancelled?) { source.token.cancelled? }
    spi.define_singleton_method(:check!) { source.token.check! }
    spi.define_singleton_method(:reason) do
      observed = source.token.reason
      now = 2.0
      observed
    end
    context = ConnectRPC::Context.new(deadline: deadline, cancellation: spi)
    worker = Thread.new { source.cancel }
    expect(entered.pop(timeout: 1)).to be(true)
    expect(context.cancellation_reason).to eq(:deadline_exceeded)
  ensure
    context&.close
    release&.push(true)
    worker&.join(1)
  end
end
