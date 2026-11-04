# frozen_string_literal: true

require "stringio"
require "connectrpc/bindings/google_protobuf"

RSpec.describe "Lifecycle execution boundaries" do
  let(:rpc) { ConnectRPC::Method.new(service: "test.Escape", name: "Call", request_type: String, response_type: String) }
  let(:binding) do
    Object.new.tap do |codec|
      codec.define_singleton_method(:encode) { |value, **| value }
      codec.define_singleton_method(:decode) { |value, **| value }
    end
  end
  let(:processing) { [] }
  let(:completions) { [] }
  let(:bus) do
    observer = Object.new
    outcomes = processing
    finished = completions
    observer.define_singleton_method(:start) { |_| observer }
    observer.define_singleton_method(:processing_finished) { |value| outcomes << value }
    observer.define_singleton_method(:finished) { |value| finished << value }
    ConnectRPC::Instrumentation.new(subscribers: [observer])
  end

  def server(**, &handler)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, methods: [rpc])],
                           binding: binding, instrumentation: bus,
                           handlers: { rpc.path => handler || ->(message, _) { message } }, **)
  end

  def dispatch(target, **)
    target.call(path: rpc.path, http_method: "POST", headers: { "content-type" => ["application/proto"] },
                input: StringIO.new("hello"), **)
  end

  def client(&transport)
    ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, instrumentation: bus,
                           transport: transport)
  end

  def inside_rescue
    raise "unrelated ambient error"
  rescue RuntimeError
    yield
  end

  it "observes successful client and server calls inside rescue without the ambient exception" do
    target = server
    caller = client { |_| dispatch(target) }
    inside_rescue { expect(caller.call(rpc, "hello").message).to eq("hello") }
    expect(processing.size).to eq(2)
    expect(processing).to all(have_attributes(code: "OK", error: nil, normalized_error: nil))
    expect(completions.size).to eq(2)
  end

  it "preserves a client throw value inside rescue without inventing success or an error" do
    value = Object.new
    caller = client { |_| throw :escape, value }
    expect(inside_rescue { catch(:escape) { caller.call(rpc, "hello") } }).to equal(value)
    expect(processing).to contain_exactly(have_attributes(code: nil, error: nil, normalized_error: nil))
    expect(completions.size).to eq(1)
  end

  it "preserves a server throw value inside rescue without attributing the ambient exception" do
    value = Object.new
    target = server { throw :escape, value }
    expect(inside_rescue { catch(:escape) { dispatch(target) } }).to equal(value)
    expect(processing).to contain_exactly(have_attributes(code: nil, error: nil, normalized_error: nil))
    expect(completions.size).to eq(1)
  end

  %i[client server].each do |role|
    it "retains the original fatal #{role} exception and backtrace" do
      original = Class.new(Exception).new("fatal") # rubocop:disable Lint/InheritException -- exercise non-StandardError exits
      original.set_backtrace(["original:1"])
      expect do
        if role == :client
          client { |_| raise original }.call(rpc, "hello")
        else
          dispatch(server { raise original })
        end
      end.to(raise_error { |error| expect(error).to equal(original) })
      expect(original.backtrace).to eq(["original:1"])
      expect(processing).to contain_exactly(have_attributes(code: nil, error: original))
      expect(completions.size).to eq(1)
    end
  end

  it "captures an exception escaping error rendering while retaining the original processing failure" do
    original = RuntimeError.new("handler")
    escaping = Class.new(Exception).new("render") # rubocop:disable Lint/InheritException -- fatal rendering escape
    target = server { raise original }
    allow(ConnectRPC::Protocol).to receive(:error_body).and_raise(escaping)
    expect { dispatch(target) }.to(raise_error { |error| expect(error).to equal(escaping) })
    expect(processing).to contain_exactly(have_attributes(code: nil, error: original))
    expect(completions.size).to eq(1)
  end

  it "captures cleanup failure before notifying processing observers" do
    escaping = Class.new(Exception).new("cleanup") # rubocop:disable Lint/InheritException -- fatal cleanup escape
    target = server
    allow(ConnectRPC::Context).to receive(:new).and_wrap_original do |constructor, **options|
      constructor.call(**options).tap do |context|
        allow(context).to receive(:close).and_wrap_original do |close|
          close.call
          raise escaping
        end
      end
    end
    expect { dispatch(target) }.to(raise_error { |error| expect(error).to equal(escaping) })
    expect(processing).to contain_exactly(have_attributes(code: nil, error: escaping))
    expect(completions.size).to eq(1)
  end

  it "preserves a cleanup throw without inventing a successful processing result" do
    value = Object.new
    target = server
    allow(ConnectRPC::Context).to receive(:new).and_wrap_original do |constructor, **options|
      constructor.call(**options).tap do |context|
        allow(context).to receive(:close).and_wrap_original do |close|
          close.call
          throw :cleanup, value
        end
      end
    end
    expect(inside_rescue { catch(:cleanup) { dispatch(target) } }).to equal(value)
    expect(processing).to contain_exactly(have_attributes(code: nil, error: nil))
    expect(completions.size).to eq(1)
  end

  def native_server(protocol, &handler)
    if protocol == :connect
      stream = ConnectRPC::ServerStreamMethod.new(service: rpc.service, name: rpc.name,
                                                  request_type: String, response_type: String)
      return ConnectRPC::Server.new(
        services: [ConnectRPC::Service.new(name: rpc.service, methods: [stream])], binding: binding,
        instrumentation: bus, handlers: { rpc.path => lambda { |request, writer, context|
          writer.send_message(handler ? handler.call(request, context) : request)
        } }
      )
    end

    server(protocol: protocol, status_codec: ConnectRPC::Bindings::GoogleProtobuf.new, &handler)
  end

  def native_call(target, protocol, **)
    media = { grpc: "application/grpc", connect: "application/connect+proto",
              grpc_web: "application/grpc-web" }.fetch(protocol)
    source = StringIO.new("#{[0, 5].pack('CN')}hello")
    source.define_singleton_method(:read) { |length, **| super(length) }
    output = Object.new
    output.define_singleton_method(:write) { |_, **| nil }
    output.define_singleton_method(:finish) { |**| nil }
    output.define_singleton_method(:abort) { nil }
    entry = { grpc: :call_grpc, connect: :call_stream, grpc_web: :call_grpc_web }.fetch(protocol)
    arguments = { path: rpc.path, http_method: "POST", headers: { "content-type" => [media] },
                  input: source, output: output, publish: ->(_) {} }
    arguments[:http2] = true unless protocol == :grpc_web
    target.public_send(entry, **arguments, **)
  end

  [["GET", "application/grpc", 405, { "allow" => ["POST"] }],
   ["POST", "text/plain", 415, { "accept-post" => ["application/grpc+proto"] }]].each do |verb, media, status, fields|
    it "returns gRPC #{status} with a host handle without beginning processing" do
      target = native_server(:grpc)
      handle = target.start_call(path: rpc.path, headers: {})
      incoming = Object.new
      expect(incoming).not_to receive(:read)
      response = native_call(target, :grpc, http_method: verb, headers: { "content-type" => [media] },
                                            lifecycle: handle, input: incoming)
      expect(response.status).to eq(status)
      expect(response.headers).to include(fields)
      expect(processing).to be_empty
      expect(completions).to be_empty
      handle.finish(delivery: :completed, http_status: response.status)
      handle.finish(delivery: :aborted)
      expect(completions).to contain_exactly(have_attributes(outcome: nil, processing_state: :not_started,
                                                             delivery: :completed, http_status: status))
    end
  end

  [[:grpc, { http2: false }], [:connect, {}]].each do |protocol, options|
    it "leaves host processing unbegun when gRPC rejects #{protocol} with #{options}" do
      target = native_server(protocol)
      handle = target.start_call(path: rpc.path, headers: {})
      response = native_call(target, :grpc, lifecycle: handle, **options)
      expect(response.status).to eq(415)
      expect(response.headers).to eq({})
      expect(processing).to be_empty
      expect(completions).to be_empty
      handle.finish(delivery: :completed)
      expect(completions).to contain_exactly(have_attributes(outcome: nil, processing_state: :not_started))
    end
  end

  it "preserves preflight cancellation and leaves the host handle unbegun" do
    target = native_server(:grpc)
    handle = target.start_call(path: rpc.path, headers: {})
    original = ConnectRPC::Error.new(:canceled, "canceled")
    cancellation = Object.new
    cancellation.define_singleton_method(:check!) { raise original }
    expect { native_call(target, :grpc, http_method: "GET", lifecycle: handle, cancellation: cancellation) }
      .to(raise_error { |error| expect(error).to equal(original) })
    expect(processing).to be_empty
    expect(completions).to be_empty
    handle.finish(delivery: :aborted, error: original)
    expect(completions).to contain_exactly(have_attributes(outcome: nil, error: original))
  end

  %i[grpc connect grpc_web].each do |protocol|
    it "leaves foreign and reused #{protocol} handles under their original ownership" do
      target = native_server(protocol)
      foreign = native_server(protocol)
      handle = foreign.start_call(path: rpc.path, headers: {})
      expect { native_call(target, protocol, lifecycle: handle) }.to raise_error(ArgumentError)
      expect(processing).to be_empty
      expect(completions).to be_empty
      expect(native_call(foreign, protocol, lifecycle: handle)).to be_nil
      outcome = processing.fetch(0)
      expect(outcome).to have_attributes(code: "OK", error: nil), outcome.error&.full_message
      expect { native_call(foreign, protocol, lifecycle: handle) }.to raise_error(ArgumentError)
      expect(processing).to eq([outcome])
      expect(completions).to be_empty
      handle.finish(delivery: :completed)
      expect(completions).to contain_exactly(have_attributes(delivery: :completed, outcome: outcome))
      expect { native_call(foreign, protocol, lifecycle: handle) }.to raise_error(ArgumentError)
      expect(processing).to eq([outcome])
      expect(completions.size).to eq(1)
    end

    it "preserves #{protocol} throw values inside rescue without ambient failures" do
      value = Object.new
      target = native_server(protocol) { throw :escape, value }
      expect(inside_rescue { catch(:escape) { native_call(target, protocol) } }).to equal(value)
      expect(processing).to contain_exactly(have_attributes(code: nil, error: nil))
      expect(completions).to contain_exactly(have_attributes(delivery: :unobserved))
    end

    it "retains the actual fatal #{protocol} escape" do
      original = Class.new(Exception).new("fatal") # rubocop:disable Lint/InheritException -- native fatal escape
      target = native_server(protocol) { raise original }
      expect { native_call(target, protocol) }.to(raise_error { |error| expect(error).to equal(original) })
      expect(processing).to contain_exactly(have_attributes(code: nil, error: original))
      expect(completions.size).to eq(1)
    end
  end
end
