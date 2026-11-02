# frozen_string_literal: true

require "spec_helper"
require "stringio"
require "socket"

RSpec.describe "RPC lifecycle instrumentation" do
  let(:rpc) { ConnectRPC::Method.new(service: "test.Echo", name: "Echo", request_type: String, response_type: String) }
  let(:binding) do
    Class.new do
      def encode(value, **) = value
      def decode(value, **) = value
    end.new
  end
  let(:events) { [] }
  let(:subscriber) do
    log = events
    Class.new do
      define_method(:start) do |call|
        log << [:start, call]
        observer = Object.new
        observer.define_singleton_method(:attach) do
          log << [:attach]
          Object.new
        end
        observer.define_singleton_method(:detach) { |_token| log << [:detach] }
        observer.define_singleton_method(:processing_finished) { |outcome| log << [:processing, outcome] }
        observer.define_singleton_method(:finished) { |completion| log << [:finished, completion] }
        observer
      end
    end.new
  end
  let(:bus) { ConnectRPC::Instrumentation.new(subscribers: [subscriber]) }

  def server(**, &handler)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, methods: [rpc])], binding: binding,
                           handlers: { rpc.path => handler || ->(message, _context) { message } },
                           instrumentation: bus, **)
  end

  def dispatch(target, **)
    target.call(path: rpc.path, http_method: "POST", headers: { "content-type" => ["application/proto"] },
                input: StringIO.new("hello"), **)
  end

  it "reports one immutable processing outcome before its unobserved completion" do
    expect(dispatch(server).body).to eq("hello")
    expect(events.map(&:first)).to eq(%i[start attach processing detach finished])
    outcome = events.find { |event| event.first == :processing }.last
    expect(outcome).to have_attributes(code: "OK", http_status: 200, error: nil, request_bytes: 5, response_bytes: 5)
    expect(outcome).to be_frozen
    expect(events.last.last).to have_attributes(delivery: :unobserved, decision: :processed, outcome: outcome)
  end

  it "latches an early abort, finishes processing first and permits callback reentry only once" do
    handle = nil
    # Abort during application execution, before processing cleanup has run.
    target = server do |message, _context|
      handle.finish(delivery: :aborted)
      message
    end
    handle = target.start_call(path: rpc.path, headers: {})
    expect(dispatch(target, lifecycle: handle).status).to eq(200)
    handle.finish(delivery: :completed)
    expect(events.count { |event| event.first == :finished }).to eq(1)
    expect(events.find { |event| event.first == :finished }.last.delivery).to eq(:aborted)
    expect(events.map(&:first).last(2)).to eq(%i[finished detach])
  end
  it "keeps original failures separate from normalized and rendered replacements" do
    [100, 1].each do |limit|
      events.clear
      original = ConnectRPC::Error.new(:permission_denied, "private" * 100)
      response = dispatch(server(response_limit: limit) { raise original })
      outcome = events.find { |event| event.first == :processing }.last
      expect(outcome.error).to equal(original)
      expect(outcome.normalized_error.code).to eq("resource_exhausted")
      expect(outcome.code).to eq(limit == 1 ? nil : "resource_exhausted")
      expect(outcome.http_status).to eq(429)
      expect(outcome.response_bytes).to eq(response.body.bytesize)
      expect(events.count { |event| event.first == :finished }).to eq(1)
    end
  end

  it "records unexpected application failures and never exposes messages in the wire error" do
    original = RuntimeError.new("private data")
    response = dispatch(server { raise original })
    outcome = events.find { |event| event.first == :processing }.last
    expect(outcome.error).to equal(original)
    expect(outcome.normalized_error.code).to eq("internal")
    expect(response.body).not_to include("private data")
  end

  scenarios = [["GET", {}, 405], ["POST", {}, 415],
               ["POST", { "content-type" => ["application/proto"], "connect-timeout-ms" => ["bad"] }, 400]]
  scenarios.each do |verb, headers, status|
    it "observes #{status} without moving validation or inventing a success code" do
      response = server.call(path: rpc.path, http_method: verb, headers: headers, input: nil)
      expect(response.status).to eq(status)
      outcome = events.find { |event| event.first == :processing }.last
      expect(outcome.code).to eq(status == 400 ? "invalid_argument" : nil)
      expect(outcome.request_bytes).to be_nil
    end
  end

  it "resolves unknown methods without giving subscribers the raw name" do
    target = server
    handle = target.start_call(path: "/test.Echo/private", headers: {})
    expect(events.first.last.method).to be_nil
    handle.finish(delivery: :completed, decision: :admission_rejected, http_status: 503)
    completion = events.last.last
    expect(completion).to have_attributes(outcome: nil, processing_state: :not_started, decision: :admission_rejected)
    expect(events.map(&:first)).to eq(%i[start finished])
  end

  it "rejects mismatched and reused handles without another invocation" do
    target = server
    handle = target.start_call(path: rpc.path, headers: {})
    expect { dispatch(server, lifecycle: handle) }.to raise_error(ArgumentError)
    expect(dispatch(target, lifecycle: handle).status).to eq(200)
    expect { dispatch(target, lifecycle: handle) }.to raise_error(ArgumentError)
    handle.finish(delivery: :completed)
  end

  %i[start attach detach processing_finished finished].each do |callback|
    it "isolates #{callback} and reporter failures without replaying work or skipping peers" do
      failing = Object.new
      observer = Object.new
      observer.define_singleton_method(:attach) { nil }
      observer.define_singleton_method(callback) { |*| raise "subscriber failed" }
      failing.define_singleton_method(:start) { |_| callback == :start ? raise("start failed") : observer }
      calls = 0
      configured = ConnectRPC::Instrumentation.new(subscribers: [failing, subscriber], on_error: lambda { |_|
        raise "reporter"
      })
      expect(dispatch(server(instrumentation: configured) do |message, _|
        calls += 1
        message
      end).body).to eq("hello")
      expect(calls).to eq(1)
      expect(events.count { |event| event.first == :finished }).to eq(1)
    end
  end

  it "unwinds successful activations in reverse order and restores nested and fatal execution" do
    active = []
    log = []
    fatal = Class.new(Exception) # rubocop:disable Lint/InheritException -- cancellation is outside StandardError
    observers = 2.times.map do |index|
      observer = Object.new
      observer.define_singleton_method(:attach) do
        active << index
        log << [:attach, index]
        active.size
      end
      observer.define_singleton_method(:detach) do |token|
        raise "Mismatched activation token" unless active.size == token

        active.pop
        log << [:detach, index]
      end
      Object.new.tap { |sub| sub.define_singleton_method(:start) { |_| observer } }
    end
    configured = ConnectRPC::Instrumentation.new(subscribers: observers)
    target = server(instrumentation: configured)
    handle = target.start_call(path: rpc.path, headers: {})
    expect { handle.activate { handle.activate { raise fatal } } }.to raise_error(fatal)
    expect(active).to be_empty
    expect(log.last(4)).to eq([[:detach, 1], [:detach, 0], [:detach, 1], [:detach, 0]])
    handle.finish(delivery: :aborted)
  end

  it "latches reentrant completion callbacks before observer invocation" do
    handle = nil
    callback = Object.new
    callback.define_singleton_method(:finished) { |_| handle.finish(delivery: :aborted) }
    sub = Object.new
    sub.define_singleton_method(:start) { |_| callback }
    target = server(instrumentation: ConnectRPC::Instrumentation.new(subscribers: [sub, subscriber]))
    handle = target.start_call(path: rpc.path, headers: {})
    handle.finish(delivery: :completed, decision: :host_rejected)
    expect(events.count { |event| event.first == :finished }).to eq(1)
  end

  it "starts client observation before invalid timeout and metadata, with no transport work" do
    transport = ->(_) { raise "transport must not run" }
    caller = ConnectRPC::Client.new(base_url: "https://example.test", binding: binding, transport: transport,
                                    instrumentation: bus)
    [{ timeout: 0 }, { metadata: { "content-type" => "reserved" } }].each do |options|
      events.clear
      expect { caller.call(rpc, "hello", **options) }.to raise_error(ArgumentError)
      expect(events.first.first).to eq(:start)
      expect(events.count { |event| event.first == :finished }).to eq(1)
      expect(events.find { |event| event.first == :processing }.last.error).to be_a(ArgumentError)
    end
  end

  it "transactionally rolls back invalid propagation while preserving caller metadata" do
    injector = Object.new
    injector.define_singleton_method(:start) do |call|
      call.carrier["x-original"] = "replacement"
      call.carrier["content-type"] = "forbidden"
      nil
    end
    reports = []
    configured = ConnectRPC::Instrumentation.new(subscribers: [injector, subscriber], on_error: ->(e) { reports << e })
    headers = nil
    transport = lambda do |request|
      headers = request.headers
      ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] }, body: "ok")
    end
    caller = ConnectRPC::Client.new(base_url: "https://example.test", binding: binding, transport: transport,
                                    instrumentation: configured)
    metadata = { "x-original" => ["kept"] }.freeze
    expect(caller.call(rpc, "request", metadata: metadata).message).to eq("ok")
    expect(headers["x-original"]).to eq(["kept"])
    expect(metadata).to eq("x-original" => ["kept"])
    expect(reports.size).to eq(1)
  end

  %i[encode decode].each do |stage|
    it "preserves the original #{stage} failure and final client error" do
      original = ConnectRPC::CodecError.new("private codec data")
      broken = Object.new
      broken.define_singleton_method(:encode) { |value, **| stage == :encode ? raise(original) : value }
      broken.define_singleton_method(:decode) { |value, **| stage == :decode ? raise(original) : value }
      transport = ->(_) { ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] }, body: "reply") }
      caller = ConnectRPC::Client.new(base_url: "https://example.test", binding: broken, transport: transport,
                                      instrumentation: bus)
      expect { caller.call(rpc, "request") }.to raise_error(ConnectRPC::Error)
      outcome = events.find { |event| event.first == :processing }.last
      expect(outcome.error).to equal(original)
      expect(outcome.normalized_error.code).to eq(stage == :encode ? "invalid_argument" : "internal")
    end
  end

  it "keeps disabled calls free of instrumentation objects and instrumentation clock reads" do
    target = server(instrumentation: nil)
    transport = lambda do |request|
      expect(request.lifecycle).to be_nil
      target.call(path: request.url.path, http_method: request.http_method, headers: request.headers,
                  input: StringIO.new(request.body))
    end
    caller = ConnectRPC::Client.new(base_url: "https://example.test", binding: binding, transport: transport)
    expect(ConnectRPC::Instrumentation).not_to receive(:start)
    expect(ConnectRPC::Instrumentation::Call).not_to receive(:new)
    expect(ConnectRPC::Instrumentation::Handle).not_to receive(:new)
    expect(Process).not_to receive(:clock_gettime)
    expect(caller.call(rpc, "request", timeout: nil).message).to eq("request")
  end
  %i[encode decode].each do |stage|
    it "retains the original server #{stage} failure while reporting the rendered protocol result" do
      original = ConnectRPC::CodecError.new("private codec data")
      broken = Object.new
      broken.define_singleton_method(:encode) { |value, **| stage == :encode ? raise(original) : value }
      broken.define_singleton_method(:decode) { |value, **| stage == :decode ? raise(original) : value }
      expect(dispatch(server(binding: broken)).status).to eq(stage == :decode ? 400 : 500)
      outcome = events.find { |event| event.first == :processing }.last
      expect(outcome.error).to equal(original)
      expect(outcome.code).to eq(stage == :decode ? "invalid_argument" : "internal")
      expect(events.count { |event| event.first == :finished }).to eq(1)
      expect(events.first.last).not_to respond_to(:request, :response)
    end
  end

  it "reports the final deadline error when final response validation replaces an earlier codec failure" do
    now = 0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    original = ConnectRPC::CodecError.new("decode failure")
    broken = Object.new
    broken.define_singleton_method(:encode) { |value, **| value }
    broken.define_singleton_method(:decode) do |*, **|
      now = 2
      raise original
    end
    transport = lambda do |_|
      ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                        body: "reply")
    end
    allow(ConnectRPC::Deadline).to receive(:new).and_return(deadline)
    caller = ConnectRPC::Client.new(base_url: "https://example.test", binding: broken, transport: transport,
                                    instrumentation: bus)
    expect { caller.call(rpc, "request") }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
    outcome = events.find { |event| event.first == :processing }.last
    expect(outcome.error).to equal(original)
    expect(outcome.normalized_error.code).to eq("deadline_exceeded")
    expect(outcome.code).to eq("deadline_exceeded")
  end

  it "isolates subscriber mutation of empty failure metadata from later observers" do
    mutator = Object.new
    mutator.define_singleton_method(:start) { |_| self }
    mutator.define_singleton_method(:processing_finished) { |outcome| outcome.response_metadata["x-added"] = ["bad"] }
    reports = []
    configured = ConnectRPC::Instrumentation.new(subscribers: [mutator, subscriber], on_error: ->(e) { reports << e })
    caller = ConnectRPC::Client.new(base_url: "https://example.test", binding: binding, instrumentation: configured)
    expect { caller.call(rpc, "request", timeout: 0) }.to raise_error(ArgumentError)
    outcome = events.find { |event| event.first == :processing }.last
    expect(outcome.response_metadata).to eq({})
    expect(outcome.response_metadata).to be_frozen
    expect(reports).to contain_exactly(an_instance_of(FrozenError))
    expect(events.last.last.outcome).to equal(outcome)
  end

  it "retains the original refused NetHTTP connection before the public unavailable error" do
    reserved = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
    reserved.bind(Socket.sockaddr_in(0, "127.0.0.1"))
    caller = ConnectRPC::Client.new(base_url: "http://127.0.0.1:#{reserved.local_address.ip_port}",
                                    binding: binding, instrumentation: bus)
    expect { caller.call(rpc, "request", timeout: nil) }.to raise_error(ConnectRPC::Error) do |error|
      expect(error.code).to eq("unavailable")
      outcome = events.find { |event| event.first == :processing }.last
      expect(outcome.normalized_error).to equal(error)
      expect(outcome.error).to be_a(Errno::ECONNREFUSED)
    end
  ensure
    reserved&.close
  end

  it "retains malformed request and response gzip before their protocol errors" do
    response = server(encoding: :gzip).call(path: rpc.path, http_method: "POST",
                                            headers: { "content-type" => ["application/proto"],
                                                       "content-encoding" => ["gzip"] }, input: StringIO.new("bad"))
    expect(response.status).to eq(400)
    outcome = events.find { |event| event.first == :processing }.last
    expect(outcome.error).to be_a(ConnectRPC::Compression::Malformed)
    expect(outcome.normalized_error.code).to eq("invalid_argument")
    [200, 503].each do |status|
      events.clear
      transport = lambda do |_|
        ConnectRPC::TransportResponse.new(status: status, headers: { "content-type" => ["application/proto"],
                                                                     "content-encoding" => ["gzip"] }, body: "not gzip")
      end
      caller = ConnectRPC::Client.new(base_url: "https://example.test", binding: binding, transport: transport,
                                      instrumentation: bus, accept_encoding: [:gzip])
      expect { caller.call(rpc, "request", timeout: nil) }.to raise_error(ConnectRPC::Error) do |error|
        expect(error.code).to eq(status == 200 ? "internal" : "unavailable")
        outcome = events.find { |event| event.first == :processing }.last
        expect(outcome.error).to be_a(ConnectRPC::Compression::Malformed)
        expect(outcome.normalized_error).to equal(error)
      end
    end
  end
end
