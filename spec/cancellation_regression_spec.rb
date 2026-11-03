# frozen_string_literal: true

require "stringio"

RSpec.describe "Cancellation at exceptional processing boundaries" do
  let(:rpc) do
    ConnectRPC::Method.new(service: "example.Boundaries", name: "Call", request_type: String,
                           response_type: String, idempotency: :no_side_effects)
  end
  let(:binding) do
    Class.new do
      def encode(message, **) = message
      def decode(bytes, **) = bytes
    end.new
  end
  let(:source) { ConnectRPC::CancellationSource.new }
  let(:outcomes) { [] }
  let(:instrumentation) do
    collected = outcomes
    observer = Object.new
    observer.define_singleton_method(:processing_finished) { |outcome| collected << outcome }
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end

  def server(**)
    service = ConnectRPC::Service.new(name: rpc.service, methods: [rpc])
    ConnectRPC::Server.new(services: [service], binding: binding, instrumentation: instrumentation,
                           handlers: { rpc.path => ->(message, _) { message } }, **)
  end

  %i[post_codec get_codec get_body verb].each do |phase|
    it "honors cancellation accepted before the bare #{phase} response" do
      target = server
      query_parser = ConnectRPC.const_get(:GetQuery).singleton_class
      reached = false
      trace = TracePoint.new(:return, :c_return) do |event|
        boundary = case phase
                   when :post_codec
                     event.defined_class == ConnectRPC::Protocol.singleton_class && event.method_id == :content_codec
                   when :get_codec
                     event.defined_class == query_parser && event.method_id == :parse
                   when :get_body
                     event.self.equal?(target) && event.method_id == :empty_get_body?
                   when :verb
                     event.self.equal?(rpc) && event.method_id == :idempotency
                   end
        next unless boundary && !reached

        reached = true
        source.cancel
      end
      verb = { post_codec: "POST", get_codec: "GET", get_body: "GET", verb: "DELETE" }.fetch(phase)
      response = trace.enable do
        target.call(path: rpc.path, http_method: verb,
                    headers: { "content-type" => ["application/unsupported"] },
                    input: phase == :get_body ? StringIO.new("unexpected") : nil,
                    query: "encoding=#{phase == :get_codec ? 'unsupported' : 'json'}&message=hello",
                    cancellation: source.token)
      end
      expect(reached).to be(true)
      expect(response.status).to eq(499)
      expect(JSON.parse(response.body)["code"]).to eq("canceled")
      expect(outcomes.size).to eq(1)
      expect(outcomes.first.code).to eq("canceled")
    ensure
      trace&.disable
    end
  end

  %i[client server].each do |role|
    it "retains the original #{role} gzip failure before final cancellation normalization" do
      native_failure = nil
      trace = TracePoint.new(:raise) do |event|
        next if native_failure || !event.raised_exception.is_a?(Zlib::DataError)

        native_failure = event.raised_exception
        source.cancel
      end
      result = trace.enable do
        if role == :client
          reply = ConnectRPC::TransportResponse.new(status: 200, body: "invalid gzip",
                                                    headers: { "content-type" => ["application/proto"],
                                                               "content-encoding" => ["gzip"] })
          client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding,
                                          transport: ->(_) { reply }, accept_encoding: [:gzip],
                                          instrumentation: instrumentation)
          begin
            client.call(rpc, "request", timeout: nil, cancellation: source.token)
          rescue ConnectRPC::Error => e
            e
          end
        else
          server(encoding: :gzip).call(path: rpc.path, http_method: "POST", input: StringIO.new("invalid gzip"),
                                       headers: { "content-type" => ["application/proto"],
                                                  "content-encoding" => ["gzip"] }, cancellation: source.token)
        end
      end
      expect(native_failure).to be_a(Zlib::DataError)
      expect(outcomes.size).to eq(1)
      expect(outcomes.first.error).to equal(native_failure)
      expect([outcomes.first.code, outcomes.first.normalized_error.cause]).to eq(["canceled", nil])
      if role == :client
        expect([result.code, result.cause]).to eq(["canceled", nil])
      else
        expect(JSON.parse(result.body)["code"]).to eq("canceled")
      end
    ensure
      trace&.disable
    end
  end
end

RSpec.describe "Compression failure reporting" do
  it "isolates a failed internal reporter while preserving the original compression outcome" do
    failures = []
    source = ConnectRPC::CancellationSource.new
    reporter = lambda do |error|
      failures << error
      raise "reporter failed"
    end
    trace = TracePoint.new(:raise) do |event|
      source.cancel if event.raised_exception.is_a?(Zlib::DataError)
    end
    expect do
      trace.enable do
        ConnectRPC::Compression.decode("invalid gzip", limit: 100, deadline: source.token, on_error: reporter)
      end
    end.to raise_error(ConnectRPC::Error) { |error| expect([error.code, error.cause]).to eq(["canceled", nil]) }
    expect(failures.size).to eq(1)
    expect(failures.first).to be_a(Zlib::DataError)
  ensure
    trace&.disable
  end
end
