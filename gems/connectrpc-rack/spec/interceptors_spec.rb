# frozen_string_literal: true

require_relative "../../../examples/echo_schema"
require_relative "../../../examples/localhost"

RSpec.describe "Unary interceptors over real Rack and NetHTTP" do
  let(:log) { Queue.new }
  let(:handled) { Queue.new }
  let(:outcomes) { Queue.new }
  let(:verbs) { Queue.new }
  let(:message) { EchoExample::Message.new(name: "intercepted", count: 7) }

  def tracing(role, name)
    log = self.log
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(role) do |call, &continuation|
        log << "#{name}>"
        if role == :server_unary
          call.context.request_metadata["x-principal"] = "#{call.protocol}:#{name}" if name == "s1"
          call.context.response_metadata["x-#{name}"] = "before"
        else
          call.request_metadata["x-#{name}"] = "sent"
        end
        result = continuation.call
        call.context.response_trailers["x-#{name}-tail"] = "after" if role == :server_unary
        log << "<#{name}"
        result
      end
    end
  end

  def rejecting
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(:server_unary) do |call, &continuation|
        next continuation.call unless call.context.request_metadata["x-reject"]

        call.context.response_metadata["x-context"] = "set"
        call.context.response_trailers["x-context-tail"] = "set"
        raise ConnectRPC::Error.new(:permission_denied, "Rejected by interceptor",
                                    metadata: { "x-error" => "meta" }, trailers: { "x-error-tail" => "tail" })
      end
    end
  end

  def drain(queue) = Array.new(queue.size) { queue.pop }

  around do |example|
    handled = self.handled
    outcomes = self.outcomes
    verbs = self.verbs
    handler = lambda do |request, context|
      handled << [request.name, context.request_metadata["x-principal"], context.request_metadata["x-c3"]]
      request
    end
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |_call|
      Object.new.tap { |observer| observer.define_singleton_method(:processing_finished) { |done| outcomes << done } }
    end
    server = ConnectRPC::Server.new(
      services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
      handlers: { EchoExample::ECHO.path => handler },
      instrumentation: ConnectRPC::Instrumentation.new(subscribers: [subscriber]),
      interceptors: [rejecting, *%w[s1 s2 s3].map { |name| tracing(:server_unary, name) }]
    )
    app = ConnectRPC::Rack.new(server: server)
    @host = EchoExample::Localhost.new(->(env) { (verbs << env["REQUEST_METHOD"]) && app.call(env) })
    example.run
  ensure
    @host&.close
  end

  def client(use_get)
    ConnectRPC::Client.new(base_url: @host.url, binding: EchoExample::BINDING, use_get: use_get,
                           interceptors: %w[c1 c2 c3].map { |name| tracing(:client_unary, name) })
  end

  { "POST" => false, "GET" => true }.each do |verb, use_get|
    it "composes both chains in order over Connect #{verb} and carries metadata both ways" do
      response = client(use_get).call(EchoExample::ECHO, message, metadata: { "x-caller" => "zero" })
      expect(response.message).to eq(message)
      expect(drain(verbs)).to eq([verb])
      expect(drain(log)).to eq(%w[c1> c2> c3> s1> s2> s3> <s3 <s2 <s1 <c3 <c2 <c1])
      expect(drain(handled)).to eq([["intercepted", ["connect:s1"], ["sent"]]])
      expect(response.metadata.to_h).to include("x-s1" => ["before"], "x-s3" => ["before"])
      expect(response.trailers.to_h).to include("x-s1-tail" => ["after"], "x-s3-tail" => ["after"])
      expect(response).to be_frozen
      outcome = drain(outcomes).first
      expect(outcome.code).to eq("OK")
      expect(outcome.request_metadata).to include("x-caller" => ["zero"], "x-c1" => ["sent"])
      expect(outcome.request_metadata).not_to have_key("x-principal")
    end

    it "sends a server short-circuit rejection with every metadata source over Connect #{verb}" do
      expect { client(use_get).call(EchoExample::ECHO, message, metadata: { "x-reject" => "yes" }) }
        .to raise_error(ConnectRPC::Error) { |error|
          expect(error.code).to eq("permission_denied")
          expect(error.message).to eq("Rejected by interceptor")
          expect(error.metadata.to_h).to include("x-context" => ["set"], "x-error" => ["meta"])
          expect(error.trailers.to_h).to include("x-context-tail" => ["set"], "x-error-tail" => ["tail"])
        }
      expect(drain(handled)).to be_empty
      expect(drain(log)).to eq(%w[c1> c2> c3>])
      expect(drain(outcomes).first.code).to eq("permission_denied")
    end
  end
end
