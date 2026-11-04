# frozen_string_literal: true

require "rack/mock"

RSpec.describe "SDK spans across lifecycle execution boundaries" do
  it "exports successful inner client and server calls inside an unrelated rescue" do
    _provider, exporter, bus = tracing
    target = server(instrumentation: bus)
    typed = client(ConnectRPC::Testing::Transport.new(server: target), "https://in-process.invalid",
                   instrumentation: bus)
    begin
      raise "ambient failure"
    rescue RuntimeError
      message = AsyncHelpers::MESSAGE.new(name: "success")
      expect(typed.echo(message).message).to eq(message)
    end
    spans = rpc_spans(exporter)
    expect(spans.size).to eq(2)
    spans.each do |span|
      expect(span.attributes["rpc.response.status_code"]).to eq("OK")
      expect(span.attributes).not_to have_key("error.type")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::UNSET)
    end
    expect(current_span.context).not_to be_valid
  end

  %i[connect grpc_web].each do |protocol|
    it "ends exactly one #{protocol} Rack span on throw inside rescue without an ambient error" do
      outcomes = []
      recorder = Object.new
      recorder.define_singleton_method(:start) { |_| recorder }
      recorder.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
      _provider, exporter, bus = tracing(subscribers: [recorder])
      value = Object.new
      target = server(instrumentation: bus, protocol: protocol, echo: ->(*) { throw :escape, value })
      app = ConnectRPC::Rack.new(server: target)
      message = AsyncHelpers::MESSAGE.new(name: "throw").to_proto
      media = protocol == :grpc_web ? "application/grpc-web" : "application/proto"
      message = [0, message.bytesize].pack("CN") + message if protocol == :grpc_web
      env = Rack::MockRequest.env_for(AsyncHelpers::ECHO::ECHO.path, method: "POST", "CONTENT_TYPE" => media,
                                                                     input: message)
      result = begin
        raise "ambient failure"
      rescue RuntimeError
        catch(:escape) { app.call(env) }
      end
      expect(result).to equal(value)
      spans = rpc_spans(exporter)
      expect(spans.size).to eq(1)
      span = spans.first
      expect(span.kind).to eq(:server)
      expect(span.attributes).to include("connectrpc.delivery" => "aborted", "error.type" => "delivery_aborted")
      expect(span.attributes).not_to have_key("rpc.response.status_code")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
      expect(outcomes).to contain_exactly(have_attributes(code: nil, error: nil))
      expect(current_span.context).not_to be_valid
    end
  end
end
