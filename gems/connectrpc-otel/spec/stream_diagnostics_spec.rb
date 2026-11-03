# frozen_string_literal: true

require "stringio"

RSpec.describe "Streaming diagnostic privacy" do
  { gzip: [1, "gzip", ConnectRPC::Compression::Malformed], json: [2, nil, JSON::ParserError] }.each do |kind, values|
    it "retains original #{kind} failure for neutral observers without exporting its detail" do
      outcomes = []
      observer = Object.new
      observer.define_singleton_method(:start) { |_| observer }
      observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
      provider, exporter, bus = tracing(subscribers: [observer])
      flag, coding, original_type = values
      payload = "private malformed payload"
      input = StringIO.new([flag, payload.bytesize].pack("CN") + payload)
      fields = { "content-type" => ["application/connect+proto"] }
      fields["connect-content-encoding"] = [coding] if coding
      exchange = Object.new
      exchange.define_singleton_method(:response_head) { |**| ConnectRPC::StreamHead.new(status: 200, headers: fields) }
      exchange.define_singleton_method(:read) { |length, **| input.read(length) }
      exchange.define_singleton_method(:abort) { nil }
      transport = Object.new
      transport.define_singleton_method(:streaming_capabilities) { [:bidi_stream] }
      transport.define_singleton_method(:open_stream) { |_, &work| work.call(exchange) }
      method = ConnectRPC::BidiStreamMethod.new(service: "trace.Diagnostics", name: "Chat", request_type: String,
                                                response_type: String)
      core = ConnectRPC::Client.new(base_url: "https://diagnostic.invalid", transport: transport,
                                    binding: Object.new, accept_encoding: [:gzip], instrumentation: bus)
      expect { core.bidi_stream(method, timeout: nil, &:receive) }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("internal")
        expect(error.cause).to be_nil
        expect(outcomes.first.normalized_error).to equal(error)
      }
      expect(outcomes.size).to eq(1)
      expect(outcomes.first.error).to be_a(original_type)
      spans = rpc_spans(exporter)
      expect(spans.size).to eq(1)
      expect(Array(spans.first.events)).to be_empty
      expect(spans.first.attributes.to_s).not_to include(payload, outcomes.first.error.message)
      expect(spans.first.attributes["rpc.response.status_code"]).to eq("internal")
      expect(current_span.context).not_to be_valid
    ensure
      provider&.shutdown
    end
  end
end
