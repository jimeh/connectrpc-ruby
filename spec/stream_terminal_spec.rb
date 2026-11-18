# frozen_string_literal: true

require "json"
require "stringio"
require "connectrpc/bindings/google_protobuf"

# Head publication and error metadata placement at the server stream terminal,
# shared by the Connect, native gRPC and gRPC-Web entry points.
RSpec.describe "Server stream terminal head and error metadata" do
  let(:binding) do
    Object.new.tap do |codec|
      codec.define_singleton_method(:encode) { |message, **| message.b }
      codec.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end
  let(:outcomes) { [] }
  let(:instrumentation) do
    recorded = outcomes
    observer = Object.new
    observer.define_singleton_method(:start) { |_| observer }
    observer.define_singleton_method(:processing_finished) { |outcome| recorded << outcome }
    ConnectRPC::Instrumentation.new(subscribers: [observer])
  end
  let(:origin) { "https://app.example" }

  def declaration(kind = ConnectRPC::ServerStreamMethod)
    kind.new(service: "terminal.Streams", name: "Run", request_type: :request, response_type: :response)
  end

  def server(protocol, rpc: declaration, **, &handler)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, rpcs: [rpc])],
                           binding: binding, status_codec: ConnectRPC::Bindings::GoogleProtobuf.new,
                           protocol: protocol, instrumentation: instrumentation,
                           handlers: { rpc.path => handler }, **)
  end

  def frame(bytes, flags = 0) = [flags, bytes.bytesize].pack("CN") + bytes

  # Records every head, body write, terminal and abort in order.
  def recorder(events)
    output = Object.new
    output.define_singleton_method(:write) do |bytes, **|
      events << [:write, bytes.b]
      nil
    end
    output.define_singleton_method(:finish) do |**fields|
      events << [:finish, fields]
      nil
    end
    output.define_singleton_method(:abort) do
      events << [:abort]
      nil
    end
    output
  end

  def dispatch(target, protocol, events, rpc: declaration, headers: {}, publish: nil)
    media = { connect: "application/connect+proto", grpc: "application/grpc",
              grpc_web: "application/grpc-web" }.fetch(protocol)
    input = StringIO.new(frame("request"))
    input.define_singleton_method(:read) { |length, **| super(length) }
    arguments = { path: rpc.path, http_method: "POST", headers: { "content-type" => [media] }.merge(headers),
                  input: input, output: recorder(events),
                  publish: publish || ->(head) { events << [:head, head.headers] } }
    case protocol
    when :grpc_web then target.call_grpc_web(**arguments, capabilities: %i[unary server_stream])
    when :grpc then target.call_grpc(**arguments, http2: true)
    else target.call_stream(**arguments, http2: true)
    end
  end

  def heads(events) = events.select { |event| event.first == :head }.map(&:last)

  # Connect writes its end-stream message as a flag-2 envelope among the body
  # writes; gRPC finishes with trailer fields; Web stages a 0x80 body terminal.
  def terminal_fields(protocol, events)
    finish = events.find { |event| event.first == :finish }&.last
    case protocol
    when :connect then connect_end_stream(events)
    when :grpc then finish.fetch(:headers).to_h
    else web_trailers(finish.fetch(:terminal))
    end
  end

  def connect_end_stream(events)
    body = events.select { |event| event.first == :write }.map(&:last).join
    offset = 0
    offset += 5 + body.byteslice(offset + 1, 4).unpack1("N") until body.getbyte(offset).anybits?(2)
    JSON.parse(body.byteslice((offset + 5)..)).fetch("metadata", {})
  end

  def web_trailers(terminal)
    terminal.byteslice(5..).split("\r\n").each_with_object({}) do |line, fields|
      key, value = line.split(": ", 2)
      (fields[key] ||= []) << value
    end
  end

  describe "M-2: a failed head publication" do
    [[:explicit, lambda(&:send_headers)], [:implicit, ->(writer) { writer.send_message("reply") }]]
      .each do |publication, publish|
      it "aborts a gRPC-Web stream on a conflicting CORS head (#{publication}) instead of a headless terminal" do
        target = server(:grpc_web, cors: ConnectRPC::GRPCWebCORS.new(origins: [origin])) do |_, writer, context|
          context.response_metadata["access-control-allow-origin"] = ["*"]
          publish.call(writer)
        end
        events = []
        expect { dispatch(target, :grpc_web, events, headers: { "origin" => [origin] }) }
          .to raise_error(ConnectRPC::Error) { |error|
            expect([error.code, error.message]).to eq(["internal", "Conflicting application CORS policy"])
          }
        expect(events).to eq([[:abort]])
        expect(outcomes.map(&:code)).to eq(["internal"])
      end

      it "aborts a gRPC-Web stream whose head exceeds the field limit (#{publication})" do
        target = server(:grpc_web) do |_, writer, context|
          context.response_metadata["x-large"] = ["x" * 70_000]
          publish.call(writer)
        end
        events = []
        expect { dispatch(target, :grpc_web, events) }.to raise_error(ConnectRPC::Error) { |error|
          expect(error.code).to eq("resource_exhausted")
        }
        expect(events).to eq([[:abort]])
        expect(outcomes.map(&:code)).to eq(["resource_exhausted"])
      end
    end

    %i[connect grpc grpc_web].each do |protocol|
      it "invokes a raising #{protocol} stream host publisher once and finishes no headless terminal" do
        failure = ConnectRPC::Error.new(:unavailable, "host publication failed")
        target = server(protocol) { |_, writer, _| writer.send_message("reply") }
        events = []
        calls = 0
        publish = lambda do |_|
          calls += 1
          raise failure
        end
        expect { dispatch(target, protocol, events, publish: publish) }
          .to(raise_error { |error| expect(error).to equal(failure) })
        expect(calls).to eq(1)
        expect(events.map(&:first) - [:abort]).to be_empty
        expect(outcomes.map(&:code)).to eq(["unavailable"])
      end

      it "writes no #{protocol} message after a handler rescues a failed head publication" do
        failure = ConnectRPC::Error.new(:unavailable, "host publication failed")
        rescued = []
        target = server(protocol) do |_, writer, _|
          begin
            writer.send_headers
          rescue ConnectRPC::Error => e
            rescued << e
          end
          writer.send_message("reply")
        end
        events = []
        calls = 0
        publish = lambda do |_|
          calls += 1
          raise failure
        end
        expect { dispatch(target, protocol, events, publish: publish) }.to raise_error(ConnectRPC::Error) { |error|
          expect([error.code, error.message]).to match(["internal", /headers were not published\z/])
        }
        expect(rescued).to eq([failure])
        expect(calls).to eq(1)
        expect(events.map(&:first) - [:abort]).to be_empty
      end

      it "finishes no #{protocol} terminal when a handler rescues a failed head publication and returns" do
        failure = ConnectRPC::Error.new(:unavailable, "host publication failed")
        target = server(protocol) do |_, writer, _|
          writer.send_headers
        rescue ConnectRPC::Error
          nil
        end
        events = []
        calls = 0
        publish = lambda do |_|
          calls += 1
          raise failure
        end
        name = { connect: "Connect", grpc: "gRPC", grpc_web: "gRPC-Web" }.fetch(protocol)
        expect { dispatch(target, protocol, events, publish: publish) }.to raise_error(ConnectRPC::Error) { |error|
          expect([error.code, error.message]).to eq(["internal", "#{name} headers were not published"])
        }
        expect(calls).to eq(1)
        expect(events.map(&:first) - [:abort]).to be_empty
      end

      # The error path observes processing before publishing the head, so a
      # subscriber can attempt the first publication after the early check.
      it "finishes no #{protocol} terminal when publication first fails inside processing_finished" do
        failure = ConnectRPC::Error.new(:unavailable, "host publication failed")
        handler_error = ConnectRPC::Error.new(:aborted, "handler failed")
        retained = nil
        observer = Object.new
        observer.define_singleton_method(:start) { |_| observer }
        observer.define_singleton_method(:processing_finished) { |_| retained.send_headers }
        isolated = []
        target = server(protocol, instrumentation: ConnectRPC::Instrumentation.new(
          subscribers: [observer], on_error: ->(error) { isolated << error }
        )) do |_, _, context|
          retained = context
          raise handler_error
        end
        events = []
        calls = 0
        publish = lambda do |_|
          calls += 1
          raise failure
        end
        expect { dispatch(target, protocol, events, publish: publish) }
          .to(raise_error { |error| expect(error).to equal(handler_error) })
        expect(isolated).to include(failure)
        expect(calls).to eq(1)
        expect(events.map(&:first) - [:abort]).to be_empty
      end

      it "keeps the #{protocol} publisher failure when even the minimal terminal exceeds its limit" do
        failure = ConnectRPC::Error.new(:unavailable, "host publication failed")
        limits = protocol == :connect ? { terminal_limit: 1 } : { grpc_status_wire_limit: 1 }
        target = server(protocol, **limits) { |_, writer, _| writer.send_message("reply") }
        events = []
        calls = 0
        publish = lambda do |_|
          calls += 1
          raise failure
        end
        expect { dispatch(target, protocol, events, publish: publish) }
          .to(raise_error { |error| expect(error).to equal(failure) })
        expect(calls).to eq(1)
        expect(events.map(&:first) - [:abort]).to be_empty
        expect(outcomes.map(&:code)).to eq(["unavailable"])
      end
    end

    %i[grpc grpc_web].each do |protocol|
      it "does not retry a raising #{protocol} unary host publisher at the terminal" do
        failure = ConnectRPC::Error.new(:unavailable, "host publication failed")
        rpc = declaration(ConnectRPC::Method)
        target = server(protocol, rpc: rpc) { |message, _| message }
        events = []
        calls = 0
        publish = lambda do |_|
          calls += 1
          raise failure
        end
        expect { dispatch(target, protocol, events, rpc: rpc, publish: publish) }
          .to(raise_error { |error| expect(error).to equal(failure) })
        expect(calls).to eq(1)
        expect(events.map(&:first) - [:abort]).to be_empty
      end
    end
  end

  describe "M-3: streaming error metadata" do
    def failure
      ConnectRPC::Error.new(:aborted, "stop", metadata: { "x-err-meta" => ["meta"], "x-order" => ["meta"] },
                                              trailers: { "x-order" => ["error-trailer"] })
    end

    %i[connect grpc grpc_web].each do |protocol|
      it "moves #{protocol} error metadata to the terminal once the head is published" do
        raised = failure
        target = server(protocol) do |_, writer, context|
          context.response_trailers["x-order"] = ["context-trailer"]
          writer.send_message("reply")
          raise raised
        end
        events = []
        expect(dispatch(target, protocol, events)).to be_nil
        expect(heads(events).size).to eq(1)
        expect(heads(events).first).not_to have_key("x-err-meta")
        fields = terminal_fields(protocol, events)
        expect(fields["x-err-meta"]).to eq(["meta"])
        expect(fields["x-order"]).to eq(%w[context-trailer meta error-trailer])
      end

      it "keeps #{protocol} error metadata in an unpublished head without duplicating it" do
        raised = failure
        target = server(protocol) { raise raised }
        events = []
        expect(dispatch(target, protocol, events)).to be_nil
        expect(heads(events).size).to eq(1)
        expect(heads(events).first).to include("x-err-meta" => ["meta"])
        fields = terminal_fields(protocol, events)
        expect(fields).not_to have_key("x-err-meta")
        expect(fields["x-order"]).to eq(["error-trailer"])
      end
    end
  end
end
