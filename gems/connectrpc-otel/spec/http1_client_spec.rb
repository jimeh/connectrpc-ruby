# frozen_string_literal: true

generated = File.expand_path("../../../spec/fixtures/generation/ruby", __dir__)
$LOAD_PATH.unshift(generated)
require "generation/streaming_connect"
$LOAD_PATH.delete(generated)

RSpec.describe "Explicit HTTP/1 client SDK propagation and task ownership" do
  it "keeps native writer/read tasks in their client span and propagates mTLS unary/SS/CS to Falcon" do
    provider, exporter, bus = tracing
    reactor do |task|
      service = GenerationFixture::Streaming::Mixed
      item = GenerationFixture::Streaming::Item
      release = Async::Queue.new
      host = nil
      observed = []
      handler_context = lambda do
        observed << [current_span.context, host.peer_certificate.serial.to_i]
      end
      implementation = ConnectRPC::Server.new(
        services: [service::SERVICE], binding: ConnectRPC::Bindings::GoogleProtobuf.new, instrumentation: bus,
        handlers: service.handlers(
          lookup: lambda { |message, _|
            handler_context.call
            message
          },
          watch: lambda { |message, writer, _|
            handler_context.call
            writer.send_message(message)
            release.dequeue
          },
          collect: lambda { |reader, _|
            handler_context.call
            message = reader.receive
            expect(reader.receive).to be_nil
            message
          }, chat: ->(*) {}
        )
      )
      falcon(server: implementation, scheme: "https",
             tls: host_tls(anchors: [tls_material.fetch(:client_ca)])) do |active, origin|
        host = active
        with_ca do |ca_file|
          identity = host_tls(tls_material.fetch(:identity)).except(:client_ca)
          transport = ConnectRPC::Async::HTTP1Transport.new(origin: origin, ca_file: ca_file, **identity)
          typed = service::Client.new(client: ConnectRPC::Client.new(base_url: origin, transport: transport,
                                                                     instrumentation: bus,
                                                                     binding: ConnectRPC::Bindings::GoogleProtobuf.new))
          operations = []
          trace = TracePoint.new(:call) do |event|
            if event.self.is_a?(AsyncHelpers::INTERNAL::HTTP1Connection) && %i[write_request
                                                                               read_head].include?(event.method_id)
              operations << [event.method_id, current_span.context.span_id]
            end
            owned = event.method_id == :close && transport.instance_variable_get(:@calls).keys.any? do |operation|
              operation[:input_body].equal?(event.self)
            end
            if owned && event.defined_class == Protocol::HTTP1::Body::Fixed
              operations << [:body_close, current_span.context.span_id]
            end
          end
          trace.enable do
            expect(typed.lookup(item.new(name: "unary")).message.name).to eq("unary")
            typed.watch(item.new(name: "ss")) do |reader|
              expect(reader.receive.name).to eq("ss")
              expect(task.async { typed.lookup(item.new(name: "concurrent")) }.wait.message.name).to eq("concurrent")
              release.enqueue(true)
              expect(reader.receive).to be_nil
            end
            expect(typed.collect { |writer| writer.send_message(item.new(name: "cs")) }.message.name).to eq("cs")
          end
          expect(transport.close).to be(true)
          wait_until(task, "eight real SDK spans") { rpc_spans(exporter).size == 8 }
          clients = rpc_spans(exporter).select { |span| span.kind == :client }
          servers = rpc_spans(exporter).select { |span| span.kind == :server }
          expect(clients.size).to eq(4)
          expect(servers.size).to eq(4)
          expect(operations.map(&:first).tally).to eq(write_request: 4, read_head: 4, body_close: 2)
          expect(operations.map(&:last).uniq.sort).to eq(clients.map(&:span_id).sort)
          expect(servers.map(&:parent_span_id).sort).to eq(clients.map(&:span_id).sort)
          expect(servers.map(&:trace_id).sort).to eq(clients.map(&:trace_id).sort)
          expect(servers.map { |span| span.attributes["connectrpc.delivery"] }).to eq(["completed"] * 4)
          expect(observed.map(&:last)).to eq([201] * 4)
          expect(current_span.context).not_to be_valid
        ensure
          trace&.disable
          transport&.close(grace: 0)
        end
      end
    ensure
      release&.enqueue(true)
      provider.shutdown
    end
  end
end
