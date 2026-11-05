# frozen_string_literal: true

generated = File.expand_path("../../../spec/fixtures/generation/ruby", __dir__)
$LOAD_PATH.unshift(generated)
require "generation/streaming_connect"
$LOAD_PATH.delete(generated)

RSpec.describe "Generated Ruby client against Falcon HTTP/1.1" do
  let(:service) { GenerationFixture::Streaming::Mixed }
  let(:item) { GenerationFixture::Streaming::Item }

  %i[tcp tls mtls].product(%i[proto json], %i[identity gzip]).each do |security, codec, coding|
    it "completes safe unary GET, incremental SS/CS and healthy concurrent calls on #{security}/#{codec}/#{coding}" do
      reactor do |task|
        release = Async::Queue.new
        received = Async::Queue.new
        implementation = ConnectRPC::Server.new(
          services: [service::SERVICE], binding: ConnectRPC::Bindings::GoogleProtobuf.new, encoding: coding,
          handlers: service.handlers(
            lookup: ->(message, _) { message },
            watch: lambda { |message, writer, context|
              context.send_headers(metadata: { "x-before" => "input" })
              writer.send_message(message)
              release.dequeue
              context.response_trailers["x-tail"] = ["done"]
              writer.send_message(message)
            },
            collect: lambda { |reader, _|
              names = []
              reader.each do |message|
                names << message.name
                received.enqueue(names.size)
              end
              item.new(name: names.join(","))
            },
            chat: ->(*) { raise "Bidi reached H1 server" }
          )
        )
        tls = host_tls(anchors: security == :mtls ? [tls_material.fetch(:client_ca)] : nil) if security != :tcp
        falcon(server: implementation, scheme: security == :tcp ? "http" : "https", tls: tls) do |_host, origin|
          with_ca do |ca_file|
            options = security == :tcp ? {} : { ca_file: ca_file }
            options.merge!(host_tls(tls_material.fetch(:identity)).except(:client_ca)) if security == :mtls
            transport = ConnectRPC::Async::HTTP1Transport.new(origin: origin, **options)
            core = ConnectRPC::Client.new(base_url: origin, transport: transport, use_get: true, codec: codec,
                                          encoding: coding, binding: ConnectRPC::Bindings::GoogleProtobuf.new)
            typed = service::Client.new(client: core)
            source = ConnectRPC::CancellationSource.new
            expect(typed.lookup(item.new(name: "safe"), timeout: nil,
                                                        cancellation: source.token).message.name).to eq("safe")
            typed.watch(item.new(name: "stream"), timeout: nil) do |reader|
              expect(reader.headers["x-before"]).to eq(["input"])
              expect(reader.receive.name).to eq("stream")
              healthy = task.async { typed.lookup(item.new(name: "concurrent"), timeout: nil) }
              expect(healthy.wait.message.name).to eq("concurrent")
              release.enqueue(true)
              expect(reader.receive.name).to eq("stream")
              expect(reader.receive).to be_nil
              expect(reader.trailers["x-tail"]).to eq(["done"])
            end
            response = typed.collect(timeout: nil) do |writer|
              writer.send_message(item.new(name: "first"))
              expect(received.dequeue).to eq(1)
              writer.send_message(item.new(name: "last"))
              expect(received.dequeue).to eq(2)
            end
            expect(response.message.name).to eq("first,last")
            expect(typed.lookup(item.new(name: "subsequent")).message.name).to eq("subsequent")
            expect { typed.chat { |_| raise "Bidi yielded" } }.to raise_error(ArgumentError)
            expect(source.token.cancelled?).to be(false)
            expect(transport.close).to be(true)
            expect(transport.instance_variable_get(:@leases)).to be_empty
          ensure
            transport&.close(grace: 0)
          end
        end
      ensure
        release&.enqueue(true)
      end
    end
  end

  %i[tcp tls mtls].each do |security|
    it "recovers an early remote rejection from a reported unfinished upload on #{security}" do
      reactor do
        rejected = Async::Promise.new
        implementation = ConnectRPC::Server.new(
          services: [service::SERVICE], binding: ConnectRPC::Bindings::GoogleProtobuf.new,
          handlers: service.handlers(lookup: ->(message, _) { message }, watch: ->(*) {}, chat: ->(*) {},
                                     collect: lambda { |_reader, _|
                                       rejected.resolve(nil)
                                       raise ConnectRPC::Error.new(:invalid_argument, "early")
                                     })
        )
        tls = host_tls(anchors: security == :mtls ? [tls_material.fetch(:client_ca)] : nil) if security != :tcp
        falcon(server: implementation, scheme: security == :tcp ? "http" : "https", tls: tls) do |_host, origin|
          with_ca do |ca_file|
            options = security == :tcp ? {} : { ca_file: ca_file }
            options.merge!(host_tls(tls_material.fetch(:identity)).except(:client_ca)) if security == :mtls
            transport = ConnectRPC::Async::HTTP1Transport.new(origin: origin, **options)
            core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                          binding: ConnectRPC::Bindings::GoogleProtobuf.new)
            typed = service::Client.new(client: core)
            writes = 0
            expect do
              typed.collect do |writer|
                rejected.wait
                2048.times do
                  writer.send_message(item.new(name: "x" * 16_000))
                  writes += 1
                end
              end
            end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("invalid_argument") }
            expect(writes).to be < 2048
            expect(typed.lookup(item.new(name: "healthy")).message.name).to eq("healthy")
            expect(transport.close).to be(true)
          ensure
            transport&.close(grace: 0)
          end
        end
      end
    end
  end
end
