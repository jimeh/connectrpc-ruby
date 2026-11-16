# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/stream_interceptor_helpers"

RSpec.describe "Falcon HTTP/1.1 stream interceptors" do
  include StreamInterceptorHelpers

  %i[server_stream client_stream].each do |shape|
    it "runs two client and two server layers and their wrappers in order for Connect #{shape}" do
      reactor do
        client_log = []
        server_log = []
        server_calls = []
        headers = []
        handlers = echo_handlers(server_log)
        handler = handlers.fetch(stream_rpc(shape).path)
        handlers[stream_rpc(shape).path] = lambda do |*args|
          headers << args.last.request_metadata.to_h.slice("x-c1", "x-c2")
          handler.call(*args)
        end
        server_layers = %w[s1 s2].map { |name| tracing_layer(:server_stream, name, server_log, calls: server_calls) }
        client_layers = %w[c1 c2].map { |name| tracing_layer(:client_stream, name, client_log) }
        falcon(server: stream_target(handlers, interceptors: server_layers)) do |_host, origin|
          transport = ConnectRPC::Async::HTTP1Transport.new(origin: origin)
          core = ConnectRPC::Client.new(base_url: origin, transport: transport, interceptors: client_layers,
                                        binding: ConnectRPC::Bindings::GoogleProtobuf.new)
          result = drive_stream(core, shape)
          if shape == :server_stream
            expect(result).to eq(%w[q-1 q-2])
          else
            expect(result.message.name).to eq("a,b")
            expect(result).to be_frozen
          end
        ensure
          transport&.close(grace: 0)
        end
        expect(client_log).to eq(traced(%w[c1 c2], client_operations(shape)))
        expect(server_log).to eq(traced(%w[s1 s2], server_operations(shape), ["handler"]))
        expect(headers).to eq([{ "x-c1" => ["sent"], "x-c2" => ["sent"] }])
        expect(server_calls.uniq.size).to eq(1)
        expect([server_calls.first.protocol, server_calls.first.method.cardinality]).to eq([:connect, shape])
      end
    end
  end
end
