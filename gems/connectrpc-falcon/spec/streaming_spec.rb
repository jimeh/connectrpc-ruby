# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/streaming_examples"

require_relative "../../connectrpc-async/spec/support/streaming_bounds"

RSpec.describe "Falcon native Connect streaming" do
  def build_stream_host(target, tls, scheme)
    bound = ConnectRPC::Falcon::Endpoint.parse("#{scheme}://127.0.0.1:0").bound
    host = ConnectRPC::Falcon::Server.new(server: target, endpoint: bound, tls: tls)
    host.run
    [host, "#{scheme}://127.0.0.1:#{bound.sockets.first.local_address.ip_port}", bound]
  end

  include_examples "native streaming matrix"
  include_examples "generated mixed streaming client"
  include_examples "native streaming bounds and cancellation"

  it "balances request metrics for mixed unary and streaming publication through complete close" do
    reactor do
      unary = ConnectRPC::Method.new(service: "test.Streams", name: "Unary", request_type: AsyncHelpers::MESSAGE,
                                     response_type: AsyncHelpers::MESSAGE)
      streaming = stream_methods.first
      target = ConnectRPC::Server.new(
        services: [ConnectRPC::Service.new(name: "test.Streams", methods: [unary, streaming])],
        binding: ConnectRPC::Bindings::GoogleProtobuf.new,
        handlers: { unary.path => ->(message, _) { message },
                    streaming.path => ->(message, writer, _) { writer.send_message(message) } }
      )
      metrics = Async::Utilization::Registry.new
      bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
      host = ConnectRPC::Falcon::Server.new(server: target, endpoint: bound, utilization_registry: metrics)
      host.run
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      message = AsyncHelpers::MESSAGE.new(name: "mixed")
      expect(core.call(unary, message).message).to eq(message)
      core.server_stream(streaming, message) { |reader| reader.each { |reply| expect(reply).to eq(message) } }
      transport.close(grace: 1)
      expect(host.close(grace: 1)).to be(true)
      expect(metrics.metric(:requests_active).value).to eq(0)
      expect(metrics.metric(:connections_active).value).to eq(0)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
      bound&.close
    end
  end
end
