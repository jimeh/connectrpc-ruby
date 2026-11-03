# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/streaming_bounds"

RSpec.describe "Falcon gRPC wire saturation and cancellation" do
  def stream_methods
    @stream_methods ||= [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod]
                        .map do |type|
      type.new(service: "test.Streams", name: type.name.split("::").last,
               request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
    end
  end

  def streaming_server(handlers, **)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: "test.Streams", methods: stream_methods)],
                           binding: ConnectRPC::Bindings::GoogleProtobuf.new, handlers: handlers, protocol: :grpc, **)
  end

  def build_stream_host(target, tls, scheme)
    bound = ConnectRPC::Falcon::Endpoint.parse("#{scheme}://127.0.0.1:0").bound
    host = ConnectRPC::Falcon::Server.new(server: target, endpoint: bound, tls: tls)
    host.run
    [host, "#{scheme}://127.0.0.1:#{bound.sockets.first.local_address.ip_port}", bound]
  end

  include_examples "native streaming bounds and cancellation", :grpc
end
