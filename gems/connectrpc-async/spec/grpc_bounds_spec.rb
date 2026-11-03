# frozen_string_literal: true

require_relative "support/streaming_bounds"

RSpec.describe "gRPC native bounds and cancellation" do
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
    host = ConnectRPC::Async::Host.new(server: target, tls: tls).start
    [host, "#{scheme}://127.0.0.1:#{host.port}", nil]
  end

  include_examples "native streaming bounds and cancellation", :grpc
end
