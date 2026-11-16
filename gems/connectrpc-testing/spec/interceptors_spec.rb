# frozen_string_literal: true

RSpec.describe "Unary interceptors through the testing transport" do
  let(:binding) { ConnectRPC::Bindings::GoogleProtobuf.new }
  let(:service) { Example::V1::EchoService }
  let(:type) { Example::V1::EchoMessage }
  let(:log) { [] }

  def layer(role, &body)
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(role) { |call, &continuation| body.call(call, &continuation) }
    end
  end

  def tracing(role, name)
    layer(role) do |call, &continuation|
      log << "#{name}>"
      if role == :server_unary
        raise ConnectRPC::Error.new(:unauthenticated, "No caller") unless call.context.request_metadata["x-c1"]

        call.context.request_metadata["x-principal"] = name
      else
        call.request_metadata["x-#{name}"] = "sent"
      end
      result = continuation.call
      log << "<#{name}"
      result
    end
  end

  def generated(client_chain, server_chain)
    log = self.log
    echo = lambda do |message, context|
      log << "handler:#{context.request_metadata['x-principal']&.join}"
      message
    end
    server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, interceptors: server_chain,
                                    handlers: service.handlers(echo: echo, fail: echo))
    core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, interceptors: client_chain,
                                  transport: ConnectRPC::Testing::Transport.new(server: server))
    service::Client.new(client: core)
  end

  it "runs both chains in process around a generated client call" do
    client = generated(%w[c1 c2].map { |name| tracing(:client_unary, name) },
                       %w[s1 s2].map { |name| tracing(:server_unary, name) })
    response = client.echo(type.new(name: "hi"))
    expect(response.message.name).to eq("hi")
    expect(response).to be_frozen
    expect(log).to eq(%w[c1> c2> s1> s2> handler:s2 <s2 <s1 <c2 <c1])
  end

  it "rejects on the server and short-circuits on the client without dispatch" do
    rejected = generated([], [tracing(:server_unary, "s1")])
    expect { rejected.echo(type.new(name: "hi")) }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unauthenticated") }
    cached = ConnectRPC::Response.new(message: type.new(name: "cached"), metadata: ConnectRPC::Metadata.new,
                                      trailers: ConnectRPC::Metadata.new)
    short = generated([layer(:client_unary) { |_call, &_continuation| cached }], [tracing(:server_unary, "s1")])
    response = short.echo(type.new(name: "hi"))
    expect(response).to equal(cached)
    expect(response).to be_frozen
    expect(log).to eq(%w[s1>])
  end
end
