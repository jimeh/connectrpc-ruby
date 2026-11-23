# frozen_string_literal: true

require_relative "support/http1_client_helpers"

RSpec.describe "Client transport origins, admission messages and lifecycle errors" do
  include HTTP1ClientHelpers

  def failure
    yield
    raise "Expected a ConnectRPC error"
  rescue ConnectRPC::Error => e
    e
  end

  it "rescues the public LifecycleError constant" do
    error = begin
      ConnectRPC::Async::Transport.new(origin: "http://127.0.0.1:1")
    rescue ConnectRPC::Async::LifecycleError => e
      e
    end
    expect([error.class.name, error.message])
      .to eq(["ConnectRPC::Async::LifecycleError", "An application-owned Async task is required"])
  end

  it "accepts Transport and HTTP1Transport origins with a bare root path" do
    reactor do
      host = deadline_host(server: server)
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: "#{origin}/")
      http1 = ConnectRPC::Async::HTTP1Transport.new(origin: "#{origin}/")
      [transport, http1].each { |accepted| expect(accepted.inspect).to end_with("origin=#{origin} proxy=environment>") }
      expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "root")).message.name).to eq("root")
      [ConnectRPC::Async::Transport, ConnectRPC::Async::HTTP1Transport].each do |kind|
        expect { kind.new(origin: "#{origin}/prefix") }
          .to raise_error(ArgumentError, "Origin must contain only an HTTP(S) scheme, host and port")
      end
    ensure
      transport&.close(grace: 0)
      http1&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "accepts a ThreadedTransport origin with a bare root path" do
    transport = threaded(origin: "http://127.0.0.1:1/")
    expect(transport.inspect).to include("origin=http://127.0.0.1:1 proxy=environment>")
  ensure
    transport&.close
  end

  it "distinguishes closed from full HTTP/2 admission" do
    reactor do |task|
      release = Async::Promise.new
      entered = Async::Queue.new
      host = deadline_host(server: server(echo: lambda { |message, _|
        entered.enqueue(true)
        release.wait
        message
      }))
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin, max_requests: 1)
      typed = client(transport, origin)
      held = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
      entered.dequeue
      full = failure { typed.echo(AsyncHelpers::MESSAGE.new) }
      stream = failure { transport.open_stream(stream_request(origin)) { nil } }
      release.resolve(nil)
      held.wait
      transport.close
      closed = failure { typed.echo(AsyncHelpers::MESSAGE.new) }
      closed_stream = failure { transport.open_stream(stream_request(origin)) { nil } }
      expect([full, stream, closed, closed_stream].map { |error| [error.code, error.message] }).to eq(
        ([["unavailable", "HTTP/2 transport admission is full"]] * 2) +
        ([["unavailable", "HTTP/2 transport admission is closed"]] * 2)
      )
    ensure
      release&.resolve(nil)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "reports a pending byte budget as full HTTP/2 admission" do
    reactor do
      origin = "http://127.0.0.1:1"
      policy = ConnectRPC::Async::ConnectionPolicy.new(max_pending_bytes: 4)
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy)
      error = failure { transport.call(request("#{origin}/path", body: "x" * 5)) }
      expect([error.code, error.message]).to eq(["unavailable", "HTTP/2 transport admission is full"])
    ensure
      transport&.close(grace: 0)
    end
  end

  it "distinguishes closed from full HTTP/1 admission" do
    release = Async::Promise.new
    response = lambda do |index, _incoming|
      release.wait if index == 1
      http1_wire
    end
    with_http1_peer(response: response, max_requests: 1) do |transport, origin, _accepted, requests, task|
      held = task.async { transport.call(request(origin)) }
      wait_until(task, "the held request at the peer") { requests.size == 1 }
      full = failure { transport.call(request(origin)) }
      release.resolve(nil)
      held.wait
      transport.close
      closed = failure { transport.call(request(origin)) }
      expect([full, closed].map(&:message))
        .to eq(["HTTP/1 transport admission is full", "HTTP/1 transport admission is closed"])
    ensure
      release.resolve(nil)
    end
  end

  it "distinguishes closed from full ThreadedTransport admission" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: server(echo: lambda { |message, _|
      entered << true
      release.pop
      message
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin, max_requests: 1)
    typed = client(transport, origin)
    held = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
    pop_within(entered, "the held call in the handler")
    full = failure { typed.echo(AsyncHelpers::MESSAGE.new) }
    release << true
    expect(value_within(held, "the held call").message.name).to eq("held")
    transport.close
    closed = failure { typed.echo(AsyncHelpers::MESSAGE.new) }
    expect([full, closed].map(&:message))
      .to eq(["HTTP/2 transport admission is full", "HTTP/2 transport admission is closed"])
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end
end
