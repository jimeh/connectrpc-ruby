# frozen_string_literal: true

RSpec.describe "HTTP/2 pool ownership" do
  it "multiplexes two observable in-flight calls on one default connection" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        entered.enqueue(message.name)
        release.dequeue
        message
      })).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      calls = %w[first second].map { |name| task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: name)) } }
      expect([entered.dequeue, entered.dequeue]).to match_array(%w[first second])
      expect(transport.instance_variable_get(:@resources).size).to eq(1)
      expect(host_session(host).instance_variable_get(:@resources).size).to eq(1)
      expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(2)
      2.times { release.enqueue(true) }
      expect(calls.map { |call| call.wait.message.name }).to eq(%w[first second])
      expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
    ensure
      2.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "expires pool waits, preserves nil waits and never dispatches a timed-out waiter" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      seen = []
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        seen << message.name
        if message.name == "hold"
          entered.enqueue(true)
          release.dequeue
        end
        message
      }), max_requests: 1).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      held = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "hold"), timeout: nil) }
      entered.dequeue
      expect do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "expired"), timeout: 0.02)
      end.to raise_error(ConnectRPC::Error) { |e|
               expect(e.code).to eq("deadline_exceeded")
             }
      waiting = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "nil"), timeout: nil) }
      wait_until(task, "pool waiter") { transport.instance_variable_get(:@calls).size == 2 }
      expect(waiting).not_to be_finished
      release.enqueue(true)
      expect(held.wait.message.name).to eq("hold")
      expect(waiting.wait.message.name).to eq("nil")
      expect(seen).to eq(%w[hold nil])
      expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "cancels nil pool waiters during forced close and preserves unavailable over cleanup errors" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        entered.enqueue(true)
        release.dequeue
        message
      }),
                                         max_requests: 1).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      first = task.async(finished: false) { typed.echo(AsyncHelpers::MESSAGE.new(name: "hold"), timeout: nil) }
      entered.dequeue
      second = task.async(finished: false) { typed.echo(AsyncHelpers::MESSAGE.new(name: "wait"), timeout: nil) }
      wait_until(task, "pool waiter started") { transport.instance_variable_get(:@calls).size == 2 }
      expect(transport.close(grace: 0)).to be(false)
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(transport.instance_variable_get(:@pool)).not_to be_busy
      [first, second].each do |call|
        expect { call.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
      end
      expect(transport.instance_variable_get(:@pool).resources).to be_empty
      expect(transport.instance_variable_get(:@resources).map(&:raw)).to all(be_closed)
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end

RSpec.describe "HTTP/2 application task cancellation" do
  it "resets an externally cancelled call and preserves its connection for a future call" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        if message.name == "cancel"
          entered.enqueue(true)
          release.dequeue
        end
        message
      })).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      call = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "cancel"), timeout: nil) }
      entered.dequeue
      record = transport.instance_variable_get(:@resources).first
      call.cancel
      call.wait
      expect(call).to be_cancelled
      expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
      expect(record.raw).not_to be_closed
      release.enqueue(true)
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      expect(transport.instance_variable_get(:@resources).size).to eq(1)
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
