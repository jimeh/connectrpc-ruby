# frozen_string_literal: true

RSpec.describe "ThreadedTransport two-owner admission" do
  # Spec-only pause between a caller's wake-up and its take of the reply.
  def pause_take(transport)
    paused = Thread::Queue.new
    resume = Thread::Queue.new
    armed = [true]
    transport.singleton_class.prepend(Module.new do
      define_method(:take) do |call|
        if armed.shift
          paused << true
          resume.pop
        end
        super(call)
      end
    end)
    [paused, resume]
  end

  it "keeps a posted reply charged until taken and drops a reply posted after abandonment" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin, max_requests: 1)
    client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "warm"))
    gate = hold_owner(transport)
    engine = engine_of(transport)
    reply = ConnectRPC::TransportResponse.new(status: 200, headers: {}, body: "")
    first = transport.send(:prepare, request("#{origin}/first", body: "1234"))
    transport.send(:admit, first)
    engine.admission.lock.synchronize { engine.settle(first, [:response, reply]) }
    expect([admitted(transport), engine.admission.bytes]).to eq([1, 4])
    replacement = transport.send(:prepare, request("#{origin}/replacement", body: "12"))
    expect { transport.send(:admit, replacement) }.to unavailable_error
    expect(transport.send(:take, first)).to eq([:response, reply])
    expect([admitted(transport), engine.admission.bytes]).to eq([0, 0])
    transport.send(:admit, replacement)
    expect([admitted(transport), engine.admission.bytes]).to eq([1, 2])
    transport.send(:abandon, replacement)
    expect(admitted(transport)).to eq(1)
    engine.admission.lock.synchronize { engine.settle(replacement, [:response, reply]) }
    expect([replacement.result, admitted(transport), engine.admission.bytes]).to eq([nil, 0, 0])
    expect(engine.calls).to be_empty
    gate << true
    expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "holds a paused caller's completed reply within max_requests until it is consumed" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin, max_requests: 1)
    typed = client(transport, origin)
    paused, resume = pause_take(transport)
    held = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
    pop_within(paused, "the caller to pause before take")
    expect(admitted(transport)).to eq(1)
    expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "replacement")) }.to unavailable_error
    resume << true
    expect(value_within(held, "the held caller").message.name).to eq("held")
    expect(admitted(transport)).to eq(0)
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "replacement")).message.name).to eq("replacement")
  ensure
    resume&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "keeps an unconsumed reply charged across owner replacement" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin, max_requests: 1)
    typed = client(transport, origin)
    paused, resume = pause_take(transport)
    held = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
    pop_within(paused, "the caller to pause before take")
    failed = engine_of(transport)
    failed.run_on_owner(-> { raise "injected owner failure" })
    expect(failed.thread.join(5)).to equal(failed.thread)
    expect(failed.state).to eq(:stopped)
    expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "replacement")) }.to unavailable_error
    replacement = engine_of(transport)
    expect(replacement).not_to equal(failed)
    expect(replacement.admission).to equal(failed.admission)
    resume << true
    expect(value_within(held, "the held caller").message.name).to eq("held")
    expect(admitted(transport)).to eq(0)
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "replacement")).message.name).to eq("replacement")
    expect(peer.peer.accepts.size).to eq(2)
  ensure
    resume&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end
end
