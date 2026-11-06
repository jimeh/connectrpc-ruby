# frozen_string_literal: true

require_relative "support/http1_client_helpers"

RSpec.describe ConnectRPC::Async::HTTP1Transport, "cancellation and shutdown" do
  include HTTP1ClientHelpers

  def canceled_call(source, code: "canceled")
    yield
    raise "Expected #{code}"
  rescue ConnectRPC::Error => e
    expect(e.code).to eq(code)
    expect(e.cause).to be_nil
  ensure
    source&.cancel
  end

  it "reserves parallel TLS setup slots atomically and admits healthy work while one handshake is stalled" do
    blocked = Async::Promise.new
    release = Async::Promise.new
    before_handshake = lambda do |_raw, index|
      next unless index == 1

      blocked.resolve(nil)
      release.wait
    end
    peer = lambda do |native, socket, *_|
      loop do
        incoming = native.read_request
        break unless incoming

        incoming.last&.each { |_| nil }
        socket.write(http1_wire)
        native.send_end_stream!
      end
    end
    with_raw_http1(peer: peer, tls: true, before_handshake: before_handshake,
                   connection_limit: 2) do |transport, origin, accepted, task|
      source = ConnectRPC::CancellationSource.new
      stalled = task.async do
        canceled_call(source) do
          transport.open_stream(stream_request(origin, cancellation: source.token)) do
            raise "Canceled setup handed off"
          end
        end
      end
      blocked.wait
      2.times { expect(transport.call(request(origin)).body).to eq("ok") }
      expect(accepted.size).to eq(2)
      source.cancel
      stalled.wait
      release.resolve(nil)
      expect(transport.call(request(origin)).body).to eq("ok")
      expect(accepted.size).to eq(2)
      expect(transport.instance_variable_get(:@leases).size).to eq(1)
    ensure
      release.resolve(nil)
    end
  end

  it "never exceeds the hard cap with more concurrent stalled TLS handshakes than slots" do
    stalled = Async::Queue.new
    release = Async::Promise.new
    hook = lambda { |_raw, index|
      stalled.enqueue(index)
      release.wait
    }
    peer = ->(*_) { raise "Canceled TLS setup was dispatched" }
    with_raw_http1(peer: peer, tls: true, before_handshake: hook,
                   connection_limit: 2) do |transport, origin, accepted, task|
      sources = Array.new(4) { ConnectRPC::CancellationSource.new }
      calls = sources.map do |source|
        task.async do
          canceled_call(source) do
            transport.open_stream(stream_request(origin, cancellation: source.token)) do
              nil
            end
          end
        end
      end
      expect(2.times.map { stalled.dequeue }).to eq([1, 2])
      wait_until(task, "four admissions") { transport.instance_variable_get(:@calls).size == 4 }
      expect(accepted.size).to eq(2)
      expect(transport.instance_variable_get(:@leases).size).to eq(2)
      sources.each(&:cancel)
      calls.each(&:wait)
      expect(transport.instance_variable_get(:@leases)).to be_empty
      expect(transport.instance_variable_get(:@resources).all? { |r| r.raw.nil? || r.raw.closed? }).to be(true)
    ensure
      release.resolve(nil)
    end
  end

  it "preserves a healthy TLS lease when a sibling's blocked SSL upload is canceled" do
    entered = Async::Promise.new
    release = Async::Promise.new
    peer = lambda do |native, socket, _raw, index|
      if index == 1
        native.read_request
        entered.resolve(nil)
        release.wait
      else
        loop do
          incoming = native.read_request
          break unless incoming

          incoming.last&.each { |_| nil }
          socket.write(http1_wire)
          native.send_end_stream!
        end
      end
    end
    with_raw_http1(peer: peer, tls: true, connection_limit: 2) do |transport, origin, accepted, task|
      source = ConnectRPC::CancellationSource.new
      upload = task.async do
        canceled_call(source) do
          transport.open_stream(stream_request(origin, cancellation: source.token)) do |exchange|
            loop { exchange.write("x" * 16_384) }
          end
        end
      end
      entered.wait
      expect(transport.call(request(origin)).body).to eq("ok")
      source.cancel
      upload.wait
      expect(transport.call(request(origin)).body).to eq("ok")
      expect(accepted.size).to eq(2)
    ensure
      release.resolve(nil)
    end
  end

  %i[headers upload read pool_wait].each do |stage|
    [false, true].each do |deadline|
      trigger = deadline ? "the original deadline" : "foreign-thread cancellation"
      it "interrupts #{stage} on #{trigger} with all owned tasks joined" do
        entered = Async::Promise.new
        release = Async::Promise.new
        peer = lambda do |native, socket, *_|
          native.read_request
          socket.write("HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n") if stage == :read
          entered.resolve(nil)
          release.wait
        end
        with_raw_http1(peer: peer, connection_limit: 1) do |transport, origin, accepted, task|
          source = ConnectRPC::CancellationSource.new
          expires = ConnectRPC::Deadline.new(0.1) if deadline
          holder = if stage == :pool_wait
                     task.async { transport.open_stream(stream_request(origin)) { release.wait } }.tap { entered.wait }
                   end
          call = task.async do
            canceled_call(source, code: deadline ? "deadline_exceeded" : "canceled") do
              transport.open_stream(stream_request(origin, cancellation: source.token, deadline: expires)) do |exchange|
                case stage
                when :upload
                  loop { exchange.write("x" * 16_384) }
                when :read
                  exchange.close_send
                  exchange.read(1)
                else
                  exchange.response_head
                end
              end
            end
          end
          entered.wait
          wait_until(task, "operation admitted") { transport.instance_variable_get(:@calls).size == (holder ? 2 : 1) }
          Thread.new { source.cancel }.join unless deadline
          call.wait
          expect(accepted.size).to eq(1)
          release.resolve(nil)
          holder&.wait
          expect(transport.instance_variable_get(:@calls)).to be_empty
          expect(transport.instance_variable_get(:@leases)).to be_empty
          expect(transport.instance_variable_get(:@lifetime).children.to_a.all?(&:finished?)).to be(true)
        ensure
          release.resolve(nil)
        end
      end
    end
  end

  it "shares concurrent forced-close outcomes and stops active leases and queued operations" do
    entered = Async::Promise.new
    release = Async::Promise.new
    peer = lambda { |native, *_|
      native.read_request
      entered.resolve(nil)
      release.wait
    }
    with_raw_http1(peer: peer, connection_limit: 1) do |transport, origin, _accepted, task|
      active = task.async do
        expect { transport.open_stream(stream_request(origin), &:response_head) }
          .to raise_error(ConnectRPC::Error)
      end
      entered.wait
      queued = task.async { expect { transport.call(request(origin)) }.to raise_error(ConnectRPC::Error) }
      closers = 2.times.map { task.async { transport.close(grace: 0) } }
      expect(closers.map(&:wait)).to eq([false, false])
      [active, queued].each(&:wait)
      expect(transport.close).to be(false)
      expect(transport.instance_variable_get(:@leases)).to be_empty
      expect { transport.call(request(origin)) }.to raise_error(ConnectRPC::Error)
    ensure
      release.resolve(nil)
    end
  end

  it "waits for both an active lease and an admitted waiter before reporting graceful close" do
    release = Async::Promise.new
    with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, accepted, _requests, task|
      first = task.async do
        transport.open_stream(stream_request(origin)) do |exchange|
          exchange.close_send
          expect(exchange.read(2)).to eq("ok")
          expect(exchange.read(1)).to be_nil
          release.wait
          exchange.finish
        end
      end
      wait_until(task, "active response EOF") do
        transport.instance_variable_get(:@calls).keys.first&.fetch(:eof, false)
      end
      queued = task.async { transport.call(request(origin)) }
      wait_until(task, "admitted waiter") { transport.instance_variable_get(:@calls).size == 2 }
      closer = task.async { transport.close(grace: 2) }
      wait_until(task, "close stopped admission") { transport.instance_variable_get(:@state) == :closing }
      expect(closer).not_to be_finished
      release.resolve(nil)
      first.wait
      expect(queued.wait.body).to eq("ok")
      expect(closer.wait).to be(true)
      expect(accepted.size).to eq(1)
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(transport.instance_variable_get(:@leases)).to be_empty
    ensure
      release.resolve(nil)
    end
  end

  it "enforces construction and operation reactor/thread affinity" do
    expect { described_class.new(origin: "http://localhost") }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
    reactor do
      transport = described_class.new(origin: "http://localhost")
      failure = Thread.new do
        transport.call(request("http://localhost"))
      rescue StandardError => e
        e
      end.value
      expect(failure).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
    ensure
      transport&.close
    end
  end

  it "does not let a completed call's late foreign-thread cancellation close a newer lease" do
    with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, accepted|
      source = ConnectRPC::CancellationSource.new
      first = request(origin)
      first.cancellation = source.token
      expect(transport.call(first).body).to eq("ok")
      transport.open_stream(stream_request(origin)) do |exchange|
        Thread.new { source.cancel }.join
        exchange.close_send
        expect(exchange.read(2)).to eq("ok")
        expect(exchange.read(1)).to be_nil
        exchange.finish
      end
      expect(accepted.size).to eq(1)
    end
  end

  it "fails a stale peer after dispatch exactly once and permits a new independent call" do
    received = []
    peer = lambda do |native, socket, _raw, index|
      incoming = native.read_request
      received << incoming[2]
      next if index == 1

      incoming.last&.each { |_| nil }
      socket.write(http1_wire)
      native.send_end_stream!
    end
    with_raw_http1(peer: peer) do |transport, origin, accepted|
      expect { transport.call(request("#{origin}/first")) }.to raise_error(ConnectRPC::Error)
      expect(accepted.size).to eq(1)
      expect(received).to eq(["/first"])
      expect(transport.call(request("#{origin}/second")).body).to eq("ok")
      expect(received).to eq(%w[/first /second])
    end
  end

  it "never replays a request when a reused keepalive peer closes after its next head" do
    received = []
    peer = lambda do |native, socket, _raw, index|
      loop do
        incoming = native.read_request
        break unless incoming

        received << incoming[2]
        break if index == 1 && incoming[2] == "/late-close"

        incoming.last&.each { |_| nil }
        socket.write(http1_wire)
        native.send_end_stream!
      end
    end
    with_raw_http1(peer: peer) do |transport, origin, accepted|
      expect(transport.call(request("#{origin}/first")).body).to eq("ok")
      expect { transport.call(request("#{origin}/late-close")) }.to raise_error(ConnectRPC::Error)
      expect(accepted.size).to eq(1)
      expect(received).to eq(%w[/first /late-close])
      expect(transport.call(request("#{origin}/independent")).body).to eq("ok")
      expect(accepted.size).to eq(2)
      expect(received).to eq(%w[/first /late-close /independent])
    end
  end

  it "retires an idle keepalive FIN before dispatching bytes on a fresh connection" do
    release = Async::Promise.new
    closed = Async::Promise.new
    received = []
    peer = lambda do |native, socket, _raw, index|
      incoming = native.read_request
      received << incoming[2]
      incoming.last&.each { |_| nil }
      socket.write(http1_wire)
      native.send_end_stream!
      if index == 1
        release.wait
        socket.close
        closed.resolve(nil)
      end
    end
    with_raw_http1(peer: peer) do |transport, origin, accepted|
      expect(transport.call(request("#{origin}/first")).body).to eq("ok")
      expect(transport.instance_variable_get(:@idle).size).to eq(1)
      release.resolve(nil)
      closed.wait
      expect(transport.call(request("#{origin}/fresh")).body).to eq("ok")
      expect(accepted.size).to eq(2)
      expect(received).to eq(%w[/first /fresh])
    ensure
      release.resolve(nil)
    end
  end

  [false, true].each do |deadline|
    it "aborts stalled TLS setup on #{deadline ? 'RPC expiry' : 'connect timeout'} without handing off its slot" do
      entered = Async::Promise.new
      release = Async::Promise.new
      hook = lambda { |*|
        entered.resolve(nil)
        release.wait
      }
      with_raw_http1(peer: ->(*) { raise "Setup handed off" }, tls: true, before_handshake: hook,
                     connect_timeout: deadline ? 1 : 0.05) do |transport, origin, accepted|
        expect do
          transport.call(request(origin, deadline: deadline ? ConnectRPC::Deadline.new(0.05) : nil))
        end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq(deadline ? "deadline_exceeded" : "unavailable") }
        expect(entered.wait).to be_nil
        expect(accepted.size).to eq(1)
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect(transport.instance_variable_get(:@resources).first.raw).to be_closed
      ensure
        release.resolve(nil)
      end
    end
  end

  it "aborts and joins owned network work when its application owner exits" do
    entered = Async::Promise.new
    release = Async::Promise.new
    peer = lambda { |native, *_|
      native.read_request
      entered.resolve(nil)
      release.wait
    }
    with_raw_http1(peer: peer) do |_unused, origin, _accepted, task|
      owned = nil
      owner = task.async do
        owned = described_class.new(origin: origin)
        owned.open_stream(stream_request(origin), &:response_head)
      end
      entered.wait
      owner.cancel
      AsyncHelpers::INTERNAL.join(owner)
      expect(owned.close).to be(false)
      expect(owned.instance_variable_get(:@calls)).to be_empty
      expect(owned.instance_variable_get(:@leases)).to be_empty
      expect(owned.instance_variable_get(:@resources).all? { |r| r.raw.closed? }).to be(true)
    ensure
      release.resolve(nil)
    end
  end
end
