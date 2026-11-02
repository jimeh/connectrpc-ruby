# frozen_string_literal: true

RSpec.describe "HTTP/2 lifecycle and admission" do
  it "releases completed call admission and closes gracefully without consuming grace" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin, max_requests: 1)
      typed = client(transport, origin)
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "first")).message.name).to eq("first")
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "second")).message.name).to eq("second")
      expect(transport.close(grace: 0.5)).to be(true)
      expect(transport.close(grace: 0)).to be(true)
      expect(host.close(grace: 0.5)).to be(true)
      expect { host.start }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end

RSpec.describe "HTTP/2 ownership and setup bounds" do
  it "rejects construction outside Async and cross-reactor use without changing resources" do
    expect { ConnectRPC::Async::Host.new(server: server) }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
    expect { ConnectRPC::Async::Transport.new(origin: "http://localhost:1") }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
    reactor do
      host = ConnectRPC::Async::Host.new(server: server).start
      failure = Thread.new do
        host.close
      rescue StandardError => e
        e
      end.value
      expect(failure).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
      expect(host.port).to be_positive
      expect(host.peer_certificate).to be_nil
    ensure
      host&.close
    end
  end

  it "rejects cross-reactor start without closing its running listener" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server).start
      listener = host.instance_variable_get(:@bound).sockets.first
      failure = Thread.new do
        host.start
      rescue StandardError => e
        e
      end.value
      expect(listener).not_to be_closed
      expect(failure).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
      expect(host.start).to equal(host)
    ensure
      # The regression's old cleanup path can leave a half-aborted lifetime.
      # Dispose that fixture explicitly so a failed assertion cannot hang RSpec.
      if host&.instance_variable_get(:@aborting)
        host_session(host).instance_variable_get(:@lifetime).cancel
      else
        host&.close(grace: 0)
      end
    end
  end

  it "binds port zero synchronously and cleans a failed bind without closing another listener" do
    reactor do
      first = ConnectRPC::Async::Host.new(server: server).start
      second = ConnectRPC::Async::Host.new(server: server, port: first.port)
      expect { second.start }.to raise_error(Errno::EADDRINUSE)
      expect(second.close).to be(false)
      raw = IO::Endpoint.tcp("127.0.0.1", first.port).connect
      expect(raw).not_to be_closed
      expect(first.addresses.first.ip_port).to eq(first.port)
    ensure
      AsyncHelpers::INTERNAL.raw_close(raw)
      first&.close(grace: 0)
    end
  end

  %i[h2c tls].each do |mode|
    it "bounds silent #{mode} setup and closes excess connections before handshake" do
      reactor do |task|
        host = ConnectRPC::Async::Host.new(server: server, tls: mode == :tls ? host_tls : nil,
                                           max_connections: 1, handshake_timeout: 0.03).start
        raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
        wait_until(task, "first socket accepted") { host_session(host).instance_variable_get(:@resources).size == 1 }
        record = host_session(host).instance_variable_get(:@resources).first
        excess = IO::Endpoint.tcp("127.0.0.1", host.port).connect
        expect(excess.read(1)).to be_nil
        expect(raw.read(1)).to be_nil
        expect(record.raw).to be_closed
        expect(record.task).to be_finished
        expect(record.tasks).to all(be_finished)
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw)
        AsyncHelpers::INTERNAL.raw_close(excess)
        host&.close(grace: 0)
      end
    end
  end

  [nil, 2].each do |rpc_timeout|
    it "bounds client preface setup with RPC timeout #{rpc_timeout.inspect} without blocking the reactor or retrying" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        listener = bound.sockets.first
        accepted = Async::Queue.new
        eof = Async::Queue.new
        peer = task.async do
          raw, = listener.accept
          raw.readpartial(4096)
          accepted.enqueue(true)
          begin
            loop { raw.readpartial(4096) }
          rescue EOFError
            eof.enqueue(true)
          end
        ensure
          AsyncHelpers::INTERNAL.raw_close(raw)
        end
        origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, connect_timeout: 0.03)
        started = AsyncHelpers::INTERNAL.now
        call = task.async(finished: false) do
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new, timeout: rpc_timeout)
        end
        accepted.dequeue
        # This independent reactor timer must run well before the RPC deadline.
        # Its elapsed wait is the external timing behavior under test.
        heartbeat = task.async do |timer|
          timer.with_timeout(0.06) { Async::Condition.new.wait }
        rescue Async::TimeoutError
          AsyncHelpers::INTERNAL.now - started
        end
        expect { call.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
        expect(AsyncHelpers::INTERNAL.now - started).to be < 0.5
        expect(heartbeat.wait).to be < 0.5
        expect(eof.dequeue).to be(true)
        record = transport.instance_variable_get(:@resources).first
        expect(record.raw).to be_closed
        expect(record.task).to be_finished
        expect(listener.accept_nonblock(exception: false)).to eq(:wait_readable)
        peer.wait
      ensure
        transport&.close(grace: 0)
        bound&.close
      end
    end
  end

  it "fails transport and host admission fast while admitted handlers remain independent" do
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
      transport = ConnectRPC::Async::Transport.new(origin: origin, max_requests: 1)
      first = task.async { client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil) }
      entered.dequeue
      expect { client(transport, origin).echo(AsyncHelpers::MESSAGE.new) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("unavailable")
      }
      other = ConnectRPC::Async::Transport.new(origin: origin)
      response = other.call(request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}",
                                    headers: { "content-type" => ["application/proto"] }))
      expect(response.status).to eq(503)
      release.enqueue(true)
      expect(first.wait.message.name).to eq("held")
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      other&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "keeps active calls alive past idle expiry and joins overlapping graceful closes" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        entered.enqueue(true)
        release.dequeue
        message
      }),
                                         idle_timeout: 0.02).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      call = task.async { client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil) }
      entered.dequeue
      record = host_session(host).instance_variable_get(:@resources).first
      # Elapsed time is the behavior here: the idle network timer must expire
      # while this already-observed active handler is still blocked.
      task.with_timeout(0.04) { Async::Condition.new.wait }
    rescue Async::TimeoutError
      expect(record.raw).not_to be_closed
      first_close = task.async { host.close(grace: 1) }
      wait_until(task, "closing host") { host_session(host).instance_variable_get(:@state) == :closing }
      second_close = task.async { host.close(grace: 0) }
      release.enqueue(true)
      expect(call.wait.message.name).to eq("held")
      expect(first_close.wait).to be(true)
      expect(second_close.wait).to be(true)
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "aborts resources when their owner is cancelled and leaves unrelated application tasks alive" do
    reactor do |task|
      ready = Async::Queue.new
      blocker = Async::Queue.new
      unrelated = task.async { blocker.dequeue }
      owner = task.async do
        host = ConnectRPC::Async::Host.new(server: server).start
        transport = ConnectRPC::Async::Transport.new(origin: "http://127.0.0.1:#{host.port}")
        client(transport, "http://127.0.0.1:#{host.port}").echo(AsyncHelpers::MESSAGE.new)
        ready.enqueue([host, transport])
        Async::Condition.new.wait
      end
      host, transport = ready.dequeue
      records = host_session(host).instance_variable_get(:@resources) + transport.instance_variable_get(:@resources)
      owner.cancel
      owner.wait
      wait_until(task, "owner resources closed") do
        records.all? do |record|
          record.raw.closed? && record.task.finished?
        end
      end
      expect(records.map(&:raw)).to all(be_closed)
      expect(records.map(&:task)).to all(be_finished)
      expect(unrelated).not_to be_finished
      expect(host.close).to be(false)
      expect(transport.close).to be(false)
      blocker.enqueue(true)
      expect(unrelated.wait).to be(true)
    end
  end
end

RSpec.describe "HTTP/2 idle expiry" do
  it "expires an empty established connection and allows a separate future RPC to reconnect" do
    reactor do |task|
      host = ConnectRPC::Async::Host.new(server: server, idle_timeout: 0.02).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
      host_record = host_session(host).instance_variable_get(:@resources).first
      client_record = transport.instance_variable_get(:@resources).first
      wait_until(task, "idle connection EOF") { host_record.raw.closed? && client_record.raw.closed? }
      expect(host_record.task).to be_finished
      expect(client_record.connection.reader_task).to be_finished
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "future")).message.name).to eq("future")
      expect(transport.instance_variable_get(:@resources).first).not_to equal(client_record)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
