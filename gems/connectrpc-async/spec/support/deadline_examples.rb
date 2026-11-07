# frozen_string_literal: true

RSpec.shared_examples "HTTP/2 deadlines and stream isolation" do
  it "retains core default/nil and huge logical deadlines without overflowing scheduler timers" do
    reactor do
      observed = []
      host = deadline_host(server: server(echo: lambda { |message, context|
        observed << context.deadline&.remaining
        message
      }))
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      typed.echo(AsyncHelpers::MESSAGE.new)
      typed.echo(AsyncHelpers::MESSAGE.new, timeout: nil)
      typed.echo(AsyncHelpers::MESSAGE.new, timeout: 1e30)
      expect(observed[0]).to be_between(29, 30)
      expect(observed[1]).to be_nil
      expect(observed[2]).to be > 9_000_000
    ensure
      transport&.close
      host&.close
    end
  end

  it "expires a guard-queued call without late dispatch or killing the guard holder" do
    reactor do |task|
      observed = []
      host = deadline_host(server: server(echo: lambda { |message, _|
        observed << message.name
        message
      }))
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
      connection = transport.instance_variable_get(:@resources).first.connection
      locked = Async::Queue.new
      release = Async::Queue.new
      holder = task.async do
        connection.synchronize do
          locked.enqueue(true)
          release.dequeue
        end
      end
      locked.dequeue
      started = AsyncHelpers::INTERNAL.now
      expect do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "expired"), timeout: 0.02)
      end.to raise_error(ConnectRPC::Error) { |e|
               expect(e.code).to eq("deadline_exceeded")
             }
      expect(AsyncHelpers::INTERNAL.now - started).to be < 0.15
      expect(holder).not_to be_finished
      release.enqueue(true)
      holder.wait
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      expect(observed).to eq(%w[warm after])
      expect(transport.instance_variable_get(:@resources).size).to eq(1)
      expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "recomputes the timeout after guard and pool waits rather than replaying the encoded value" do
    reactor do |task|
      observed = []
      host = deadline_host(server: server(echo: lambda { |message, context|
        observed << context.deadline.remaining
        message
      }))
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      typed.echo(AsyncHelpers::MESSAGE.new)
      connection = transport.instance_variable_get(:@resources).first.connection
      # Controlled logical time avoids wall-clock scheduling as proof of budget.
      clock = 0.0
      deadline = ConnectRPC::Deadline.new(2, clock: -> { clock })
      locked = Async::Queue.new
      release = Async::Queue.new
      holder = task.async do
        connection.synchronize do
          locked.enqueue(true)
          release.dequeue
        end
      end
      locked.dequeue
      headers = { "content-type" => ["application/proto"], "connect-timeout-ms" => ["2000"] }
      outgoing = request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}", deadline: deadline, headers: headers)
      call = task.async { transport.call(outgoing) }
      wait_until(task, "writer queued") { connection.writers.any? }
      clock = 1.5
      release.enqueue(true)
      holder.wait
      expect(call.wait.status).to eq(200)
      expect(observed.last).to be_between(0.4, 0.5)
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "keeps a real inflight sibling alive when a cancelled partial frame completes within cleanup" do
    reactor do |task|
      entered = Async::Queue.new
      release_handler = Async::Queue.new
      host = deadline_host(server: server(echo: lambda { |message, _|
        if message.name == "sibling"
          entered.enqueue(true)
          release_handler.dequeue
        end
        message
      }))
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      sibling = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "sibling"), timeout: nil) }
      entered.dequeue
      resource = transport.instance_variable_get(:@resources).first
      gate = FrameGate.new(resource.socket)
      resource.connection.stream.instance_variable_set(:@io, gate)
      gate.armed = true
      partial = task.async(finished: false) do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "partial"), timeout: 0.02)
      end
      gate.started.dequeue
      wait_until(task, "deferred frame cancellation") do
        transport.instance_variable_get(:@calls).keys.any? { |op| op[:task]&.cancel_deferred? }
      end
      gate.release.enqueue(true)
      expect { partial.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("deadline_exceeded") }
      release_handler.enqueue(true)
      expect(sibling.wait.message.name).to eq("sibling")
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      expect(transport.instance_variable_get(:@resources).size).to eq(1)
      expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
    ensure
      gate&.release&.enqueue(true)
      release_handler&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  %i[h2c tls].each do |mode|
    it "raw-aborts a true nonreading #{mode} peer within one cleanup budget and joins writers/readers" do
      reactor do |task|
        with_ca do |ca_file|
          bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
          listener = bound.sockets.first
          ready = Async::Queue.new
          peer_release = Async::Queue.new
          peer_task = task.async do
            raw, = listener.accept
            raw.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 4096)
            peer = raw
            if mode == :tls
              peer = AsyncHelpers::INTERNAL::TLS.server(**host_tls).socket(raw)
              peer.accept
            end
            protocol = Protocol::HTTP2::Server.new(Protocol::HTTP2::Framer.new(IO::Stream(peer)))
            protocol.read_connection_preface(AsyncHelpers::INTERNAL.settings(128))
            ready.enqueue(true)
            # This external peer deliberately never reads after SETTINGS.
            peer_release.dequeue
          ensure
            AsyncHelpers::INTERNAL.raw_close(raw)
            peer&.close
          end
          origin = "#{mode == :tls ? 'https' : 'http'}://localhost:#{listener.local_address.ip_port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: mode == :tls ? ca_file : nil)
          sibling = task.async(finished: false) { transport.call(request("#{origin}/sibling")) }
          ready.dequeue
          resource = transport.instance_variable_get(:@resources).first
          resource.raw.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF, 4096)
          # Tildes expand under HPACK Huffman coding: enough wire bytes to block TCP,
          # without a megabyte of CPU-heavy header encoding before deadline observation.
          headers = { "x-large" => ["~" * 65_536] }
          started = AsyncHelpers::INTERNAL.now
          stuck = task.async(finished: false) do
            transport.call(request("#{origin}/blocked", headers: headers, deadline: ConnectRPC::Deadline.new(0.03)))
          end
          wait_until(task, "partial frame is cancellation deferred") do
            transport.instance_variable_get(:@calls).keys.any? { |op| op[:task]&.cancel_deferred? }
          end
          writer_tasks = resource.connection.writers.keys
          expect(writer_tasks).not_to be_empty
          expect { stuck.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("deadline_exceeded") }
          expect(AsyncHelpers::INTERNAL.now - started).to be < 0.4
          expect { sibling.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
          expect(resource.raw).to be_closed
          expect(writer_tasks).to all(be_finished)
          expect(resource.connection.reader_task).to be_finished
          expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
        ensure
          transport&.close(grace: 0)
          peer_release&.enqueue(true)
          peer_task&.wait
          bound&.close
        end
      end
    end
  end
end
