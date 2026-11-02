# frozen_string_literal: true

RSpec.describe "HTTP/2 wire drain ownership" do
  it "waits after handler return for the last DATA frame, while later old streams get 503" do
    reactor do |task|
      handler_done = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        handler_done.enqueue(true) if message.name.size > 1000
        message
      }), tls: host_tls).start
      with_ca do |ca_file|
        origin = "https://localhost:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        typed = client(transport, origin)
        typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
        record = host.instance_variable_get(:@resources).first
        gate = FrameGate.new(record.socket, frame_type: 0)
        record.connection.stream.instance_variable_set(:@io, gate)
        gate.armed = true
        message = AsyncHelpers::MESSAGE.new(name: "x" * 200_000)
        response = task.async { typed.echo(message, timeout: nil) }
        handler_done.dequeue
        gate.started.dequeue
        expect(record.connection.streams).not_to be_empty
        host.reload_tls(**host_tls)
        drain = task.async { host.drain_tls(grace: 1) }
        wait_until(task, "draining after handler") { record.draining }
        expect(drain).not_to be_finished
        denied = task.async do
          transport.call(request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}",
                                 headers: { "content-type" => ["application/proto"] }))
        end
        gate.release.enqueue(true)
        expect(response.wait.message).to eq(message)
        expect(denied.wait.status).to eq(503)
        expect(drain.wait).to be(true)
        expect(record.raw).to be_closed
      ensure
        gate&.release&.enqueue(true)
        transport&.close(grace: 0)
      end
    ensure
      host&.close(grace: 0)
    end
  end

  it "shares connection retirement across overlapping drains and lets close supersede them without task leaks" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        entered.enqueue(true)
        release.dequeue
        message
      }),
                                         tls: host_tls).start
      with_ca do |ca_file|
        origin = "https://localhost:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        call = task.async(finished: false) { client(transport, origin).echo(AsyncHelpers::MESSAGE.new, timeout: nil) }
        entered.dequeue
        record = host.instance_variable_get(:@resources).first
        host.reload_tls(**host_tls)
        first = task.async { host.drain_tls(grace: 1) }
        wait_until(task, "first retirement") { record.draining }
        second = task.async { host.drain_tls(grace: 1) }
        expect(host.close(grace: 0)).to be(false)
        expect(first.wait).to be(false)
        expect(second.wait).to be(false)
        expect { call.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
        expect(record.raw).to be_closed
        expect(record.tasks).to all(be_finished)
        expect(record.connection.reader_task).to be_finished
        expect { host.reload_tls(**host_tls) }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
      ensure
        release&.enqueue(true)
        transport&.close(grace: 0)
      end
    ensure
      host&.close(grace: 0)
    end
  end
end
