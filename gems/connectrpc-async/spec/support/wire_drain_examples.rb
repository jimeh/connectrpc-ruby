# frozen_string_literal: true

RSpec.shared_examples "HTTP/2 wire drain ownership" do
  it "waits after handler return for the last DATA frame, while later old streams are still served" do
    reactor do |task|
      handler_done = Async::Queue.new
      host = wire_host(server: server(echo: lambda { |message, _|
        handler_done.enqueue(true) if message.name.size > 1000
        message
      }), tls: host_tls)
      with_ca do |ca_file|
        origin = "https://localhost:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        typed = client(transport, origin)
        typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
        record = wire_session(host).instance_variable_get(:@resources).first
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
        # The heads-up GOAWAY waits behind the held frame, so this stream
        # opens before the cutoff and is dispatched, not refused with 503.
        later = task.async do
          transport.call(request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}",
                                 headers: { "content-type" => ["application/proto"] }))
        end
        gate.release.enqueue(true)
        expect(response.wait.message).to eq(message)
        expect(later.wait.status).to eq(200)
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

  # One held echo call on an instrumented host, with the server resource,
  # the client connection and the server completions. `finished` runs as the
  # server observer's post-delivery callback, after recording its completion.
  def held_drain_call(task, tls: false, finished: nil)
    entered = Async::Queue.new
    release = Async::Queue.new
    completions = []
    observer = Object.new
    observer.define_singleton_method(:finished) do |completion|
      completions << completion
      finished&.call
    end
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    instrumentation = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    host = wire_host(server: server(instrumentation: instrumentation, echo: lambda { |message, _|
      entered.enqueue(true)
      release.dequeue
      message
    }), tls: (host_tls if tls))
    with_ca do |ca_file|
      origin = "#{tls ? 'https' : 'http'}://localhost:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: (ca_file if tls))
      call = task.async(finished: false) do
        client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
      end
      entered.dequeue
      yield host: host, call: call, release: release, completions: completions,
            record: wire_session(host).instance_variable_get(:@resources).first,
            peer: transport.instance_variable_get(:@resources).first.connection, transport: transport
    ensure
      release.enqueue(true)
      transport&.close(grace: 0)
    end
  ensure
    host&.close(grace: 0)
  end

  # A client that saw the drain's GOAWAY retires the connection as soon as
  # its last stream ends, so its EOF or reset can reach the server reader
  # before the server's stream worker finishes its post-delivery work.
  [false, true].product(%i[cleanup observer]).each do |tls, point|
    it "completes a drain when the peer leaves during #{point} work after its last stream (#{tls ? 'TLS' : 'h2c'})" do
      reactor do |task|
        record = nil
        # Post-delivery work that yields until the server reader has seen the
        # peer leave, as an observer that does I/O would.
        settle = lambda do
          wait_until(Async::Task.current, "server reader ended") { record.connection.reader_task.finished? }
        end
        held_drain_call(task, tls: tls, finished: (settle if point == :observer)) do |held|
          record = held.fetch(:record)
          if point == :cleanup
            allow(record.connection).to receive(:finish_request).and_wrap_original do |original, *args, **options|
              settle.call
            ensure
              original.call(*args, **options)
            end
          end
          closing = task.async { held.fetch(:host).close(grace: 5) }
          wait_until(task, "client GOAWAY") { held.fetch(:peer).goaway_received? }
          held.fetch(:release).enqueue(true)
          expect(held.fetch(:call).wait.message.name).to eq("held")
          expect(closing.wait).to be(true)
          expect(held.fetch(:completions).map(&:delivery)).to eq([:completed])
          expect(record.tasks).to all(be_finished)
          expect(record.raw).to be_closed
        end
      end
    end
  end

  it "fails a drain at its grace when post-delivery work outlives the peer that left" do
    reactor do |task|
      held_drain_call(task, finished: -> { Async::Condition.new.wait }) do |held|
        record = held.fetch(:record)
        closing = task.async { held.fetch(:host).close(grace: 0.5) }
        wait_until(task, "client GOAWAY") { held.fetch(:peer).goaway_received? }
        held.fetch(:release).enqueue(true)
        expect(held.fetch(:call).wait.message.name).to eq("held")
        wait_until(task, "server reader ended") { record.connection.reader_task.finished? }
        expect(record).not_to be_finished
        expect(closing.wait).to be(false)
        expect(record.tasks).to all(be_finished)
        expect(record.raw).to be_closed
      end
    end
  end

  it "ends a drain at once, before its grace, when the peer leaves with a stream still open" do
    reactor do |task|
      held_drain_call(task) do |held|
        closing = task.async { held.fetch(:host).close(grace: 5) }
        wait_until(task, "client GOAWAY") { held.fetch(:peer).goaway_received? }
        held.fetch(:transport).close(grace: 0)
        wait_until(task, "drain ended") { closing.finished? }
        expect(closing.wait).to be(false)
        expect { held.fetch(:call).wait }.to raise_error(ConnectRPC::Error)
        wait_until(task, "server completion") { held.fetch(:completions).any? }
        expect(held.fetch(:completions).map(&:delivery)).to eq([:aborted])
        expect(held.fetch(:record).raw).to be_closed
      end
    end
  end

  it "shares connection retirement across overlapping drains and lets close supersede them without task leaks" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = wire_host(server: server(echo: lambda { |message, _|
        entered.enqueue(true)
        release.dequeue
        message
      }),
                       tls: host_tls)
      with_ca do |ca_file|
        origin = "https://localhost:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        call = task.async(finished: false) { client(transport, origin).echo(AsyncHelpers::MESSAGE.new, timeout: nil) }
        entered.dequeue
        record = wire_session(host).instance_variable_get(:@resources).first
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
