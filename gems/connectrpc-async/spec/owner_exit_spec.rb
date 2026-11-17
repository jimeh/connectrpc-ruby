# frozen_string_literal: true

require_relative "support/http1_client_helpers"

# A transport the application never closes is aborted once its owner task and
# every other task under that owner have finished, so the owner can finish.
RSpec.describe "Client transport owner exit without close" do
  include HTTP1ClientHelpers

  def raws(transport) = transport.instance_variable_get(:@resources).filter_map(&:raw)
  def state(transport) = transport.instance_variable_get(:@state)

  # An H2 host and a raw H1 peer on one reactor.
  def with_both_origins
    body = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "h1"))
    wire = http1_wire(body, fields: "Content-Type: application/proto\r\n")
    with_http1_peer(response: wire) do |_transport, h1_origin, _accepted, _requests, task|
      host = deadline_host(server: server)
      yield task, { ConnectRPC::Async::Transport => "http://127.0.0.1:#{host.port}",
                    ConnectRPC::Async::HTTP1Transport => h1_origin }
    ensure
      host&.close(grace: 0)
    end
  end

  def echo(transport, origin, name)
    client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: name), timeout: 5).message.name
  end

  [ConnectRPC::Async::Transport, ConnectRPC::Async::HTTP1Transport].each do |kind|
    name = kind.name.split("::").last

    it "aborts an unclosed #{name} once its nested owner finishes, closing its sockets" do
      with_both_origins do |task, origins|
        origin = origins.fetch(kind)
        transport = nil
        owner = task.async do
          transport = kind.new(origin: origin)
          echo(transport, origin, "owned")
        end
        expect(owner.wait).to eq(kind == ConnectRPC::Async::Transport ? "owned" : "h1")
        wait_until(task, "the owner task to finish") { owner.finished? }
        expect(state(transport)).to eq(:closed)
        expect(raws(transport)).not_to be_empty
        expect(raws(transport)).to all(be_closed)
      end
    end

    it "keeps an unclosed #{name} for a live descendant of its finished owner block" do
      with_both_origins do |task, origins|
        origin = origins.fetch(kind)
        gate = Async::Promise.new
        transport = nil
        results = []
        owner = task.async do |owned|
          transport = kind.new(origin: origin)
          # The child's own block ends at once; its grandchild still uses the transport.
          owned.async do |child|
            child.async do
              gate.wait
              results << echo(transport, origin, "late")
            end
          end
          :returned
        end
        expect(owner.wait).to eq(:returned)
        wait_until(task, "the owner block to complete") { owner.completed? }
        expect([owner.finished?, state(transport)]).to eq([false, :running])
        gate.resolve(nil)
        wait_until(task, "the owner task to finish") { owner.finished? }
        expect(results).to eq([kind == ConnectRPC::Async::Transport ? "late" : "h1"])
        expect(state(transport)).to eq(:closed)
        expect(raws(transport)).to all(be_closed)
      ensure
        gate&.resolve(nil)
      end
    end

    it "aborts an unclosed #{name} once its owner block raises, letting the owner finish" do
      with_both_origins do |task, origins|
        origin = origins.fetch(kind)
        transport = nil
        owner = task.async(finished: false) do
          transport = kind.new(origin: origin)
          echo(transport, origin, "owned")
          raise IOError, "owner failed"
        end
        expect { owner.wait }.to raise_error(IOError, "owner failed")
        wait_until(task, "the owner task to finish") { owner.finished? }
        expect(state(transport)).to eq(:closed)
        expect(raws(transport)).not_to be_empty
        expect(raws(transport)).to all(be_closed)
      end
    end

    it "still aborts an unclosed #{name} when its owner is cancelled" do
      with_both_origins do |task, origins|
        origin = origins.fetch(kind)
        transport = nil
        ready = Async::Promise.new
        owner = task.async do
          transport = kind.new(origin: origin)
          echo(transport, origin, "owned")
          ready.resolve(nil)
          Async::Promise.new.wait
        end
        ready.wait
        owner.stop
        owner.wait
        wait_until(task, "the owner task to finish") { owner.finished? }
        expect(state(transport)).to eq(:closed)
        expect(raws(transport)).to all(be_closed)
      end
    end

    # A cancelled task resolves its promise before it stops its children, so
    # its fiber stays alive while their cleanup yields. Here that cleanup
    # waits until another task runs, after the owner's block has returned.
    # The reactor runs in a thread so a guard that spins on the resolved task
    # fails the bounded join instead of freezing the suite.
    it "lets a cancelled task under the owner of an unclosed #{name} finish its yielding cleanup" do
      cleaned = Thread::Queue.new
      outcome = Thread.new do
        Sync do |root|
          release = Async::Promise.new
          worker = nil
          owner = root.async do |owned|
            transport = kind.new(origin: "http://127.0.0.1:1")
            worker = owned.async do |child|
              child.async do
                Async::Promise.new.wait
              ensure
                release.wait
                # Work still running under the owner keeps the transport open.
                cleaned << state(transport)
              end
              Async::Promise.new.wait
            end
            :owner
          end
          owner.wait
          worker.stop
          root.yield
          release.resolve(nil)
          owner.wait
          root.yield until owner.finished?
          :done
        end
      end
      expect(outcome.join(5)&.value).to eq(:done)
      expect(cleaned.size).to eq(1)
      expect(cleaned.pop).to eq(:running)
    ensure
      outcome&.kill
      outcome&.join(5)
    end

    it "returns from a root Sync block that leaves a #{name} unclosed" do
      outcome = Thread.new do
        Sync do
          kind.new(origin: "http://127.0.0.1:1")
          :done
        end
      end
      expect(outcome.join(5)&.value).to eq(:done)
    ensure
      outcome&.kill
      outcome&.join(5)
    end
  end

  it "finishes an owner shared by unclosed HTTP/2 and HTTP/1 transports" do
    with_both_origins do |task, origins|
      transports = {}
      owner = task.async do
        origins.each do |kind, origin|
          transports[kind] = kind.new(origin: origin)
          echo(transports[kind], origin, "shared")
        end
      end
      owner.wait
      wait_until(task, "the shared owner to finish") { owner.finished? }
      expect(transports.values.map { |transport| state(transport) }).to eq(%i[closed closed])
      expect(transports.values.flat_map { |transport| raws(transport) }).to all(be_closed)
    end
  end
end
