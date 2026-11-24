# frozen_string_literal: true

require_relative "support/http1_helpers"

RSpec.describe "Connect H1 cooperative final-close grace" do
  include ConnectH1Helpers

  def observe_owners
    owners = Async::Queue.new
    type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
    allow(type).to receive(:new).and_wrap_original do |constructor, *args, **options|
      constructor.call(*args, **options).tap { |owner| owners.enqueue(owner) }
    end
    owners
  end

  def maps(session)
    [session.instance_variable_get(:@admitted), session.instance_variable_get(:@h1_wire)]
  end

  [false, true].product(%i[fixed chunked]).each do |secure, framing|
    label = "#{secure ? 'TLS' : 'TCP'}/#{framing}"
    it "delivers HTTP EOF, then retains admission without reads to the total expiry on #{label}" do
      reactor do |task|
        _, cs, = methods
        owners = observe_owners
        # The stalled remainder read stops at request_body_timeout.
        with_host(target({ cs.path => lambda { |*|
          AsyncHelpers::MESSAGE.new(name: "early")
        } }), secure, request_body_timeout: 0.05) do |host, peer, origin|
          peer.write(request_head(cs, framing))
          owner = owners.dequeue
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("early")
          session = host.instance_variable_get(:@session)
          resource = session.instance_variable_get(:@resources).first
          eof_at = AsyncHelpers::INTERNAL.now
          closed_at = nil
          allow(resource.raw).to receive(:close).and_wrap_original do |close|
            closed_at ||= AsyncHelpers::INTERNAL.now
            close.call
          end
          expect(resource.raw).not_to be_closed
          expect(maps(session).map(&:size)).to eq([1, 0])
          # Data sent after delivery must stay unread through retirement.
          expect(owner.instance_variable_get(:@body)).not_to receive(:read)
          expect(resource.connection).not_to receive(:read_request)
          peer.write(upload("x", framing))
          expect(http1(origin, "/healthy", context: secure ? tls_context : nil).first).to eq(404)
          wait_until(task, "natural retirement releases both maps before host teardown") do
            resource.finished? && resource.task.finished? && maps(session).all?(&:empty?)
          end
          flushed_at = owner.instance_variable_get(:@close_expires) - 0.5
          elapsed = closed_at - flushed_at
          # Cooperative scheduler overshoot is allowed; an extra read allowance is not.
          expect(elapsed).to be_between(0.5, 0.58)
          expect(owner.instance_variable_get(:@discarded)).to eq(0)
          RSpec.configuration.reporter.message("H1 grace #{label}: #{JSON.generate(
            eof_ms: (eof_at - flushed_at) * 1000, raw_close_ms: elapsed * 1000
          )}")
        end
      end
    end

    it "reads at most 256 KiB first and parses no pipeline while waiting on #{label}" do
      reactor do |task|
        _, cs, = methods
        owners = observe_owners
        calls = 0
        handler = lambda { |*|
          calls += 1
          AsyncHelpers::MESSAGE.new(name: "capped")
        }
        with_host(target({ cs.path => handler }), secure) do |host, peer, _|
          peer.write(request_head(cs, framing, length: 524_288))
          owner = owners.dequeue
          sending = task.async do
            peer.write(upload("x" * 524_288, framing, finish: true) + request_head(cs, :fixed, length: 0))
          rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
            # The server stops reading partway, then closes.
            nil
          end
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("capped")
          session = host.instance_variable_get(:@session)
          resource = session.instance_variable_get(:@resources).first
          expect(resource.connection).not_to receive(:read_request)
          expect(owner.instance_variable_get(:@body)).not_to receive(:read)
          discarded = owner.instance_variable_get(:@discarded)
          # A declared remainder of 256 KiB is left unread, as net/http does.
          if framing == :fixed
            expect(discarded).to eq(0)
          else
            expect(discarded).to be_between(262_144, 262_144 + 65_535)
          end
          expect(maps(session).first.size).to eq(1)
          wait_until(task, "capped retirement joins ownership") do
            resource.finished? && resource.task.finished? && maps(session).all?(&:empty?)
          end
          expect(owner.instance_variable_get(:@discarded)).to eq(discarded)
          expect(calls).to eq(1)
          sending.stop
        end
      end
    end
  end

  # A delivered response counts as finished during its post-flush wait, so
  # a grace that ends inside it still reports drained, while it still forces
  # raw-first retirement.
  [false, true].each do |secure|
    it "reports a close that ends inside the post-response wait as drained, TLS=#{secure}" do
      reactor do |task|
        _, cs, = methods
        owners = observe_owners
        with_host(target({ cs.path => lambda { |*|
          AsyncHelpers::MESSAGE.new(name: "lingering")
        } }), secure, request_body_timeout: 0.05) do |host, peer, _|
          peer.write(request_head(cs, :chunked))
          owner = owners.dequeue
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("lingering")
          resource = host.instance_variable_get(:@session).instance_variable_get(:@resources).first
          wait_until(task, "the post-response wait") { owner.lingering? }
          expect(resource).not_to be_empty
          closing = task.async(finished: false) { host.close(grace: 0.1) }
          expect(closing).not_to be_finished
          expect(closing.wait).to be(true)
          expect(AsyncHelpers::INTERNAL.now).to be < owner.instance_variable_get(:@close_expires)
          wait_until(task, "forced retirement") { resource.send(:retirement_completed?) }
          expect(resource.raw).to be_closed
        end
      end
    end

    # A drain already waiting when the response is delivered wakes as the
    # post-flush wait begins, so its grace still ends inside that wait.
    it "wakes a waiting drain as the post-response wait begins, TLS=#{secure}" do
      reactor do |task|
        _, cs, = methods
        owners = observe_owners
        entered = Async::Queue.new
        gate = Async::Queue.new
        with_host(target({ cs.path => lambda { |*|
          entered.enqueue(true)
          gate.dequeue
          AsyncHelpers::MESSAGE.new(name: "lingering")
        } }), secure, request_body_timeout: 0.05) do |host, peer, _|
          peer.write(request_head(cs, :chunked))
          owner = owners.dequeue
          entered.dequeue
          resource = host.instance_variable_get(:@session).instance_variable_get(:@resources).first
          # The half-close that starts the wait yields, as a yielding
          # lifecycle observer before it would, so the drain consumes the
          # response's earlier wake-up before the wait begins.
          allow(resource.raw).to receive(:shutdown).and_wrap_original do |shutdown, how|
            Async::Task.current.yield if how == Socket::SHUT_WR
            shutdown.call(how)
          end
          closing = task.async(finished: false) { host.close(grace: 0.2) }
          wait_until(task, "the drain waits for the exchange") { resource.instance_variable_get(:@drain_task) }
          gate.enqueue(true)
          expect(peer.head.first).to eq(200)
          expect(closing.wait).to be(true)
          expect(owner).to be_lingering
          expect(AsyncHelpers::INTERNAL.now).to be < owner.instance_variable_get(:@close_expires)
        ensure
          gate&.enqueue(true)
        end
      end
    end

    # Each caller of one drain is judged against its own deadline.
    it "reports each joined H1 drain against its own deadline, TLS=#{secure}" do
      reactor do |task|
        _, cs, = methods
        owners = observe_owners
        entered = Async::Queue.new
        gate = Async::Queue.new
        with_host(target({ cs.path => lambda { |*|
          entered.enqueue(true)
          gate.dequeue
          AsyncHelpers::MESSAGE.new(name: "joined")
        } }), secure, request_body_timeout: 0.05) do |host, peer, _|
          peer.write(request_head(cs, :chunked))
          owner = owners.dequeue
          entered.dequeue
          session = host.instance_variable_get(:@session)
          lifetime = session.instance_variable_get(:@lifetime)
          resource = session.instance_variable_get(:@resources).first
          closing = task.async(finished: false) { host.close(grace: 5) }
          wait_until(task, "the drain waits for the exchange") { resource.instance_variable_get(:@drain_task) }
          before_finish = AsyncHelpers::INTERNAL.now
          gate.enqueue(true)
          expect(peer.head.first).to eq(200)
          wait_until(task, "the post-response wait") { owner.lingering? }
          expect(resource.drain(lifetime, AsyncHelpers::INTERNAL.now + 0.05)).to be(true)
          expect(resource.drain(lifetime, before_finish)).to be(false)
          expect(closing.wait).to be(true)
        ensure
          gate&.enqueue(true)
        end
      end
    end
  end

  [false, true].product(%i[abort close drain_tls]).each do |secure, operation|
    next if operation == :drain_tls && !secure

    it "interrupts remaining grace with owner #{operation}, TLS=#{secure}" do
      reactor do |task|
        _, cs, = methods
        owners = observe_owners
        with_host(target({ cs.path => lambda { |*|
          AsyncHelpers::MESSAGE.new(name: "remaining")
        } }), secure, request_body_timeout: 0.05) do |host, peer, _|
          peer.write(request_head(cs, :chunked))
          owner = owners.dequeue
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("remaining")
          session = host.instance_variable_get(:@session)
          resource = session.instance_variable_get(:@resources).first
          expect(resource.raw).not_to be_closed
          expect(maps(session).first.size).to eq(1)
          # Observe the join before host.close/abort can hide leaks by clearing.
          maps(session).each do |map|
            allow(map).to receive(:clear).and_wrap_original do |clear|
              expect(map).to be_empty
              clear.call
            end
          end
          case operation
          when :abort then host.abort
          when :close then host.close(grace: 0.02)
          when :drain_tls
            host.reload_tls(**host_tls)
            host.drain_tls(grace: 0.02)
          end
          wait_until(task, "shutdown joins the connection and all processor tasks") do
            resource.finished? && resource.task.finished? && resource.instance_variable_get(:@tasks).all?(&:finished?)
          end
          expect(maps(session)).to all(be_empty)
          expect(AsyncHelpers::INTERNAL.now).to be < owner.instance_variable_get(:@close_expires)
        end
      end
    end
  end
end
