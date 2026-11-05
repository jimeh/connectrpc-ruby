# frozen_string_literal: true

require_relative "support/http1_helpers"

RSpec.describe "Connect H1 cooperative final-close grace" do
  include ConnectH1Helpers

  def observe_discard
    entered = Async::Queue.new
    stopped = Async::Queue.new
    type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
    allow(type).to receive(:new).and_wrap_original do |constructor, *args, **options|
      constructor.call(*args, **options).tap do |owner|
        allow(owner).to receive(:discard).and_wrap_original do |discard|
          started = AsyncHelpers::INTERNAL.now
          entered.enqueue(owner)
          begin
            discard.call
          ensure
            stopped.enqueue([owner, AsyncHelpers::INTERNAL.now - started])
          end
        end
      end
    end
    [entered, stopped]
  end

  def maps(session)
    [session.instance_variable_get(:@admitted), session.instance_variable_get(:@h1_wire)]
  end

  [false, true].product(%i[fixed chunked]).each do |secure, framing|
    label = "#{secure ? 'TLS' : 'TCP'}/#{framing}"
    it "delivers HTTP EOF, stops reads at 100 ms, then retains admission to the total expiry on #{label}" do
      reactor do |task|
        _, cs, = methods
        entered, stopped = observe_discard
        with_host(target({ cs.path => lambda { |*|
          AsyncHelpers::MESSAGE.new(name: "early")
        } }), secure) do |host, peer, origin|
          peer.write(request_head(cs, framing))
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("early")
          owner = entered.dequeue
          session = host.instance_variable_get(:@session)
          resource = session.instance_variable_get(:@resources).first
          eof_at = AsyncHelpers::INTERNAL.now
          closed_at = nil
          allow(resource.raw).to receive(:close).and_wrap_original do |close|
            closed_at ||= AsyncHelpers::INTERNAL.now
            close.call
          end
          expect(resource.raw).not_to be_closed
          expect(maps(session).first.size).to eq(1)
          # The HTTP terminal arrived while the framed read was still pending.
          expect(stopped).to be_empty
          _, discard_elapsed = stopped.dequeue
          expect(discard_elapsed).to be_between(0.07, 0.3)
          expect(resource.raw).not_to be_closed
          expect(maps(session).map(&:size)).to eq([1, 0])
          # Data sent after discard stops must stay unread through retirement.
          expect(owner.instance_variable_get(:@body)).not_to receive(:read)
          expect(resource.connection).not_to receive(:read_request)
          peer.write(upload("x", framing))
          expect(http1(origin, "/healthy", context: secure ? tls_context : nil).first).to eq(404)
          wait_until(task, "natural retirement releases both maps before host teardown") do
            resource.finished? && resource.task.finished? && maps(session).all?(&:empty?)
          end
          flushed_at = owner.instance_variable_get(:@close_expires) - 0.5
          elapsed = closed_at - flushed_at
          # Cooperative scheduler overshoot is allowed; an extra 100 ms is not.
          expect(elapsed).to be_between(0.5, 0.58)
          expect(owner.instance_variable_get(:@discarded)).to eq(0)
          RSpec.configuration.reporter.message("H1 grace #{label}: #{JSON.generate(
            eof_ms: (eof_at - flushed_at) * 1000, discard_ms: discard_elapsed * 1000,
            raw_close_ms: elapsed * 1000
          )}")
        end
      end
    end

    it "keeps the byte cap and parses no pipeline while waiting on #{label}" do
      reactor do |task|
        _, cs, = methods
        entered, stopped = observe_discard
        calls = 0
        handler = lambda { |*|
          calls += 1
          AsyncHelpers::MESSAGE.new(name: "capped")
        }
        with_host(target({ cs.path => handler }), secure) do |host, peer, _|
          peer.write(request_head(cs, framing, length: 524_288))
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("capped")
          owner = entered.dequeue
          reads = []
          allow(owner.instance_variable_get(:@body)).to receive(:read).and_wrap_original do |read|
            read.call.tap { |bytes| reads << bytes.bytesize if bytes }
          end
          session = host.instance_variable_get(:@session)
          resource = session.instance_variable_get(:@resources).first
          expect(resource.connection).not_to receive(:read_request)
          sending = task.async do
            peer.write(upload("x" * 524_288, framing, finish: true) + request_head(cs, :fixed, length: 0))
          end
          expect(stopped.dequeue.first).to equal(owner)
          sending.wait
          expect(resource.raw).not_to be_closed
          discarded = owner.instance_variable_get(:@discarded)
          expect(discarded).to be_between(262_144 - 65_535, 262_144)
          expect(reads).not_to be_empty
          expect(reads.max).to be <= 65_536
          expect(owner.instance_variable_get(:@body)).not_to receive(:read)
          expect(maps(session).first.size).to eq(1)
          wait_until(task, "byte-capped retirement joins ownership") do
            resource.finished? && resource.task.finished? && maps(session).all?(&:empty?)
          end
          expect(owner.instance_variable_get(:@discarded)).to eq(discarded)
          expect(calls).to eq(1)
          RSpec.configuration.reporter.message("H1 cap #{label}: #{JSON.generate(discarded: discarded, reads: reads)}")
        end
      end
    end
  end

  [false, true].product(%i[abort close drain_tls]).each do |secure, operation|
    next if operation == :drain_tls && !secure

    it "interrupts remaining grace with owner #{operation}, TLS=#{secure}" do
      reactor do |task|
        _, cs, = methods
        entered, stopped = observe_discard
        with_host(target({ cs.path => lambda { |*|
          AsyncHelpers::MESSAGE.new(name: "remaining")
        } }), secure) do |host, peer, _|
          peer.write(request_head(cs, :chunked))
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("remaining")
          owner = entered.dequeue
          expect(stopped.dequeue.first).to equal(owner)
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
