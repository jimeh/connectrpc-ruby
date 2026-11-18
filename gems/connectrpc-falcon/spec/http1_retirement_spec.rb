# frozen_string_literal: true

require_relative "support/http1_helpers"
require_relative "../../connectrpc-async/spec/support/retirement_helpers"

# Forced HTTP/1 retirement shuts raw TCP down first, so a fiber blocked on it
# wakes with end of stream, and closes the descriptor only once every task
# that may block on it has left it. On Ruby 4 an earlier close could raise a
# late IOError into a fiber that had already left its read.
RSpec.describe "Falcon HTTP/1 forced retirement" do
  include ConnectH1Helpers
  include RetirementHelpers

  # Reads whatever the peer has left, including TLS records, and reports
  # whether it then reached end of stream.
  def peer_eof?(raw)
    loop do
      result = raw.read_nonblock(16_384, exception: false)
      return result.nil? unless result.is_a?(String)
    end
  rescue IOError, SystemCallError
    true
  end

  [false, true].each do |secure|
    label = secure ? "TLS" : "TCP"

    it "fails every #{label} peer before listener cleanup and closes after a streaming input unwinds" do
      reactor do |task|
        _, cs, = methods
        entered = Async::Queue.new
        gate = RetirementHelpers::CleanupGate.new
        woken = []
        handler = lambda do |reader, _context|
          entered.enqueue(true)
          begin
            reader.receive
          rescue StandardError => e
            # The blocked body read wakes at the raw shutdown. The handler
            # then stays a retirement member until retirement cancels it.
            woken << e
            Async::Condition.new.wait
          end
        ensure
          gate.hold($ERROR_INFO)
        end
        inputs = []
        input_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
        closed_inputs = Async::Queue.new
        allow(input_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
          constructor.call(*args, **options).tap do |input|
            inputs << input
            allow(input).to receive(:close).and_wrap_original do |close, *values|
              close.call(*values)
            ensure
              closed_inputs.enqueue(input)
            end
          end
        end
        with_host(target({ cs.path => handler }), secure) do |host, peer, origin|
          session = host.instance_variable_get(:@session)
          peer.write(request_head(cs, :chunked))
          task.with_timeout(2) { entered.dequeue }
          # A second peer whose protocol or TLS handshake is still pending.
          pending = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
          wait_until(task, "both peers accepted") { session.instance_variable_get(:@resources).size == 2 }
          resources = session.instance_variable_get(:@resources).dup
          http1 = resources.find { |resource| resource.is_a?(ConnectRPC::Falcon.const_get(:HTTP1Resource)) }
          expect(http1.input_retirement).to equal(inputs.fetch(0))
          boundary = Async::Queue.new
          release = Async::Queue.new
          allow(session).to receive(:stop_accepting).and_wrap_original do |stop|
            boundary.enqueue(true)
            release.dequeue
            stop.call
          end
          aborter = task.async(finished: false) { session.abort }
          task.with_timeout(2) { boundary.dequeue }
          # While listener cleanup is held, both peers read end of stream and
          # neither server descriptor has closed.
          wait_until(task, "both peers read end of stream") { [peer.raw, pending].all? { |raw| peer_eof?(raw) } }
          expect(resources.map(&:raw)).to all(satisfy { |raw| !raw.closed? })
          wait_until(task, "the handler's blocked body read woke") { !woken.empty? }
          expect(woken.first).to be_a(EOFError)
          release.enqueue(true)
          # The connection's input cleanup leaves raw to retirement, which
          # still waits for the handler.
          expect(task.with_timeout(2) { closed_inputs.dequeue }).to equal(inputs.first)
          expect(task.with_timeout(2) { gate.entered.dequeue }).not_to be_nil
          expect(http1.raw).not_to be_closed
          expect(aborter).not_to be_finished
          gate.release
          join_retirement_tasks(task, aborter)
          expect(resources).to all(satisfy { |resource| retirement_complete?(resource) })
          expect(resources.map(&:raw)).to all(be_closed)
          expect(session.instance_variable_get(:@state)).to eq(:closed)
        ensure
          release&.enqueue(true)
          gate.release
          AsyncHelpers::INTERNAL.raw_close(pending)
        end
      end
    end

    it "reports a #{label} drain interrupted in its discard read by session abort as not drained" do
      reactor do |task|
        rack = ->(_env) { [200, { "content-type" => "text/plain" }, ["ok"]] }
        falcon(rack_app: rack, scheme: secure ? "https" : "http", tls: secure ? host_tls : nil) do |host, origin|
          peer = ConnectH1Wire.new(origin, context: secure ? tls_context : nil)
          peer.write("GET /kept HTTP/1.1\r\nHost: localhost\r\n\r\n")
          expect(peer.head.first).to eq(200)
          session = host.instance_variable_get(:@session)
          resource = session.instance_variable_get(:@resources).first
          wait_until(task, "the response completed") { resource.empty? }
          lifetime = session.instance_variable_get(:@lifetime)
          drainer = task.async(finished: false) { resource.drain(lifetime, AsyncHelpers::INTERNAL.now + 5) }
          wait_until(task, "the drain discarding input") { resource.instance_variable_get(:@discarding) }
          aborter = task.async(finished: false) { session.abort }
          join_retirement_tasks(task, aborter, drainer)
          # The end of stream after the local raw failure is not the peer's.
          expect(drainer.result).to be(false)
          expect(retirement_complete?(resource)).to be(true)
          expect(resource.raw).to be_closed
        ensure
          peer&.close
        end
      end
    end
  end
end
