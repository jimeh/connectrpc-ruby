# frozen_string_literal: true

require_relative "support/http1_helpers"

RSpec.describe "Connect H1 adversarial wire lifecycle" do
  include ConnectH1Helpers

  [false, true].product(%i[fixed chunked]).each do |secure, framing|
    label = "#{secure ? 'TLS' : 'TCP'}/#{framing}"

    it "keeps consumed CS reusable on #{label}" do
      reactor do
        _, cs, = methods
        handler = lambda do |reader, _|
          first = reader.receive
          expect(reader.receive).to be_nil
          first
        end
        with_host(target({ cs.path => handler }), secure) do |_host, peer, _|
          2.times do
            bytes = frame("reused")
            peer.write(request_head(cs, framing, length: bytes.bytesize) + upload(bytes, framing, finish: true))
            expect(peer.head.first).to eq(200)
            expect(peer.body).to include("reused")
          end
        end
      end
    end

    it "retires oversized unread #{label} input without replaying a pipelined request" do
      reactor do |task|
        _, cs, = methods
        calls = 0
        entered = Async::Queue.new
        owner_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
        allow(owner_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
          constructor.call(*args, **options).tap do |owner|
            allow(owner).to receive(:discard).and_wrap_original do |discard|
              entered.enqueue(owner)
              discard.call
            end
          end
        end
        handler = lambda { |*|
          calls += 1
          AsyncHelpers::MESSAGE.new(name: "early")
        }
        with_host(target({ cs.path => handler }), secure) do |host, peer, _|
          peer.write(request_head(cs, framing, length: 400_000))
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("early")
          owner = entered.dequeue
          # Supply more than the allowance, followed by a real second request.
          sending = task.async do
            peer.write(upload("x" * 400_000, framing, finish: true) + request_head(cs, :fixed, length: 0))
          rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
            nil
          end
          session = host.instance_variable_get(:@session)
          wait_until(task, "retirement releases admission and connection") do
            session.instance_variable_get(:@admitted).empty? &&
              session.instance_variable_get(:@resources).all?(&:finished?)
          end
          sending.wait
          expect(owner.instance_variable_get(:@discarded)).to be <= 262_144
          expect(calls).to eq(1)
          expect(session.instance_variable_get(:@resources)).to all(be_finished)
        end
      end
    end

    %i[abort drain].each do |stop|
      it "retains ownership until #{stop} interrupts #{label} hygiene, leaving other peers healthy" do
        reactor do |task|
          _, cs, = methods
          entered = Async::Queue.new
          owner_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
          allow(owner_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
            constructor.call(*args, **options).tap do |owner|
              allow(owner).to receive(:discard).and_wrap_original do |discard|
                entered.enqueue(owner)
                discard.call
              end
            end
          end
          handler = ->(*) { AsyncHelpers::MESSAGE.new(name: "early") }
          with_host(target({ cs.path => handler }), secure) do |host, peer, origin|
            peer.write(request_head(cs, framing))
            expect(peer.head.first).to eq(200)
            expect(peer.body).to include("early")
            entered.dequeue
            session = host.instance_variable_get(:@session)
            expect(session.instance_variable_get(:@admitted).size).to eq(1)
            other = ConnectH1Wire.new(origin, context: secure ? tls_context : nil)
            other.write(request_head(cs, :fixed, length: 0))
            expect(other.head.first).to eq(200)
            expect(other.body).to include("early")
            stop == :abort ? session.abort : host.close(grace: 0.02)
            wait_until(task, "all retirement owners joined") do
              session.instance_variable_get(:@admitted).empty? &&
                session.instance_variable_get(:@resources).all?(&:finished?)
            end
          ensure
            other&.close
          end
        end
      end
    end
  end

  [false, true].each do |secure|
    {
      malformed_length: "oops\r\n",
      malformed_terminator: "1\r\nx!!",
      invalid_trailer: "0\r\ncontent-length: 9\r\n\r\n",
      truncated_chunk: "9\r\nx",
      truncated_fixed: "x"
    }.each do |kind, payload|
      it "preserves delivered response across #{kind} hygiene failure over #{secure ? 'TLS' : 'TCP'}" do
        reactor do |task|
          _, cs, = methods
          errors = []
          owner_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
          allow(owner_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
            constructor.call(*args, **options).tap do |owner|
              allow(owner).to receive(:discard).and_wrap_original do |discard|
                discard.call
              rescue StandardError => e
                errors << e
                raise
              end
            end
          end
          with_host(target({ cs.path => lambda { |*|
            AsyncHelpers::MESSAGE.new(name: "delivered")
          } }), secure) do |host, peer, _|
            framing = kind == :truncated_fixed ? :fixed : :chunked
            peer.write(request_head(cs, framing))
            expect(peer.head.first).to eq(200)
            expect(peer.body).to include("delivered")
            peer.write(payload)
            # TLS close_notify would end both directions; the completed HTTP
            # response is already observed, so raw EOF is the intended boundary.
            peer.raw.shutdown(Socket::SHUT_WR) if kind.to_s.start_with?("truncated")
            session = host.instance_variable_get(:@session)
            wait_until(task, "malformed input retirement completed") do
              session.instance_variable_get(:@admitted).empty?
            end
            expect(errors).not_to be_empty
            expect(errors.first).to be_a(Protocol::HTTP::InvalidTrailerError) if kind == :invalid_trailer
          end
        end
      end
    end

    it "bounds continuing trickle by absolute elapsed time over #{secure ? 'TLS' : 'TCP'}" do
      reactor do |task|
        _, cs, = methods
        progress = Async::Queue.new
        retired = Async::Queue.new
        owner_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
        allow(owner_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
          constructor.call(*args, **options).tap do |owner|
            allow(owner).to receive(:discard).and_wrap_original do |discard|
              body = owner.instance_variable_get(:@body)
              allow(body).to receive(:read).and_wrap_original do |read|
                read.call.tap { progress.enqueue(true) }
              end
              start = AsyncHelpers::INTERNAL.now
              begin
                discard.call
              ensure
                retired.enqueue(AsyncHelpers::INTERNAL.now - start)
              end
            end
          end
        end
        with_host(target({ cs.path => ->(*) { AsyncHelpers::MESSAGE.new(name: "early") } }), secure) do |_host, peer, _|
          peer.write(request_head(cs, :chunked, extra: "grpc-timeout: 1n\r\n"))
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("early")
          sender = task.async do
            loop do
              peer.write("1\r\nx\r\n")
              progress.dequeue
              # Elapsed time is the real external upload boundary under test.
              # Each delay follows an observed framed read, never assumed progress.
              task.sleep(0.01)
            end
          rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
            nil
          end
          elapsed = retired.dequeue
          expect(elapsed).to be_between(0.07, 0.3)
        ensure
          AsyncHelpers::INTERNAL.cancel(sender)
          AsyncHelpers::INTERNAL.join(sender)
        end
      end
    end
  end

  [false, true].product(%i[fixed chunked]).each do |secure, framing|
    it "never parses a pipeline after completing original #{framing} hygiene, TLS #{secure}" do
      reactor do |task|
        _, cs, = methods
        calls = 0
        handler = lambda { |*|
          calls += 1
          AsyncHelpers::MESSAGE.new(name: "early")
        }
        with_host(target({ cs.path => handler }), secure) do |host, peer, _|
          peer.write(request_head(cs, framing, length: 1))
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("early")
          session = host.instance_variable_get(:@session)
          resource = session.instance_variable_get(:@resources).first
          expect(resource.connection).not_to receive(:read_request)
          peer.write(upload("x", framing, finish: true) + request_head(cs, :fixed, length: 0))
          wait_until(task, "connection retired after original framing completes") do
            session.instance_variable_get(:@resources).all?(&:finished?)
          end
          expect(calls).to eq(1)
        end
      end
    end

    it "renders a bounded oversized-envelope error before #{framing} upload finishes, TLS #{secure}" do
      reactor do
        _, cs, = methods
        implementation = target({ cs.path => ->(reader, _) { reader.receive } }, request_limit: 16)
        expect(implementation.instance_variable_get(:@binding)).not_to receive(:decode)
        with_host(implementation, secure) do |_host, peer, _|
          prefix = [0, 32].pack("CN")
          peer.write(request_head(cs, framing) + upload(prefix, framing))
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include('"code":"resource_exhausted"')
        end
      end
    end
  end

  [false, true].each do |secure|
    it "joins an actual deadline input child before H1 hygiene, TLS #{secure}" do
      reactor do
        _, cs, = methods
        readers = []
        checked = Async::Queue.new
        input_type = AsyncHelpers::INTERNAL::Input
        allow(input_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
          constructor.call(*args, **options).tap do |input|
            allow(input).to receive(:read_fragment).and_wrap_original do |read, *values|
              readers << Async::Task.current
              read.call(*values)
            end
          end
        end
        owner_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
        allow(owner_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
          constructor.call(*args, **options).tap do |owner|
            allow(owner).to receive(:discard).and_wrap_original do |discard|
              expect(readers).not_to be_empty
              expect(readers).to all(be_finished)
              checked.enqueue(true)
              discard.call
            end
          end
        end
        handler = lambda do |reader, context|
          context.send_headers(metadata: { "x-started" => "yes" })
          reader.receive
        end
        with_host(target({ cs.path => handler }), secure) do |_host, peer, _|
          peer.write(request_head(cs, :chunked, extra: "connect-timeout-ms: 20\r\n"))
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include('"code":"deadline_exceeded"')
          expect(checked.dequeue).to be(true)
        end
      end
    end
  end

  it "does not synthesize Context cancellation from actual H1 peer upload EOF" do
    reactor do
      _, cs, = methods
      entered = Async::Queue.new
      release = Async::Queue.new
      observed = Async::Queue.new
      handler = lambda do |reader, context|
        context.send_headers(metadata: { "x-started" => "yes" })
        entered.enqueue(true)
        release.dequeue
        begin
          reader.receive
        ensure
          observed.enqueue(context.cancellation_reason)
        end
      end
      with_host(target({ cs.path => handler }), false) do |_host, peer, _|
        peer.write(request_head(cs, :fixed))
        expect(peer.head.first).to eq(200)
        entered.dequeue
        peer.raw.shutdown(Socket::SHUT_WR)
        release.enqueue(true)
        expect(observed.dequeue).to be_nil
      ensure
        release.enqueue(true)
      end
    end
  end

  it "isolates a real blocked Connect writer from unrelated healthy H1 requests" do
    reactor do |task|
      ss, = methods
      handler = lambda do |message, writer, _|
        count = message.name == "blocked" ? 4096 : 1
        count.times { writer.send_message(AsyncHelpers::MESSAGE.new(name: "x" * 16_000)) }
      end
      with_host(target({ ss.path => handler }), false) do |host, peer, origin|
        peer.raw.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 4096)
        bytes = frame("blocked")
        peer.write(request_head(ss, :fixed, length: bytes.bytesize) + bytes)
        expect(peer.head.first).to eq(200)
        expect(peer.chunk).not_to be_empty
        session = host.instance_variable_get(:@session)
        resource = session.instance_variable_get(:@resources).first
        writing = false
        resource.raw.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF, 4096)
        allow(resource.socket).to receive(:write).and_wrap_original do |write, *args|
          writing = true
          begin
            write.call(*args)
          ensure
            writing = false
          end
        end
        # This flag is visible only when the actual socket write has yielded.
        # Synchronous writes cannot expose their transient flag to this task.
        wait_until(task, "actual socket writer suspended") { writing }
        other = ConnectH1Wire.new(origin)
        bytes = frame("healthy")
        other.write(request_head(ss, :fixed, length: bytes.bytesize) + bytes)
        expect(other.head.first).to eq(200)
        expect(other.body).to include("x" * 16_000)
        expect(host.close(grace: 0.02)).to be(false)
        expect(session.instance_variable_get(:@admitted)).to be_empty
      ensure
        other&.close
      end
    end
  end
end
