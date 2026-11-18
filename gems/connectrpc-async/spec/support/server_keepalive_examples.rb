# frozen_string_literal: true

require "English"
require_relative "connection_peer"
require_relative "frame_gate"
require_relative "keepalive_client"
require_relative "write_probe"

# Server keepalive, PING enforcement and graceful idle/age drains over a real
# server connection. The including group defines `keepalive_host(**options)`,
# returning a started host with `port` and `close`, and
# `keepalive_session(host)`. Every server timer runs on an injected FakeClock.
RSpec.shared_examples "HTTP/2 server keepalive" do
  include KeepaliveClient

  def echo_path = AsyncHelpers::ECHO::ECHO.path
  def encoded(name) = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: name))
  def server_keepalive(**) = ConnectRPC::Async::ServerKeepalive.new(**)

  def watch_method
    @watch_method ||= ConnectRPC::ServerStreamMethod.new(service: "test.Keepalive", name: "Watch",
                                                         request_type: AsyncHelpers::MESSAGE,
                                                         response_type: AsyncHelpers::MESSAGE)
  end

  def collect_method
    @collect_method ||= ConnectRPC::ClientStreamMethod.new(service: "test.Keepalive", name: "Collect",
                                                           request_type: AsyncHelpers::MESSAGE,
                                                           response_type: AsyncHelpers::MESSAGE)
  end

  # Unary calls are logged and those named hold* wait for their release.
  # Watch streams every name the example feeds until it feeds nil. Collect
  # logs its start and joins every name it receives. Every completion is
  # logged. Connect and gRPC are served.
  def keepalive_server(log)
    echo = lambda do |message, _context|
      log[:invoked] << message.name
      log[:holds][message.name].dequeue if message.name.start_with?("hold")
      message
    ensure
      log[:finished] << [message.name, $ERROR_INFO]
    end
    watch = lambda do |message, output, _context|
      while (name = log[:feeds][message.name].dequeue)
        output.send_message(AsyncHelpers::MESSAGE.new(name: name))
      end
    end
    collect = lambda do |reader, _context|
      log[:invoked] << "collect"
      names = []
      while (message = reader.receive)
        names << message.name
      end
      AsyncHelpers::MESSAGE.new(name: names.join(","))
    end
    handlers = AsyncHelpers::ECHO.handlers(echo: echo, fail: ->(message, _context) { message })
    services = [AsyncHelpers::ECHO::SERVICE,
                ConnectRPC::Service.new(name: "test.Keepalive", methods: [watch_method, collect_method])]
    ConnectRPC::Server.new(services: services, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                           handlers: handlers.merge(watch_method.path => watch, collect_method.path => collect),
                           protocol: %i[connect grpc], instrumentation: completion_log(log[:completions]))
  end

  def completion_log(completions)
    observer = Object.new
    observer.define_singleton_method(:finished) { |completion| completions << completion }
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_call| observer }
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end

  def keepalive_log
    queues = -> { Hash.new { |hash, key| hash[key] = Async::Queue.new } }
    { invoked: [], finished: [], completions: [], holds: queues.call, feeds: queues.call }
  end

  def with_keepalive_host(task, keepalive:, idle_timeout: nil, tls: false, client_options: {}, **)
    log = keepalive_log
    host = keepalive_host(server: keepalive_server(log), idle_timeout: idle_timeout, keepalive: keepalive,
                          tls: tls ? host_tls : nil, **)
    session = keepalive_session(host)
    clock = ConnectionPeer::FakeClock.new
    session.instance_variable_set(:@clock, clock)
    session.instance_variable_set(:@random, ConnectionPeer::SequenceRandom.new(0.5))
    client = keepalive_client(task, host.port, tls: tls ? client_tls : nil, **client_options)
    resource = nil
    wait_until(task, "server connection upkeep") { resource = served_resource(session) }
    yield KeepaliveClient::Fixture.new(host, clock, client, resource, log)
  ensure
    log[:holds].each_value { |queue| queue.enqueue(true) }
    log[:feeds].each_value { |queue| queue.enqueue(nil) }
    close_keepalive_client(client)
    host&.close(grace: 0)
  end

  # The reader starts once the upkeep's timers have started.
  def served_resource(session)
    resource = session.instance_variable_get(:@resources).first
    resource if resource&.upkeep && resource.connection&.reader_task
  end

  def unary(fixture, name)
    response = fixture.client.connection.call(connect_request(echo_path, name))
    [response.status, response.read]
  end

  # Starts a Watch stream and returns its response after its first message.
  def watch(fixture, name, first)
    fixture.log[:feeds][name].enqueue(first)
    request = connect_request(watch_method.path, name, content_type: "application/connect+proto")
    response = fixture.client.connection.call(request)
    read_until(response, first)
    response
  end

  def read_until(response, text)
    received = +"".b
    received << response.body.read until received.include?(text)
    received
  end

  def hold_frames(resource, frame_type, **)
    gate = FrameGate.new(resource.socket, frame_type: frame_type, **)
    resource.connection.stream.instance_variable_set(:@io, gate)
    gate.armed = true
    gate
  end

  # Holds one control frame's write once the control writer holds the frame
  # guard, before the frame reaches the buffered stream. A frame held inside
  # the stream would also hold the reader at its next buffer refill, which
  # flushes pending output first, so the reader could not see the peer leave.
  def hold_control_frame(resource, frame_type, stream_id:)
    gate = Data.define(:started, :release).new(Async::Queue.new, Async::Queue.new)
    armed = true
    resource.connection.define_singleton_method(:write_frame) do |frame|
      if armed && frame.type == frame_type && frame.stream_id == stream_id
        armed = false
        gate.started.enqueue(true)
        gate.release.dequeue
      end
      super(frame)
    end
    gate
  end

  def retired?(resource) = resource.send(:retirement_completed?)

  def client_tls
    context = OpenSSL::SSL::SSLContext.new
    context.alpn_protocols = ["h2"]
    context.cert_store = OpenSSL::X509::Store.new.tap { |store| store.add_cert(tls_material.fetch(:root).first) }
    context.verify_mode = OpenSSL::SSL::VERIFY_PEER
    context
  end

  describe "PING enforcement" do
    # grpc-go clients ping at most every 10 s; one-way delay jitter can make
    # an idle connection's gaps arrive shorter, which must never strike.
    it "answers jittered PINGs at grpc-go's 10 s minimum with the defaults, with and without calls" do
      gaps = [10, 5.5, 10, 5.5, 10]
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive) do |fixture|
          client = fixture.client
          5.times do |index|
            fixture.clock.advance(gaps[index - 1]) unless index.zero?
            ping_through(task, client, format("idle%04d", index))
          end
          held = task.async { unary(fixture, "hold-call") }
          wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-call"] }
          5.times do |index|
            fixture.clock.advance(gaps[index])
            ping_through(task, client, format("busy%04d", index))
          end
          expect(client.wire.goaways).to eq([])
          fixture.log[:holds]["hold-call"].enqueue(true)
          expect(held.wait).to eq([200, encoded("hold-call")])
          expect(fixture.resource).not_to be_finished
        end
      end
    end

    it "ends the connection with too_many_pings on the third strike" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          payloads = Array.new(4) { |index| format("strike%02d", index) }
          payloads.each { |payload| send_ping(client, payload) }
          wait_until(task, "server half-close") { client.eof? }
          expect(client.wire.sequence).to eq(
            [*payloads.map { |payload| [:ack, payload] },
             [:goaway, 0, KeepaliveClient::ENHANCE_YOUR_CALM, "too_many_pings"]]
          )
          wait_until(task, "connection retired") { retired?(fixture.resource) }
        end
      end
    end

    # Under CPU starvation every yield costs the control writer a reactor
    # iteration (#139), so arming and disarming its bounds must never yield.
    # The four PINGs arrive in one write, so the reader queues every ACK and
    # the GOAWAY before the writer runs, and one resume writes them all.
    it "writes the ACKs and too_many_pings GOAWAY of one read in a single control writer resume" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          connection = fixture.resource.connection
          writer = nil
          resumes = nil
          switches = []
          connection.define_singleton_method(:write_frame) do |frame|
            writer ||= Fiber.current if frame.type == Protocol::HTTP2::PingFrame::TYPE
            super(frame)
          ensure
            resumes ||= switches.count(writer) if frame.type == Protocol::HTTP2::GoawayFrame::TYPE
          end
          payloads = Array.new(4) { |index| format("strike%02d", index) }
          trace = TracePoint.new(:fiber_switch) { switches << Fiber.current }
          trace.enable do
            client.connection.write_frames do |framer|
              payloads.each do |payload|
                frame = Protocol::HTTP2::PingFrame.new
                frame.pack(payload)
                framer.write_frame(frame)
              end
            end
            wait_until(task, "too_many_pings GOAWAY written") { resumes }
          end
          expect(resumes).to eq(1)
          wait_until(task, "server half-close") { client.eof? }
          expect(client.wire.sequence).to eq(
            [*payloads.map { |payload| [:ack, payload] },
             [:goaway, 0, KeepaliveClient::ENHANCE_YOUR_CALM, "too_many_pings"]]
          )
        end
      end
    end

    it "forgives a PING after a server DATA write and names the cutoff in its GOAWAY" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          before = Array.new(3) { |index| format("before%02d", index) }
          before.each { |payload| ping_through(task, client, payload) }
          expect(unary(fixture, "data")).to eq([200, encoded("data")])
          after = Array.new(4) { |index| format("after%03d", index) }
          after.each { |payload| send_ping(client, payload) }
          wait_until(task, "server half-close") { client.eof? }
          expect(client.wire.sequence).to eq(
            [*(before + after).map { |payload| [:ack, payload] },
             [:goaway, 1, KeepaliveClient::ENHANCE_YOUR_CALM, "too_many_pings"]]
          )
          wait_until(task, "connection retired") { retired?(fixture.resource) }
        end
      end
    end

    [[false, false, true], [true, false, false], [false, true, false]].each do |permit, call, violation|
      it "#{violation ? 'strikes' : 'accepts'} PINGs 100 s apart with permit_without_calls: #{permit} " \
         "and #{call ? 'a call' : 'no call'}" do
        reactor do |task|
          with_keepalive_host(task, keepalive: server_keepalive(time: nil, permit_without_calls: permit)) do |fixture|
            client = fixture.client
            held = task.async { unary(fixture, "hold-call") } if call
            wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-call"] } if call
            4.times do |index|
              fixture.clock.advance(100) unless index.zero?
              ping_through(task, client, format("slow%04d", index))
            end
            if violation
              wait_until(task, "too_many_pings GOAWAY") { client.wire.goaways.any? }
              expect(client.wire.goaways).to eq([[0, KeepaliveClient::ENHANCE_YOUR_CALM, "too_many_pings"]])
            else
              # The control writer is FIFO: a GOAWAY would precede this ACK.
              ping_through(task, client, "slow0004")
              expect(client.wire.goaways).to eq([])
            end
            fixture.log[:holds]["hold-call"].enqueue(true) if call
            expect(held.wait).to eq([200, encoded("hold-call")]) if call && !violation
          end
        end
      end
    end

    # The reader never writes the ACK itself. The PING and the later request
    # reach the socket before the server reads, so one buffer refill delivers
    # both; a further refill would flush, and stall, behind the held write.
    it "applies frames already read while an outbound frame is held and ACKs once it finishes" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          response = watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          send_ping(client, "held-001")
          probe = task.async { unary(fixture, "probe") }
          wait_until(task, "a later request dispatched behind the held frame") do
            fixture.log[:invoked] == ["probe"]
          end
          expect(client.wire.acks).to eq([])
          gate.release.enqueue(true)
          wait_until(task, "ACK after the held frame") { client.wire.acks == ["held-001"] }
          read_until(response, "two")
          expect(probe.wait).to eq([200, encoded("probe")])
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    it "fails the connection when queued control frames outrun a held writer with enforcement off" do
      stub_const("ConnectRPC::Async::Internal::ControlWriter::MAX_PENDING", 8)
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil)) do |fixture|
          client = fixture.client
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          # One write, so the reader applies every PING from a single refill.
          client.connection.write_frames do |framer|
            12.times do |index|
              frame = Protocol::HTTP2::PingFrame.new
              frame.pack(format("flood%03d", index))
              framer.write_frame(frame)
            end
          end
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(client.wire.acks).to eq([])
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    it "aborts within the cleanup allowance when the too_many_pings GOAWAY is stuck behind a held frame" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          4.times { |index| send_ping(client, format("flood%03d", index)) }
          # The reader decided the third strike although no ACK could be written.
          await_due(task, fixture.clock, 0.1)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(0.1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          wait_until(task, "client EOF") { client.eof? }
          expect([client.wire.acks, client.wire.goaways]).to eq([[], []])
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end
  end

  describe "local close" do
    # The session's final GOAWAY closes the connection before its reader
    # stops. A PING the reader already read is applied in that window. The
    # held reader cannot read the drain PING's ACK, so the 5 s fallback
    # writes the cutoff.
    it "ignores a client PING applied after the final GOAWAY instead of sending a second GOAWAY" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          connection = fixture.resource.connection
          closed = Async::Promise.new
          held = Async::Promise.new
          connection.define_singleton_method(:send_goaway) do |*arguments|
            super(*arguments)
          ensure
            closed.resolve(true)
          end
          connection.define_singleton_method(:receive_ping) do |frame|
            if frame.unpack == "late-001"
              held.resolve(true)
              Async::Task.current.defer_cancel { closed.wait }
            end
            super(frame)
          end
          send_ping(client, "late-001")
          held.wait
          closing = task.async { fixture.host.close(grace: 1) }
          await_due(task, fixture.clock, 5)
          fixture.clock.advance(5)
          expect(closing.wait).to be(true)
          expect(client.wire.goaways).to eq(
            [[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR, "graceful_stop"],
             [0, KeepaliveClient::NO_ERROR, "graceful_stop"], [0, KeepaliveClient::NO_ERROR, ""]]
          )
          expect(client.wire.acks).to eq([])
        end
      end
    end
  end

  describe "server PINGs" do
    it "aborts a client that never acknowledges, cancelling its handler" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: 100, timeout: 20, min_time: nil)) do |fixture|
          client = fixture.client
          client.wire.ack_pings = false
          held = task.async do
            unary(fixture, "hold-ping")
          rescue StandardError => e
            e
          end
          wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-ping"] }
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          wait_until(task, "server PING") { client.wire.pings == [[1].pack("Q>")] }
          await_due(task, fixture.clock, 120)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(20)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(fixture.log[:finished].map(&:first)).to eq(["hold-ping"])
          expect(fixture.log[:finished].first.last).to be_a(Exception)
          expect(held.wait).to be_a(StandardError)
        end
      end
    end
  end

  describe "graceful drains" do
    it "drains an aged connection: heads-up, dispatch until the cutoff, refusal while a sibling streams" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil,
                                                              max_connection_age: 100)) do |fixture|
          client = fixture.client
          wire = client.wire
          response = watch(fixture, "sibling", "one")
          wire.ack_pings = false
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          wait_until(task, "heads-up GOAWAY and drain PING") { wire.pings == [KeepaliveClient::DRAIN_PING] }
          expect(wire.goaways).to eq([[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR, "max_age"]])
          expect(unary(fixture, "between")).to eq([200, encoded("between")])
          send_ping(client, KeepaliveClient::DRAIN_PING, ack: true)
          wait_until(task, "cutoff GOAWAY") { wire.goaways.size == 2 }
          expect(wire.goaways.last).to eq([3, KeepaliveClient::NO_ERROR, "max_age"])

          # Refused streams still keep HPACK, CONTINUATION and flow control in step.
          fragmented = client.connection.next_stream_id
          padding = Array.new(24) { |index| ["x-pad-#{index}", "p" * 1000] }
          headers = raw_headers(client, fragmented, request_headers(echo_path, *padding, %w[x-indexed marker]))
          expect(headers.continuation).not_to be_nil
          indexed = client.connection.next_stream_id
          raw_headers(client, indexed, request_headers(echo_path, %w[x-indexed marker]))
          wait_until(task, "both streams refused") { wire.resets.keys == [fragmented, indexed] }
          expect(wire.resets.values).to eq([KeepaliveClient::REFUSED_STREAM, KeepaliveClient::REFUSED_STREAM])
          raw_data(client, fragmented, "x" * 1000)
          raw_headers(client, fragmented, [%w[x-trailer late]], end_stream: true)
          raw_reset(client, indexed)

          fixture.log[:feeds]["sibling"].enqueue("two")
          read_until(response, "two")
          fixture.log[:feeds]["sibling"].enqueue(nil)
          nil while response.body.read
          expect(fixture.log[:invoked]).to eq(["between"])
          wait_until(task, "server half-close") { client.eof? }
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(wire.goaways.size).to eq(2)
        end
      end
    end

    it "cuts off after 5 s without the drain ACK and aborts when the grace from age expiry ends" do
      reactor do |task|
        keepalive = server_keepalive(time: nil, min_time: nil, max_connection_age: 100, max_connection_age_grace: 50)
        with_keepalive_host(task, keepalive: keepalive) do |fixture|
          client = fixture.client
          client.wire.ack_pings = false
          held = task.async do
            unary(fixture, "hold-age")
          rescue StandardError => e
            e
          end
          wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-age"] }
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          wait_until(task, "drain PING") { client.wire.pings == [KeepaliveClient::DRAIN_PING] }
          # Grace counts from age expiry, not from the cutoff five seconds later.
          await_due(task, fixture.clock, 150)
          await_due(task, fixture.clock, 105)
          expect(client.wire.goaways.size).to eq(1)
          fixture.clock.advance(5)
          wait_until(task, "cutoff GOAWAY") { client.wire.goaways.size == 2 }
          expect(client.wire.goaways.last).to eq([1, KeepaliveClient::NO_ERROR, "max_age"])
          fixture.clock.advance(44)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(fixture.log[:finished].map(&:first)).to eq(["hold-age"])
          expect(fixture.log[:finished].first.last).to be_a(Exception)
          expect(held.wait).to be_a(StandardError)
        end
      end
    end

    it "aborts a drain whose GOAWAY write stalls" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil,
                                                              max_connection_age: 100)) do |fixture|
          gate = hold_frames(fixture.resource, Protocol::HTTP2::GoawayFrame::TYPE)
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          gate.started.dequeue
          await_due(task, fixture.clock, 100.1)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(0.1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    it "bounds the wait for EOF after the half-close" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil,
                                                              max_connection_age: 100)) do |fixture|
          # A second descriptor keeps the socket open after the client closes.
          keeper = fixture.client.raw.dup
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          wait_until(task, "server half-close") { fixture.client.eof? }
          await_due(task, fixture.clock, 101)
          expect(retired?(fixture.resource)).to be(false)
          fixture.clock.advance(1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(fixture.client.wire.goaways.map(&:first)).to eq([KeepaliveClient::MAX_STREAM_ID, 0])
        ensure
          keeper&.close
        end
      end
    end

    # Retirement shuts raw TCP down before it closes it, so its own failure
    # also ends the discard with EOF.
    it "reports a drain joined in its final discard as not drained when its bound aborts it" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil,
                                                              max_connection_age: 100)) do |fixture|
          # A second descriptor keeps the socket open after the client closes.
          keeper = fixture.client.raw.dup
          resource = fixture.resource
          raw = resource.raw
          # After its EOF, the discard read stays on raw until retirement has
          # joined every member and waits on the resource, or until a close
          # reaches raw. No close may reach raw while the read is on it.
          discarding_at_close = []
          gate = nil
          allow(raw).to receive(:readpartial).and_wrap_original do |read, *args|
            read.call(*args)
          rescue EOFError
            members = resource.instance_variable_get(:@retirement_members)
            wait_until(Async::Task.current, "retirement joined its members") do
              gate = :closed unless discarding_at_close.empty?
              gate ||= :joined if !members.empty? && members.keys.all?(&:finished?) && resource.progress.waiting?
              gate
            end
            raise
          end
          allow(raw).to receive(:close).and_wrap_original do |close|
            discarding_at_close << resource.instance_variable_get(:@discarding)
            close.call
          end
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          wait_until(task, "server half-close") { fixture.client.eof? }
          closing = task.async { fixture.host.close(grace: 5) }
          await_due(task, fixture.clock, 101)
          fixture.clock.advance(1)
          expect(closing.wait).to be(false)
          expect(retired?(resource)).to be(true)
          expect(raw).to be_closed
          expect(gate).to eq(:joined)
          expect(discarding_at_close).not_to be_empty
          expect(discarding_at_close).to all(be(false))
        ensure
          keeper&.close
        end
      end
    end

    it "reports a session drain in its final discard as not drained when the connection aborts" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil)) do |fixture|
          # A second descriptor keeps the socket open after the client closes.
          keeper = fixture.client.raw.dup
          resource = fixture.resource
          closing = task.async { fixture.host.close(grace: 5) }
          wait_until(task, "final discard") { resource.instance_variable_get(:@discarding) }
          # The connection's own raw failure ends the discard with EOF; it is
          # not the peer's close.
          resource.abort
          expect(closing.wait).to be(false)
          expect(retired?(resource)).to be(true)
          expect(resource.raw).to be_closed
        ensure
          keeper&.close
        end
      end
    end

    # Session retirement shuts raw TCP down before it aborts each resource, so
    # a draining reader can see EOF first. That EOF is not the peer leaving.
    it "does not read a session abort's raw failure in a drain as the peer leaving" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil)) do |fixture|
          client = fixture.client
          resource = fixture.resource
          session = keepalive_session(fixture.host)
          client.wire.ack_pings = false
          closing = task.async { fixture.host.close(grace: 5) }
          wait_until(task, "drain PING") { client.wire.pings == [KeepaliveClient::DRAIN_PING] }
          held = Async::Queue.new
          release = Async::Queue.new
          allow(session).to receive(:stop_accepting).and_wrap_original do |stop|
            held.enqueue(true)
            release.dequeue
            stop.call
          end
          aborter = task.async { session.abort }
          held.dequeue
          wait_until(task, "server reader saw EOF") { resource.connection.reader_task.finished? }
          expect(resource.peer_departed?).to be(false)
          expect(resource).to be_finished
          release.enqueue(true)
          aborter.wait
          expect(resource.instance_variable_get(:@drain_task).wait).to be(false)
          expect(closing.wait).to be(false)
          expect(retired?(resource)).to be(true)
        ensure
          release&.enqueue(true)
        end
      end
    end

    it "lets session close supersede a drain without raising its cutoff" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil,
                                                              max_connection_age: 100)) do |fixture|
          client = fixture.client
          response = watch(fixture, "sibling", "one")
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          wait_until(task, "cutoff GOAWAY") { client.wire.goaways.size == 2 }
          refused = client.connection.next_stream_id
          raw_headers(client, refused, request_headers(echo_path), end_stream: true)
          wait_until(task, "refusal") { client.wire.resets[refused] == KeepaliveClient::REFUSED_STREAM }
          closing = task.async { fixture.host.close(grace: 5) }
          fixture.log[:feeds]["sibling"].enqueue(nil)
          nil while response.body.read
          expect(closing.wait).to be(true)
          expect(client.wire.goaways.map(&:first)).to eq([KeepaliveClient::MAX_STREAM_ID, 1, 1])
        end
      end
    end

    # A guard holder that writes no frame, such as a HEAD response closing
    # its body, is not bounded by the frame write watchdog.
    it "aborts within the keepalive timeout when a drain GOAWAY waits behind a held guard" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil,
                                                              max_connection_age: 100)) do |fixture|
          release = Async::Promise.new
          holding = Async::Promise.new
          holder = task.async do
            fixture.resource.connection.synchronize do
              holding.resolve(true)
              release.wait
            end
          end
          holding.wait
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          # Without server PINGs or age grace, only the control frame bound,
          # counted from the GOAWAY being queued, ends the connection.
          wait_until(task, "heads-up GOAWAY waits for the frame guard") do
            fixture.resource.connection.guard_waiting?
          end
          await_due(task, fixture.clock, 120)
          fixture.clock.advance(19)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(fixture.client.wire.goaways).to eq([])
        ensure
          release&.resolve(true)
          holder&.wait
        end
      end
    end

    it "starts the drain ACK fallback only once the drain PING is written" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil,
                                                              max_connection_age: 100)) do |fixture|
          client = fixture.client
          client.wire.ack_pings = false
          watch(fixture, "sibling", "one")
          # The held frame starts late enough to finish within its write bound.
          fixture.clock.advance(90)
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(10)
          wait_until(task, "heads-up GOAWAY waits for the frame guard") do
            fixture.resource.connection.guard_waiting?
          end
          # Longer than the ACK fallback, while the heads-up cannot be written.
          fixture.clock.advance(6)
          gate.release.enqueue(true)
          wait_until(task, "drain PING") { client.wire.pings == [KeepaliveClient::DRAIN_PING] }
          await_due(task, fixture.clock, 111)
          expect(client.wire.goaways).to eq([[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR, "max_age"]])
          fixture.clock.advance(5)
          wait_until(task, "cutoff GOAWAY") { client.wire.goaways.size == 2 }
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    it "lets drain_tls supersede a drain without raising its cutoff" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil, max_connection_age: 100),
                                  tls: true) do |fixture|
          client = fixture.client
          response = watch(fixture, "sibling", "one")
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          wait_until(task, "cutoff GOAWAY") { client.wire.goaways.size == 2 }
          refused = client.connection.next_stream_id
          raw_headers(client, refused, request_headers(echo_path), end_stream: true)
          wait_until(task, "refusal") { client.wire.resets[refused] == KeepaliveClient::REFUSED_STREAM }
          fixture.host.reload_tls(**host_tls)
          draining = task.async { fixture.host.drain_tls(grace: 5) }
          wait_until(task, "TLS retirement started") { fixture.resource.draining }
          fixture.log[:feeds]["sibling"].enqueue(nil)
          nil while response.body.read
          expect(draining.wait).to be(true)
          expect(client.wire.goaways.map(&:first)).to eq([KeepaliveClient::MAX_STREAM_ID, 1, 1])
          expect(retired?(fixture.resource)).to be(true)
        end
      end
    end

    it "ends a too_many_pings connection within the EOF bound while drain_tls waits on a held call" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), tls: true) do |fixture|
          client = fixture.client
          response = watch(fixture, "sibling", "one")
          fixture.host.reload_tls(**host_tls)
          draining = task.async { fixture.host.drain_tls(grace: 60) }
          wait_until(task, "TLS retirement started") { fixture.resource.draining }
          4.times { |index| send_ping(client, format("strike%02d", index)) }
          wait_until(task, "too_many_pings GOAWAY") do
            client.wire.goaways.any? { |goaway| goaway.last == "too_many_pings" }
          end
          await_due(task, fixture.clock, 1)
          expect(retired?(fixture.resource)).to be(false)
          fixture.clock.advance(1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(fixture.clock.now).to be < 60
          expect(draining.wait).to be(false)
          response.body&.close
        end
      end
    end

    it "drains an idle connection without keepalive, the reader answering PINGs and the drain ACK" do
      reactor do |task|
        with_keepalive_host(task, keepalive: nil, idle_timeout: 30) do |fixture|
          client = fixture.client
          ping_through(task, client, "nokeep01")
          await_due(task, fixture.clock, 30)
          fixture.clock.advance(30)
          # Only the drain PING's ACK, not the 5 s fallback, can send the cutoff.
          wait_until(task, "idle GOAWAY pair") { client.wire.goaways.size == 2 }
          expect(client.wire.goaways).to eq([[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR, "max_idle"],
                                             [0, KeepaliveClient::NO_ERROR, "max_idle"]])
          expect(client.wire.pings).to eq([KeepaliveClient::DRAIN_PING])
          wait_until(task, "server half-close") { client.eof? }
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(fixture.clock.now).to eq(30)
        end
      end
    end

    it "drains an idle connection measured from its last stream, not from PINGs" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), idle_timeout: 30) do |fixture|
          client = fixture.client
          fixture.clock.advance(10)
          expect(unary(fixture, "work")).to eq([200, encoded("work")])
          await_due(task, fixture.clock, 30)
          fixture.clock.advance(10)
          ping_through(task, client, "idle0020")
          fixture.clock.advance(10)
          ping_through(task, client, "idle0030")
          await_due(task, fixture.clock, 40)
          expect(client.wire.goaways).to eq([])
          fixture.clock.advance(10)
          wait_until(task, "idle GOAWAY pair") { client.wire.goaways.size == 2 }
          expect(client.wire.goaways).to eq([[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR, "max_idle"],
                                             [1, KeepaliveClient::NO_ERROR, "max_idle"]])
          wait_until(task, "server half-close") { client.eof? }
          wait_until(task, "connection retired") { retired?(fixture.resource) }
        end
      end
    end

    # Holds a completed call's stream worker at the start of its post-delivery
    # work, after its stream has closed.
    def hold_post_delivery(fixture)
      held = Async::Queue.new
      release = Async::Queue.new
      allow(fixture.resource.connection).to receive(:finish_request).and_wrap_original do |original, *args, **options|
        held.enqueue(true)
        release.dequeue
      ensure
        original.call(*args, **options)
      end
      expect(unary(fixture, "done")).to eq([200, encoded("done")])
      held.dequeue
      release
    end

    def send_goaway(client)
      frame = Protocol::HTTP2::GoawayFrame.new
      frame.pack(0, KeepaliveClient::NO_ERROR, "")
      client.connection.write_frame(frame)
    end

    it "lets a drain finish post-delivery work when the peer's GOAWAY precedes the cutoff" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          # Without the drain PING's ACK, the cutoff waits for the 5 s fallback
          # on the fake clock, which never advances.
          client.wire.ack_pings = false
          release = hold_post_delivery(fixture)
          closing = task.async { fixture.host.close(grace: 5) }
          # The drain PING is written and the cutoff waits for its ACK.
          await_due(task, fixture.clock, 5)
          send_goaway(client)
          wait_until(task, "server reader ended") { fixture.resource.connection.reader_task.finished? }
          expect(fixture.resource).not_to be_finished
          release.enqueue(true)
          expect(closing.wait).to be(true)
          expect(fixture.log[:completions].map(&:delivery)).to eq([:completed])
          expect(client.wire.goaways).to eq([[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR,
                                              "graceful_stop"]])
          expect(fixture.clock.now).to eq(0)
        end
      end
    end

    # The departure cancels the control writer, but a writer already due to
    # run gets that cancellation only after its next write, which then fails
    # because the peer left. The held write stands in for that ordering by
    # outlasting the cancellation; its failure must not fail the drain.
    it "lets a drain finish when a control frame write fails after the peer left" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          client.wire.ack_pings = false
          connection = fixture.resource.connection
          release = hold_post_delivery(fixture)
          closing = task.async { fixture.host.close(grace: 5) }
          await_due(task, fixture.clock, 5)
          started = Async::Promise.new
          failing = Async::Promise.new
          connection.define_singleton_method(:write_frame) do |frame|
            return super(frame) unless frame.type == Protocol::HTTP2::ResetStreamFrame::TYPE

            started.resolve(true)
            begin
              failing.wait
            rescue Async::Cancel
              retry
            end
            raise Errno::EPIPE
          end
          written = fixture.resource.upkeep.refuse(99)
          started.wait
          send_goaway(client)
          wait_until(task, "server reader ended") { connection.reader_task.finished? }
          failing.resolve(true)
          expect(written.wait).to be(false)
          expect(fixture.resource).not_to be_finished
          release.enqueue(true)
          expect(closing.wait).to be(true)
          expect(fixture.log[:completions].map(&:delivery)).to eq([:completed])
        ensure
          failing&.resolve(true)
          release&.enqueue(true)
        end
      end
    end

    # Writing the final GOAWAY marks the connection finishing while the peer
    # is still there. A control frame write that fails then still fails the
    # connection raw-first, rather than leaving it to the drain's grace. The
    # final GOAWAY is held before it takes the frame guard, and a duplicate of
    # the client's socket keeps the peer from closing after a half-close.
    it "fails the connection raw-first when a control frame write fails while a live peer awaits the final GOAWAY" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          kept = fixture.client.raw.dup
          resource = fixture.resource
          connection = resource.connection
          finishing = Async::Promise.new
          final = Async::Promise.new
          connection.define_singleton_method(:send_goaway) do |*arguments|
            finishing.resolve(true)
            final.wait
            super(*arguments)
          end
          release = hold_post_delivery(fixture)
          closing = task.async { fixture.host.close(grace: 5) }
          wait_until(task, "cutoff GOAWAY") { fixture.client.wire.goaways.size == 2 }
          release.enqueue(true)
          finishing.wait
          expect(resource).to be_finishing
          connection.define_singleton_method(:write_frame) do |frame|
            raise Errno::EPIPE if frame.type == Protocol::HTTP2::ResetStreamFrame::TYPE

            super(frame)
          end
          written = resource.upkeep.refuse(99)
          expect(written.wait).to be(false)
          wait_until(task, "connection retired") { retired?(resource) }
          expect(fixture.client.wire.goaways.size).to eq(2)
          closing.wait
        ensure
          final&.resolve(true)
          release&.enqueue(true)
          kept&.close
        end
      end
    end

    # Parks the server reader before its next frame, so it cannot see the
    # peer leave until resumed. The client's WINDOW_UPDATE ends the read in
    # progress without any server reply.
    def park_reader(fixture)
      parked = Async::Promise.new
      resume = Async::Promise.new
      parking = true
      fixture.resource.connection.define_singleton_method(:read_frame) do
        if parking
          parking = false
          parked.resolve(true)
          resume.wait
        end
        super()
      end
      raw_window_update(fixture.client, 0, 1)
      parked.wait
      resume
    end

    # Resets the client's TCP connection and waits until the server socket
    # has the reset, without reading from it. Polling SO_ERROR consumes the
    # pending ECONNRESET, so the next server write fails with EPIPE.
    def reset_client(task, fixture)
      fixture.client.raw.setsockopt(Socket::Option.linger(true, 0))
      AsyncHelpers::INTERNAL.raw_close(fixture.client.raw)
      raw = fixture.resource.raw
      wait_until(task, "server socket reset") do
        raw.getsockopt(Socket::SOL_SOCKET, Socket::SO_ERROR).int == Errno::ECONNRESET::Errno
      end
    end

    # The peer leaves once its last stream closed, and the cutoff GOAWAY's
    # write finds the reset before the reader does. The writer leaves the
    # connection to the reader, which sees the peer leave once resumed.
    [false, true].each do |tls|
      it "lets a drain finish when a control frame write sees the peer leave before the reader " \
         "(#{tls ? 'TLS' : 'h2c'})" do
        reactor do |task|
          with_keepalive_host(task, keepalive: server_keepalive(time: nil), tls: tls) do |fixture|
            client = fixture.client
            client.wire.ack_pings = false
            resource = fixture.resource
            connection = resource.connection
            release = hold_post_delivery(fixture)
            closing = task.async { fixture.host.close(grace: 5) }
            # The drain PING is written and the cutoff waits for its 5 s fallback.
            await_due(task, fixture.clock, 5)
            resume = park_reader(fixture)
            failures = []
            connection.define_singleton_method(:write_frame) do |frame|
              super(frame)
            rescue StandardError => e
              failures << e if frame.type == Protocol::HTTP2::GoawayFrame::TYPE
              raise
            end
            reset_client(task, fixture)
            fixture.clock.advance(5)
            expect(resource.upkeep.drain("graceful_stop").wait).to be(false)
            expect(failures).to contain_exactly(an_instance_of(Errno::EPIPE))
            expect(resource).not_to be_finished
            resume.resolve(true)
            wait_until(task, "server reader ended") { connection.reader_task.finished? }
            expect(resource).not_to be_finished
            expect(resource.peer_departed?).to be(true)
            release.enqueue(true)
            expect(closing.wait).to be(true)
            expect(fixture.log[:completions].map(&:delivery)).to eq([:completed])
            expect(client.wire.goaways).to eq([[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR,
                                                "graceful_stop"]])
          ensure
            resume&.resolve(true)
            release&.enqueue(true)
          end
        end
      end
    end

    # Data the peer sent before its reset stays readable after a write sees
    # the reset. The stream the reader then opens is still open when the
    # reader sees the peer leave, so the drain fails, as it does when the
    # reader sees the peer leave first.
    it "fails a drain when the reader opens a stream after a control frame write saw the peer leave" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          client.wire.ack_pings = false
          resource = fixture.resource
          closing = task.async { fixture.host.close(grace: 5) }
          await_due(task, fixture.clock, 5)
          resume = park_reader(fixture)
          late = client.connection.next_stream_id
          raw_headers(client, late, request_headers(echo_path))
          raw_data(client, late, encoded("hold-late"), end_stream: true)
          reset_client(task, fixture)
          expect(resource.upkeep.refuse(99).wait).to be(false)
          expect(resource).not_to be_finished
          resume.resolve(true)
          expect(closing.wait).to be(false)
          expect(resource.connection.remote_stream_id).to eq(late)
          expect(resource.peer_departed?).to be(false)
          expect(retired?(resource)).to be(true)
        ensure
          resume&.resolve(true)
        end
      end
    end

    # A writer that stopped for a peer that left resolves later control
    # frames at once, so nothing waits on a frame it never writes, such as a
    # HEADERS refusal's reset. A too_many_pings decision then still ends the
    # connection at its own bound. Held post-delivery work keeps the drain
    # from ending the connection first.
    it "resolves control frames queued after the writer stopped for a peer that left, keeping the " \
       "too_many_pings bound" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          fixture.client.wire.ack_pings = false
          resource = fixture.resource
          cleanup = AsyncHelpers::INTERNAL::CLEANUP_SECONDS
          release = hold_post_delivery(fixture)
          closing = task.async { fixture.host.close(grace: 5) }
          await_due(task, fixture.clock, 5)
          resume = park_reader(fixture)
          reset_client(task, fixture)
          expect(resource.upkeep.refuse(99).wait).to be(false)
          later = resource.upkeep.refuse(101)
          expect(later).to be_resolved
          expect(later.value).to be(false)
          resource.upkeep.send(:violate)
          await_due(task, fixture.clock, cleanup)
          expect(resource).not_to be_finished
          fixture.clock.advance(cleanup)
          wait_until(task, "too_many_pings bound") { resource.finished? }
          expect(closing.wait).to be(false)
        ensure
          resume&.resolve(true)
          release&.enqueue(true)
        end
      end
    end

    it "keeps a too_many_pings decision ahead of a peer that leaves while a drain waits for its work" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          release = hold_post_delivery(fixture)
          closing = task.async { fixture.host.close(grace: 5) }
          wait_until(task, "cutoff GOAWAY") { client.wire.goaways.size == 2 }
          4.times { |index| send_ping(client, format("strike%02d", index)) }
          # The last strike's ACK shows the reader decided the violation. Its
          # 1 s EOF bound runs on the fake clock, which never advances.
          wait_until(task, "last strike ACK") { client.wire.acks.include?("strike03") }
          AsyncHelpers::INTERNAL.raw_close(client.raw)
          wait_until(task, "server reader ended") { fixture.resource.connection.reader_task.finished? }
          release.enqueue(true)
          expect(closing.wait).to be(false)
          expect(fixture.resource).to be_finished
        end
      end
    end
  end

  describe "stream admission" do
    # One connection holding body-less streams up to the host-wide cap used
    # to leave every other connection with 503.
    it "refuses streams over the per-connection limit and keeps other connections served" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          ids = Array.new(128) do
            client.connection.next_stream_id.tap { |id| raw_headers(client, id, request_headers(echo_path)) }
          end
          ping_through(task, client, "seen0128")
          # Body-less streams invoke no handler; host admission shows their dispatch.
          admitted = keepalive_session(fixture.host).instance_variable_get(:@admitted)
          wait_until(task, "every held stream dispatched or refused; admitted #{admitted.size}") do
            admitted.size + client.wire.resets.size == 128
          end
          other = keepalive_client(task, fixture.host.port)
          response = other.connection.call(connect_request(echo_path, "other"))
          expect([response.status, response.read]).to eq([200, encoded("other")])
          wait_until(task, "over-limit refusals; resets #{client.wire.resets.size}") do
            client.wire.resets.size == 28
          end
          expect(client.wire.resets).to eq(ids.drop(100).to_h { |id| [id, KeepaliveClient::REFUSED_STREAM] })
          expect(client.wire.goaways).to eq([])
          expect(client.wire.settings.first).to include([Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS, 100])
        ensure
          close_keepalive_client(other)
        end
      end
    end

    it "advertises max_concurrent_streams and enforces it before the client acknowledges SETTINGS" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_concurrent_streams: 2) do |fixture|
          early = keepalive_client(task, fixture.host.port, ack_settings: false)
          expect(early.wire.settings.first).to include([Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS, 2])
          ids = Array.new(3) do
            early.connection.next_stream_id.tap { |id| raw_headers(early, id, request_headers(echo_path)) }
          end
          ping_through(task, early, "unacked1")
          wait_until(task, "refusal; resets #{early.wire.resets}") { early.wire.resets.any? }
          expect(early.wire.resets).to eq(ids.last => KeepaliveClient::REFUSED_STREAM)
          expect(early.wire.goaways).to eq([])
        ensure
          close_keepalive_client(early)
        end
      end
    end

    # Clients then queue streams the host could never admit, rather than
    # sending them to be refused.
    it "advertises max_requests when it is below max_concurrent_streams" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 3) do |fixture|
          expect(fixture.client.wire.settings.first)
            .to include([Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS, 3])
        end
      end
    end

    # Refused streams keep HPACK, CONTINUATION and flow control in step, and
    # their later frames never fail the connection or its open sibling.
    it "decodes and drops fragmented, indexed and late frames of streams over the limit beside a healthy sibling" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_concurrent_streams: 1) do |fixture|
          client = fixture.client
          wire = client.wire
          response = watch(fixture, "sibling", "one")
          fragmented = client.connection.next_stream_id
          padding = Array.new(24) { |index| ["x-pad-#{index}", "p" * 1000] }
          headers = raw_headers(client, fragmented, request_headers(echo_path, *padding, %w[x-indexed marker]))
          expect(headers.continuation).not_to be_nil
          indexed = client.connection.next_stream_id
          raw_headers(client, indexed, request_headers(echo_path, %w[x-indexed marker]))
          wait_until(task, "both streams refused") { wire.resets.keys == [fragmented, indexed] }
          expect(wire.resets.values).to eq([KeepaliveClient::REFUSED_STREAM, KeepaliveClient::REFUSED_STREAM])
          raw_data(client, fragmented, "x" * 1000)
          raw_headers(client, fragmented, [%w[x-trailer late]], end_stream: true)
          raw_reset(client, indexed)
          ping_through(task, client, "lateok01")

          fixture.log[:feeds]["sibling"].enqueue("two")
          read_until(response, "two")
          fixture.log[:feeds]["sibling"].enqueue(nil)
          nil while response.body.read
          wait_until(task, "sibling closed") { fixture.resource.connection.streams.empty? }
          expect(unary(fixture, "after")).to eq([200, encoded("after")])
          expect(wire.goaways).to eq([])
          expect(fixture.log[:invoked]).to eq(["after"])
        end
      end
    end

    it "refuses a stream with REFUSED_STREAM instead of 503 when host admission is full" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 1) do |fixture|
          held = task.async { unary(fixture, "hold-admission") }
          wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-admission"] }
          other = keepalive_client(task, fixture.host.port)
          refused = other.connection.next_stream_id
          raw_headers(other, refused, request_headers(echo_path), end_stream: true)
          wait_until(task, "refusal; resets #{other.wire.resets}") do
            other.wire.resets[refused] == KeepaliveClient::REFUSED_STREAM
          end
          fixture.log[:holds]["hold-admission"].enqueue(true)
          expect(held.wait).to eq([200, encoded("hold-admission")])
          expect(fixture.log[:invoked]).to eq(["hold-admission"])
        ensure
          close_keepalive_client(other)
        end
      end
    end

    # Holds the session's lifecycle factory for requests that carry x-gate,
    # recording their stream ids, until the example resolves `release`.
    def gate_lifecycle(fixture)
      session = keepalive_session(fixture.host)
      factory = session.instance_variable_get(:@lifecycle_factory)
      gate = { held: [], release: Async::Promise.new }
      session.instance_variable_set(:@lifecycle_factory, lambda do |request|
        if request.headers["x-gate"]
          gate[:held] << request.stream.id
          gate[:release].wait
        end
        factory&.call(request)
      end)
      gate
    end

    def release_gate(gate)
      gate[:release].resolve(true) if gate && !gate[:release].resolved?
    end

    # Records each call of the session's rejection hook, which Falcon counts
    # in requests_total, before calling it.
    def count_rejections(fixture)
      session = keepalive_session(fixture.host)
      hook = session.instance_variable_get(:@on_request_rejected)
      rejections = []
      session.instance_variable_set(:@on_request_rejected, lambda do
        rejections << true
        hook&.call
      end)
      rejections
    end

    # The served resource of the connection after the fixture's own.
    def second_resource(task, fixture)
      resources = keepalive_session(fixture.host).instance_variable_get(:@resources)
      resource = nil
      wait_until(task, "second server connection") do
        resource = resources.find do |candidate|
          !candidate.equal?(fixture.resource) && candidate.upkeep && candidate.connection&.reader_task
        end
      end
      resource
    end

    # Counts the DATA bytes each later stream of `connection` takes as body.
    def body_bytes(connection)
      received = Hash.new(0)
      spy = Module.new do
        define_method(:process_data) do |frame|
          received[id] += frame.length
          super(frame)
        end
      end
      connection.define_singleton_method(:create_stream) do |*arguments, &block|
        super(*arguments, &block).tap { |stream| stream.extend(spy) }
      end
      received
    end

    # Writes whole requests, each HEADERS followed by its DATA, in one flush.
    def raw_requests(client, requests)
      connection = client.connection
      connection.write_frames do |framer|
        requests.each do |stream_id, headers, body|
          frame = Protocol::HTTP2::HeadersFrame.new(stream_id)
          frame.pack(connection.encode_headers(headers), maximum_size: connection.maximum_frame_size)
          framer.write_frame(frame)
          chunks = body.b.scan(/.{1,16384}/m)
          chunks.each_with_index do |chunk, index|
            last = index == chunks.size - 1
            data = Protocol::HTTP2::DataFrame.new(stream_id, last ? Protocol::HTTP2::END_STREAM : 0)
            data.pack(chunk)
            framer.write_frame(data)
          end
        end
      end
    end

    def rejected_completions(fixture)
      fixture.log[:completions].select { |completion| completion.decision == :admission_rejected }
    end

    # The refusal is decided while the HEADERS are read: it reaches the wire
    # while the lifecycle factory for the stream is still held, so dispatch
    # never saw it, and the reader takes none of its DATA as body.
    it "refuses a stream at HEADERS, before reading its DATA, when host admission is full" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 1,
                                  connection_receive_window: 65_535) do |fixture|
          gate = gate_lifecycle(fixture)
          rejections = count_rejections(fixture)
          held = task.async { unary(fixture, "hold-admission") }
          wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-admission"] }
          other = keepalive_client(task, fixture.host.port)
          connection = second_resource(task, fixture).connection
          received = body_bytes(connection)
          refused = other.connection.next_stream_id
          raw_requests(other, [[refused, request_headers(echo_path, %w[x-gate 1]), "x" * 40_000]])
          # The control writer queues the reset before the PING's ACK.
          ping_through(task, other, "refused1")
          expect(other.wire.resets).to eq(refused => KeepaliveClient::REFUSED_STREAM)
          expect(gate[:held]).to eq([refused])
          expect(fixture.log[:completions]).to eq([])
          expect(received).to eq({})
          expect(connection.streams).to be_empty
          # Connection credit for the ignored DATA returns once it crosses
          # half of the 65,535-byte window.
          expect(other.wire.credit[0]).to eq(40_000)

          release_gate(gate)
          wait_until(task, "refusal observed") { fixture.log[:completions].size == 1 }
          completion = fixture.log[:completions].first
          expect([completion.decision, completion.http_status, completion.delivery])
            .to eq([:admission_rejected, nil, :completed])
          expect(rejections.size).to eq(1)
          fixture.log[:holds]["hold-admission"].enqueue(true)
          expect(held.wait).to eq([200, encoded("hold-admission")])
          response = other.connection.call(connect_request(echo_path, "after"))
          expect([response.status, response.read]).to eq([200, encoded("after")])
          expect(fixture.log[:invoked]).to eq(%w[hold-admission after])
          expect(rejected_completions(fixture).size).to eq(1)
          expect(rejections.size).to eq(1)
          expect(other.wire.reset_counts).to eq(refused => 1)
          expect(other.wire.goaways).to eq([])
        ensure
          release_gate(gate)
          close_keepalive_client(other)
        end
      end
    end

    # HEADERS read together reserve slots in order, so one free slot admits
    # exactly the first stream before any of them dispatches.
    it "admits one stream of a HEADERS burst into one free slot and refuses the rest at HEADERS" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 3) do |fixture|
          gate = gate_lifecycle(fixture)
          rejections = count_rejections(fixture)
          held = %w[hold-a hold-b]
          holds = held.map { |name| task.async { unary(fixture, name) } }
          wait_until(task, "held calls invoked") { fixture.log[:invoked].sort == held }
          other = keepalive_client(task, fixture.host.port)
          received = body_bytes(second_resource(task, fixture).connection)
          ids = Array.new(3) { other.connection.next_stream_id }
          names = %w[burst1 burst2 burst3]
          raw_requests(other, ids.zip(names).map do |id, name|
            [id, request_headers(echo_path, %w[x-gate 1]), encoded(name)]
          end)
          ping_through(task, other, "burst001")
          expect(other.wire.resets).to eq(ids.drop(1).to_h { |id| [id, KeepaliveClient::REFUSED_STREAM] })
          wait_until(task, "every factory held; held #{gate[:held]}") { gate[:held].size == 3 }
          expect(gate[:held]).to match_array(ids)
          expect(fixture.log[:completions]).to eq([])
          expect(received).to eq(ids.first => encoded("burst1").bytesize)

          release_gate(gate)
          wait_until(task, "admitted response DATA") { other.wire.data_bytes[ids.first].positive? }
          wait_until(task, "refusals observed") { rejected_completions(fixture).size == 2 }
          expect(rejected_completions(fixture).map { |completion| [completion.http_status, completion.delivery] })
            .to eq([[nil, :completed]] * 2)
          expect(rejections.size).to eq(2)
          held.each { |name| fixture.log[:holds][name].enqueue(true) }
          expect(holds.map(&:wait)).to eq([[200, encoded("hold-a")], [200, encoded("hold-b")]])
          expect(fixture.log[:invoked]).to match_array(%w[hold-a hold-b burst1])
          expect(other.wire.reset_counts).to eq(ids.drop(1).to_h { |id| [id, 1] })
          expect(other.wire.goaways).to eq([])
        ensure
          release_gate(gate)
          close_keepalive_client(other)
        end
      end
    end

    # Raises from the session's lifecycle factory for requests that carry
    # x-fail, recording their stream ids.
    def fail_lifecycle(fixture)
      session = keepalive_session(fixture.host)
      factory = session.instance_variable_get(:@lifecycle_factory)
      failed = []
      session.instance_variable_set(:@lifecycle_factory, lambda do |request|
        if request.headers["x-fail"]
          failed << request.stream.id
          raise "lifecycle factory failed"
        end
        factory&.call(request)
      end)
      failed
    end

    # Records each call of the session's rejection hook, then raises.
    def fail_rejections(fixture)
      session = keepalive_session(fixture.host)
      attempts = []
      session.instance_variable_set(:@on_request_rejected, lambda do
        attempts << true
        raise "rejection hook failed"
      end)
      attempts
    end

    # Holds the fixture connection's call in the only admission slot and
    # opens a second connection for streams the host then refuses.
    def fill_admission(task, fixture)
      held = task.async { unary(fixture, "hold-admission") }
      wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-admission"] }
      [held, keepalive_client(task, fixture.host.port)]
    end

    def release_admission(fixture, held)
      fixture.log[:holds]["hold-admission"].enqueue(true)
      expect(held.wait).to eq([200, encoded("hold-admission")])
    end

    # The rejection hook runs before the lifecycle factory, so a factory
    # that raises still leaves the refusal counted, with no exchange.
    it "counts a HEADERS refusal once when its lifecycle factory raises" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 1) do |fixture|
          rejections = count_rejections(fixture)
          failed = fail_lifecycle(fixture)
          held, other = fill_admission(task, fixture)
          refused = other.connection.next_stream_id
          raw_requests(other, [[refused, request_headers(echo_path, %w[x-fail 1]), encoded("refused")]])
          ping_through(task, other, "factory1")
          expect(other.wire.resets).to eq(refused => KeepaliveClient::REFUSED_STREAM)
          wait_until(task, "lifecycle factory raised") { failed == [refused] }
          expect(rejections.size).to eq(1)

          release_admission(fixture, held)
          wait_until(task, "held call observed") { fixture.log[:completions].size == 1 }
          expect(rejected_completions(fixture)).to eq([])
          expect(rejections.size).to eq(1)
          expect(other.wire.reset_counts).to eq(refused => 1)
        ensure
          close_keepalive_client(other)
        end
      end
    end

    # Delivery comes from the reset's write alone, so a rejection hook that
    # raises leaves a written reset observed as completed.
    it "completes a HEADERS refusal's delivery once its reset is written when the rejection hook raises" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 1) do |fixture|
          attempts = fail_rejections(fixture)
          held, other = fill_admission(task, fixture)
          refused = other.connection.next_stream_id
          raw_requests(other, [[refused, request_headers(echo_path), encoded("refused")]])
          ping_through(task, other, "rejects1")
          expect(other.wire.resets).to eq(refused => KeepaliveClient::REFUSED_STREAM)
          wait_until(task, "refusal observed") { fixture.log[:completions].size == 1 }
          completion = fixture.log[:completions].first
          expect([completion.decision, completion.http_status, completion.delivery])
            .to eq([:admission_rejected, nil, :completed])
          expect(attempts.size).to eq(1)

          release_admission(fixture, held)
          wait_until(task, "held call observed") { fixture.log[:completions].size == 2 }
          expect(rejected_completions(fixture).size).to eq(1)
          expect(attempts.size).to eq(1)
        ensure
          close_keepalive_client(other)
        end
      end
    end

    # A refusal whose reset is never written, because its connection ended
    # first, is observed once as aborted.
    it "observes a HEADERS refusal as aborted when its connection ends before the reset is written" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 1) do |fixture|
          rejections = count_rejections(fixture)
          held, other = fill_admission(task, fixture)
          resource = second_resource(task, fixture)
          refused = other.connection.next_stream_id
          gate = hold_control_frame(resource, Protocol::HTTP2::ResetStreamFrame::TYPE, stream_id: refused)
          raw_requests(other, [[refused, request_headers(echo_path), encoded("refused")]])
          gate.started.dequeue
          wait_until(task, "refusal counted") { rejections.size == 1 }
          expect(fixture.log[:completions]).to eq([])

          close_keepalive_client(other)
          wait_until(task, "connection retired") { retired?(resource) }
          gate.release.enqueue(true)
          wait_until(task, "refusal observed") { fixture.log[:completions].size == 1 }
          completion = fixture.log[:completions].first
          expect([completion.decision, completion.http_status, completion.delivery])
            .to eq([:admission_rejected, nil, :aborted])

          release_admission(fixture, held)
          wait_until(task, "held call observed") { fixture.log[:completions].size == 2 }
          expect(rejected_completions(fixture).size).to eq(1)
          expect(rejections.size).to eq(1)
        ensure
          gate&.release&.enqueue(true)
          close_keepalive_client(other)
        end
      end
    end

    # A refused stream leaves the connection's streams at once, but counts
    # toward the stream limit until its reset is written, so a peer that
    # ignores the limit while resets queue gets the unobserved over-limit
    # refusal instead of more observed ones.
    it "counts unwritten HEADERS refusals toward the stream limit" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 2,
                                  max_concurrent_streams: 2) do |fixture|
          rejections = count_rejections(fixture)
          held, other = fill_admission(task, fixture)
          sibling = task.async do
            response = other.connection.call(connect_request(echo_path, "hold-sibling"))
            [response.status, response.read]
          end
          wait_until(task, "sibling call invoked") { fixture.log[:invoked].include?("hold-sibling") }
          resource = second_resource(task, fixture)
          refused, over = Array.new(2) { other.connection.next_stream_id }
          gate = hold_control_frame(resource, Protocol::HTTP2::ResetStreamFrame::TYPE, stream_id: refused)
          raw_requests(other, [[refused, request_headers(echo_path), encoded("refused")]])
          gate.started.dequeue
          wait_until(task, "refusal counted") { rejections.size == 1 }

          raw_requests(other, [[over, request_headers(echo_path), encoded("over")]])
          queue = resource.upkeep.instance_variable_get(:@writer).instance_variable_get(:@queue)
          wait_until(task, "over-limit reset queued behind the held reset") { queue.size == 1 }
          gate.release.enqueue(true)
          ping_through(task, other, "limit001")
          expect(other.wire.resets).to eq(refused => KeepaliveClient::REFUSED_STREAM,
                                          over => KeepaliveClient::REFUSED_STREAM)
          wait_until(task, "refusal observed") { rejected_completions(fixture).any? }
          expect(rejections.size).to eq(1)

          release_admission(fixture, held)
          fixture.log[:holds]["hold-sibling"].enqueue(true)
          expect(sibling.wait).to eq([200, encoded("hold-sibling")])
          wait_until(task, "held calls observed") { fixture.log[:completions].size == 3 }
          expect(rejected_completions(fixture).size).to eq(1)
          expect(rejections.size).to eq(1)
          expect(fixture.log[:invoked]).to match_array(%w[hold-admission hold-sibling])
          expect(other.wire.reset_counts).to eq(refused => 1, over => 1)
          expect(other.wire.goaways).to eq([])
        ensure
          gate&.release&.enqueue(true)
          close_keepalive_client(other)
        end
      end
    end

    # A stream holds its reserved slot while its dispatch has not reached
    # admission, and releases it when the peer resets it. Its held dispatch
    # then neither runs the application nor counts a refusal, whether the
    # later call that took the slot has finished or still holds it.
    { "free" => "after", "taken" => "hold-after" }.each do |slot, later|
      it "holds a reserved slot until dispatch, releases it on a peer reset, and skips that stream at a #{slot} slot" do
        reactor do |task|
          with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 1) do |fixture|
            gate = gate_lifecycle(fixture)
            rejections = count_rejections(fixture)
            client = fixture.client
            reserved = client.connection.next_stream_id
            raw_headers(client, reserved, request_headers(echo_path, %w[x-gate 1]))
            wait_until(task, "reserved stream's dispatch held") { gate[:held] == [reserved] }
            other = keepalive_client(task, fixture.host.port)
            refused = other.connection.next_stream_id
            raw_requests(other, [[refused, request_headers(echo_path), encoded("refused")]])
            ping_through(task, other, "reserve1")
            expect(other.wire.resets).to eq(refused => KeepaliveClient::REFUSED_STREAM)
            wait_until(task, "refusal observed") { fixture.log[:completions].size == 1 }
            completion = fixture.log[:completions].first
            expect([completion.decision, completion.http_status, completion.delivery])
              .to eq([:admission_rejected, nil, :completed])

            raw_reset(client, reserved)
            ping_through(task, client, "reserve2")
            call = task.async do
              response = other.connection.call(connect_request(echo_path, later))
              [response.status, response.read]
            end
            wait_until(task, "later call invoked") { fixture.log[:invoked] == [later] }
            held = later.start_with?("hold")
            expect(call.wait).to eq([200, encoded(later)]) unless held
            expect(rejections.size).to eq(1)
            expect(other.wire.reset_counts).to eq(refused => 1)
            expect(client.wire.resets).to eq({})

            release_gate(gate)
            expected = [[:admission_rejected, :not_started, :completed, nil], [:processed, :not_started, :aborted, nil]]
            expected << [:processed, :finished, :completed, 200] unless held
            wait_until(task, "reset stream's exchange finished") { fixture.log[:completions].size == expected.size }
            facts = fixture.log[:completions].map do |finished|
              [finished.decision, finished.processing_state, finished.delivery, finished.http_status]
            end
            expect(facts).to match_array(expected)
            expect(rejections.size).to eq(1)
            fixture.log[:holds][later].enqueue(true) if held
            expect(call.wait).to eq([200, encoded(later)])
            expect(fixture.log[:invoked]).to eq([later])
            expect(client.wire.resets).to eq({})
          ensure
            release_gate(gate)
            close_keepalive_client(other)
          end
        end
      end
    end
  end

  describe "request body progress" do
    def admitted(fixture) = keepalive_session(fixture.host).instance_variable_get(:@admitted)
    # Due times of pending request body waits; other timers also sleep.
    def body_waits(fixture) = fixture.clock.sleepers.select { |sleeper| sleeper.seconds == 60 }.map(&:due)

    it "resets a unary stream whose body stalls with CANCEL after 60 s and releases its admission" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          stalled = client.connection.next_stream_id
          raw_headers(client, stalled, request_headers(echo_path))
          await_due(task, fixture.clock, 60)
          expect(admitted(fixture).size).to eq(1)
          fixture.clock.advance(59)
          ping_through(task, client, "early059")
          expect(client.wire.resets).to eq({})
          fixture.clock.advance(1)
          wait_until(task, "CANCEL; resets #{client.wire.resets}") do
            client.wire.resets[stalled] == KeepaliveClient::CANCEL
          end
          wait_until(task, "admission released") { admitted(fixture).empty? }
          expect(fixture.log[:invoked]).to eq([])
          expect(unary(fixture, "after")).to eq([200, encoded("after")])
          expect(client.wire.goaways).to eq([])
        end
      end
    end

    it "restarts the bound on body bytes, not on empty DATA" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          stream = client.connection.next_stream_id
          raw_headers(client, stream, request_headers(echo_path))
          await_due(task, fixture.clock, 60)
          fixture.clock.advance(30)
          raw_data(client, stream, "")
          ping_through(task, client, "empty030")
          expect(body_waits(fixture)).to eq([60])
          raw_data(client, stream, encoded("slow").byteslice(0, 2))
          await_due(task, fixture.clock, 90)
          wait_until(task, "the first body wait ended") { body_waits(fixture) == [90] }
          fixture.clock.advance(59)
          ping_through(task, client, "bytes089")
          expect(client.wire.resets).to eq({})
          fixture.clock.advance(1)
          wait_until(task, "CANCEL") { client.wire.resets[stream] == KeepaliveClient::CANCEL }
        end
      end
    end

    it "ends the bound with the body's end or a peer reset" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          finished = client.connection.next_stream_id
          raw_headers(client, finished, request_headers(echo_path))
          await_due(task, fixture.clock, 60)
          raw_data(client, finished, encoded("whole"), end_stream: true)
          wait_until(task, "response DATA") { client.wire.data[finished].positive? }
          expect(fixture.log[:invoked]).to eq(["whole"])
          expect(body_waits(fixture)).to eq([])

          reset = client.connection.next_stream_id
          raw_headers(client, reset, request_headers(echo_path))
          await_due(task, fixture.clock, 60)
          raw_reset(client, reset)
          wait_until(task, "admission released") { admitted(fixture).empty? }
          expect(body_waits(fixture)).to eq([])
          expect(client.wire.resets).to eq({})
        end
      end
    end

    it "lets a client-streaming request idle past the bound" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          body = Protocol::HTTP::Body::Writable.new
          request = Protocol::HTTP::Request["POST", collect_method.path,
                                            { "content-type" => "application/connect+proto" }, body,
                                            scheme: "http", authority: "127.0.0.1"]
          call = task.async { client.connection.call(request) }
          wait_until(task, "Collect started") { fixture.log[:invoked] == ["collect"] }
          ping_through(task, client, "collect0")
          expect(body_waits(fixture)).to eq([])
          fixture.clock.advance(61)
          ping_through(task, client, "collect1")
          expect(client.wire.resets).to eq({})
          bytes = encoded("idle")
          body.write([0, bytes.bytesize].pack("CN") + bytes)
          body.close_write
          response = call.wait
          expect(response.status).to eq(200)
          expect(read_until(response, encoded("idle"))).to include(encoded("idle"))
        end
      end
    end
  end

  describe "response write progress" do
    def zero_window = { KeepaliveClient::INITIAL_WINDOW_SIZE => 0 }
    def open_window = { KeepaliveClient::INITIAL_WINDOW_SIZE => KeepaliveClient::MAXIMUM_WINDOW }

    # Due times of pending response credit waits; other timers also sleep.
    def credit_waits(fixture) = fixture.clock.sleepers.select { |sleeper| sleeper.seconds == 60 }.map(&:due)

    # A whole request written frame by frame; returns its stream id.
    def start_call(client, path, name, content_type: "application/proto")
      bytes = encoded(name)
      bytes = [0, bytes.bytesize].pack("CN") + bytes unless content_type == "application/proto"
      headers = request_headers(path).map { |key, value| [key, key == "content-type" ? content_type : value] }
      headers << %w[te trailers] if content_type.start_with?("application/grpc")
      id = client.connection.next_stream_id
      raw_headers(client, id, headers)
      raw_data(client, id, bytes, end_stream: true)
      id
    end

    # The response HEADERS arrived and the server applied every earlier frame.
    def await_headers(task, client, stream, payload)
      wait_until(task, "response HEADERS on #{stream}") { client.wire.headers[stream].positive? }
      ping_through(task, client, payload)
    end

    it "resets a unary response that waits 60 s for credit with CANCEL and frees its slot" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), max_requests: 1,
                                  client_options: { settings: zero_window }) do |fixture|
          client = fixture.client
          stalled = start_call(client, echo_path, "stalled")
          await_headers(task, client, stalled, "credit00")
          expect(credit_waits(fixture)).to eq([60])
          fixture.clock.advance(59)
          ping_through(task, client, "credit59")
          expect(client.wire.resets).to eq({})
          fixture.clock.advance(1)
          wait_until(task, "CANCEL; resets #{client.wire.resets}") do
            client.wire.resets[stalled] == KeepaliveClient::CANCEL
          end
          expect(client.wire.data[stalled]).to eq(0)
          wait_until(task, "stalled exchange finished") { fixture.log[:completions].size == 1 }
          completion = fixture.log[:completions].first
          expect([completion.delivery, completion.error.class.name])
            .to eq([:aborted, "ConnectRPC::Async::Internal::ResponseBodyTimeout"])
          # With max_requests: 1, the later call shows the slot was released.
          later = start_call(client, echo_path, "later")
          await_headers(task, client, later, "later000")
          raw_window_update(client, later, 1024)
          wait_until(task, "later response DATA") { client.wire.data_bytes[later] == encoded("later").bytesize }
          expect(fixture.log[:invoked]).to eq(%w[stalled later])
          expect(client.wire.resets).to eq(stalled => KeepaliveClient::CANCEL)
          expect(client.wire.goaways).to eq([])
        end
      end
    end

    it "restarts the bound when credit arrives, not on a wakeup without credit" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil),
                                  client_options: { settings: zero_window }) do |fixture|
          client = fixture.client
          stream = start_call(client, echo_path, "slow")
          await_headers(task, client, stream, "credit00")
          expect(credit_waits(fixture)).to eq([60])
          fixture.clock.advance(20)
          # Connection credit wakes the waiting output; the stream has none.
          raw_window_update(client, 0, 1024)
          ping_through(task, client, "conn0020")
          expect(credit_waits(fixture)).to eq([60])
          fixture.clock.advance(10)
          raw_window_update(client, stream, 1)
          wait_until(task, "one byte of DATA") { client.wire.data_bytes[stream] == 1 }
          wait_until(task, "the first credit wait ended") { credit_waits(fixture) == [90] }
          fixture.clock.advance(59)
          ping_through(task, client, "credit89")
          expect(client.wire.resets).to eq({})
          fixture.clock.advance(1)
          wait_until(task, "CANCEL") { client.wire.resets[stream] == KeepaliveClient::CANCEL }
        end
      end
    end

    it "bounds a response that waits for connection credit while its stream has credit" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil),
                                  client_options: { settings: open_window }) do |fixture|
          client = fixture.client
          client.connection.define_singleton_method(:request_window_update) { nil }
          ping_through(task, client, "settings")
          name = "c" * 70_000
          call = task.async { client.connection.call(connect_request(echo_path, name)) }
          response = call.wait
          stream = response.stream.id
          wait_until(task, "the connection window spent") { client.wire.data_bytes[stream] == 65_535 }
          ping_through(task, client, "spent000")
          expect(credit_waits(fixture)).to eq([60])
          fixture.clock.advance(60)
          wait_until(task, "CANCEL") { client.wire.resets[stream] == KeepaliveClient::CANCEL }
        end
      end
    end

    it "ends a credit wait when the peer resets the stream" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil),
                                  client_options: { settings: zero_window }) do |fixture|
          client = fixture.client
          stream = start_call(client, echo_path, "reset")
          await_headers(task, client, stream, "credit00")
          expect(credit_waits(fixture)).to eq([60])
          raw_reset(client, stream)
          wait_until(task, "credit wait ended") { credit_waits(fixture).empty? }
          wait_until(task, "stream closed") { fixture.resource.connection.streams.empty? }
          expect(client.wire.resets).to eq({})
        end
      end
    end

    it "bounds a client-streaming response" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil),
                                  client_options: { settings: zero_window }) do |fixture|
          client = fixture.client
          stream = start_call(client, collect_method.path, "one", content_type: "application/connect+proto")
          await_headers(task, client, stream, "credit00")
          expect(credit_waits(fixture)).to eq([60])
          fixture.clock.advance(60)
          wait_until(task, "CANCEL") { client.wire.resets[stream] == KeepaliveClient::CANCEL }
        end
      end
    end

    # A subscriber may apply backpressure indefinitely; deadlines, age and
    # close still end it.
    it "lets a server stream wait for credit past the bound" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil),
                                  client_options: { settings: zero_window }) do |fixture|
          client = fixture.client
          fixture.log[:feeds]["watch"].enqueue("first")
          stream = start_call(client, watch_method.path, "watch", content_type: "application/connect+proto")
          await_headers(task, client, stream, "credit00")
          fixture.clock.advance(61)
          ping_through(task, client, "credit61")
          expect(client.wire.resets).to eq({})
          expect(credit_waits(fixture)).to eq([])
          raw_window_update(client, stream, 1024)
          wait_until(task, "streamed DATA") { client.wire.data_bytes[stream].positive? }
        end
      end
    end

    it "leaves credit waits unbounded with response_body_timeout: nil" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), response_body_timeout: nil,
                                  client_options: { settings: zero_window }) do |fixture|
          client = fixture.client
          stream = start_call(client, echo_path, "unbounded")
          await_headers(task, client, stream, "credit00")
          expect(credit_waits(fixture)).to eq([])
          fixture.clock.advance(120)
          ping_through(task, client, "credt120")
          expect(client.wire.resets).to eq({})
          raw_window_update(client, stream, 1024)
          wait_until(task, "response DATA") { client.wire.data_bytes[stream] == encoded("unbounded").bytesize }
        end
      end
    end

    # The bound is frame_write_timeout, 60 s by default whatever the
    # keepalive timeout, and applies to each frame write rather than to the
    # whole response.
    headers = Protocol::HTTP2::HeadersFrame::TYPE
    data = Protocol::HTTP2::DataFrame::TYPE
    end_stream = Protocol::HTTP2::END_STREAM
    [
      { label: "initial HEADERS", keepalive: { time: nil }, bound: 60, frame: [headers] },
      { label: "DATA", keepalive: { time: nil }, options: { frame_write_timeout: 7 }, bound: 7, frame: [data] },
      { label: "DATA with a 7 s keepalive timeout", keepalive: { time: nil, timeout: 7 }, bound: 60, frame: [data] },
      { label: "DATA without keepalive", keepalive: nil, bound: 60, frame: [data] },
      { label: "DATA over TLS", keepalive: { time: nil }, bound: 60, frame: [data], tls: true },
      { label: "END_STREAM DATA", keepalive: { time: nil }, bound: 60, frame: [data, end_stream] },
      { label: "gRPC trailers", keepalive: { time: nil }, bound: 60, frame: [headers, end_stream],
        content_type: "application/grpc" }
    ].each do |row|
      bound = row[:bound]
      it "aborts the connection when the held #{row[:label]} write does not finish within #{bound} s" do
        reactor do |task|
          keepalive = row[:keepalive] && server_keepalive(**row[:keepalive])
          with_keepalive_host(task, keepalive: keepalive, tls: row.fetch(:tls, false),
                                    **row.fetch(:options, {})) do |fixture|
            frame_type, flags = row[:frame]
            gate = hold_frames(fixture.resource, frame_type, flags: flags)
            start_call(fixture.client, echo_path, "held", content_type: row.fetch(:content_type, "application/proto"))
            gate.started.dequeue
            fixture.clock.advance(bound - 0.1)
            await_due(task, fixture.clock, bound)
            expect(fixture.resource).not_to be_finished
            fixture.clock.advance(0.1)
            wait_until(task, "connection retired") { retired?(fixture.resource) }
            wait_until(task, "client EOF") { fixture.client.eof? }
          ensure
            gate&.release&.enqueue(true)
          end
        end
      end
    end

    # A control frame queued behind a timed frame write, such as the ACK of
    # a grpc-go BDP PING, waits for that write's own bound instead of
    # failing the connection at its 20 s deadline. Without the write bound,
    # the control frame deadline still ends the connection.
    def due_at(clock, due) = clock.sleepers.count { |sleeper| (sleeper.due - due).abs < 1e-9 }

    # Past its 20 s deadline, the control frame waits for the held write's
    # 60 s bound, in steps that may end sooner.
    def await_behind_write(task, clock)
      wait_until(task, "the control deadline waits for the write bound; pending #{clock.sleepers.map(&:due)}") do
        due_at(clock, 60) >= 1 && clock.sleepers.count { |sleeper| sleeper.due > 20 && sleeper.due <= 60 } == 2
      end
    end

    it "keeps a control frame queued behind a held write until the frame write bound" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          send_ping(client, "bdp00001")
          await_due(task, fixture.clock, 20)
          fixture.clock.advance(20)
          await_behind_write(task, fixture.clock)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(39.9)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(0.1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(client.wire.acks).to eq([])
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    # A frame written late because it waited for a timed write passes its
    # lateness on, so the control frames queued behind it are not failed
    # for a deadline they missed only by waiting for the same write.
    it "writes every control frame queued behind a held write that finishes within the bound" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          # Both PINGs in one write, so both ACKs queue at the same instant.
          client.connection.write_frames do |framer|
            %w[bdp00001 bdp00002].each do |payload|
              frame = Protocol::HTTP2::PingFrame.new
              frame.pack(payload)
              framer.write_frame(frame)
            end
          end
          queue = fixture.resource.upkeep.instance_variable_get(:@writer).instance_variable_get(:@queue)
          wait_until(task, "the second ACK queued behind the first") { queue.size == 1 }
          await_due(task, fixture.clock, 20)
          fixture.clock.advance(20)
          await_behind_write(task, fixture.clock)
          fixture.clock.advance(10)
          gate.release.enqueue(true)
          wait_until(task, "both ACKs; acks #{client.wire.acks}") { client.wire.acks.size == 2 }
          expect(client.wire.acks).to eq(%w[bdp00001 bdp00002])
          ping_through(task, client, "after030")
          expect(fixture.resource).not_to be_finished
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    # The first ACK is written 10 s late, behind the held write, so the
    # second one's deadline restarts at 50 s. The untimed guard holder that
    # takes the guard in between has no bound of its own, and the second
    # ACK still fails the connection at its restarted deadline rather than
    # at the 60 s write bound the first ACK was waiting for.
    it "fails at a restarted control frame deadline that falls before the bound it waited for" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          connection = fixture.resource.connection
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          client.connection.write_frames do |framer|
            %w[bdp00001 bdp00002].each do |payload|
              frame = Protocol::HTTP2::PingFrame.new
              frame.pack(payload)
              framer.write_frame(frame)
            end
          end
          wait_until(task, "the first ACK waits for the frame guard") { connection.guard_waiting? }
          release = Async::Promise.new
          holding = Async::Promise.new
          holder = task.async do
            connection.synchronize do
              holding.resolve(true)
              release.wait
            end
          end
          waiting = connection.instance_variable_get(:@frame_guard).waiting
          wait_until(task, "the guard holder queued behind the first ACK") { waiting.size == 2 }
          await_due(task, fixture.clock, 20)
          fixture.clock.advance(20)
          await_behind_write(task, fixture.clock)
          fixture.clock.advance(10)
          gate.release.enqueue(true)
          wait_until(task, "the first ACK") { client.wire.acks == ["bdp00001"] }
          holding.wait
          fixture.clock.advance(10)
          await_due(task, fixture.clock, 50)
          fixture.clock.advance(9.9)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(0.1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(client.wire.acks).to eq(["bdp00001"])
        ensure
          gate&.release&.enqueue(true)
          release&.resolve(true)
          holder&.wait
        end
      end
    end

    # The first ACK is written exactly at its 20 s deadline behind the held
    # write, so the reset queued at 5 s keeps its own 25 s deadline. Behind
    # an untimed guard holder, that reset fails the connection at 25 s, not
    # when the watch would otherwise next wake.
    it "fails a queued control frame at its own deadline after the frame ahead is written exactly at its" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          connection = fixture.resource.connection
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          send_ping(client, "bdp00001")
          wait_until(task, "the ACK waits for the frame guard") { connection.guard_waiting? }
          release = Async::Promise.new
          holding = Async::Promise.new
          holder = task.async do
            connection.synchronize do
              holding.resolve(true)
              release.wait
            end
          end
          waiting = connection.instance_variable_get(:@frame_guard).waiting
          wait_until(task, "the guard holder queued behind the ACK") { waiting.size == 2 }
          await_due(task, fixture.clock, 20)
          fixture.clock.advance(5)
          refused = fixture.resource.upkeep.refuse(99)
          fixture.clock.advance(15)
          await_behind_write(task, fixture.clock)
          gate.release.enqueue(true)
          wait_until(task, "the ACK") { client.wire.acks == ["bdp00001"] }
          holding.wait
          await_due(task, fixture.clock, 25)
          fixture.clock.advance(4.9)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(0.1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(refused.wait).to be(false)
        ensure
          gate&.release&.enqueue(true)
          release&.resolve(true)
          holder&.wait
        end
      end
    end

    # The reset is dequeued past its deadline while a second stream's DATA
    # write, timed from 20 s, holds the guard, so it waits out that write's
    # 80 s bound. The write finishes at 25 s and hands the guard to an
    # untimed holder; the reset still fails the connection only at 80 s.
    it "waits out the bound of a timed write running when a control frame is dequeued past its deadline" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          connection = fixture.resource.connection
          watch(fixture, "sibling", "one")
          watch(fixture, "other", "uno")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          send_ping(client, "bdp00001")
          waiting = connection.instance_variable_get(:@frame_guard).waiting
          wait_until(task, "the ACK waits for the frame guard") { waiting.size == 1 }
          refused = fixture.resource.upkeep.refuse(99)
          fixture.log[:feeds]["other"].enqueue("dos")
          wait_until(task, "the second stream's DATA queued behind the ACK") { waiting.size == 2 }
          release = Async::Promise.new
          holding = Async::Promise.new
          holder = task.async do
            connection.synchronize do
              holding.resolve(true)
              release.wait
            end
          end
          wait_until(task, "the guard holder queued behind that DATA") { waiting.size == 3 }
          gate.armed = true
          await_due(task, fixture.clock, 20)
          fixture.clock.advance(20)
          await_behind_write(task, fixture.clock)
          gate.release.enqueue(true)
          gate.started.dequeue
          wait_until(task, "the ACK") { client.wire.acks == ["bdp00001"] }
          fixture.clock.advance(5)
          gate.release.enqueue(true)
          holding.wait
          fixture.clock.advance(15)
          # Beside the first DATA write's watchdog, the watch sleeps on from
          # 40 s toward 80 s, unless it failed the connection.
          wait_until(task, "the watch passes 40 s") do
            retired?(fixture.resource) || due_at(fixture.clock, 60) == 2
          end
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(39.9)
          await_due(task, fixture.clock, 80)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(0.1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(refused.wait).to be(false)
        ensure
          gate&.release&.enqueue(true)
          release&.resolve(true)
          holder&.wait
        end
      end
    end

    # The control writer is woken without yielding, so a starved writer can
    # dequeue a frame after its deadline. With no timed write to wait out,
    # the frame fails the connection instead of being written late.
    it "fails a control frame the writer dequeues past its deadline" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          written = fixture.resource.upkeep.refuse(99)
          fixture.clock.advance(21)
          expect(written.wait).to be(false)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
        end
      end
    end

    # The first ACK is written exactly at its 20 s deadline, so it was not
    # late and the second ACK keeps the same deadline. With no timed write
    # left to wait out, the second ACK fails the connection at once rather
    # than when the watch next wakes.
    it "fails the next control frame at once when one is written exactly at the shared deadline" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          client.connection.write_frames do |framer|
            %w[bdp00001 bdp00002].each do |payload|
              frame = Protocol::HTTP2::PingFrame.new
              frame.pack(payload)
              framer.write_frame(frame)
            end
          end
          await_due(task, fixture.clock, 20)
          fixture.clock.advance(20)
          await_behind_write(task, fixture.clock)
          gate.release.enqueue(true)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(client.wire.acks).to eq(["bdp00001"])
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    it "sends a drain's GOAWAY and PING queued behind a held write that finishes within the bound" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          client.wire.ack_pings = false
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          closing = task.async { fixture.host.close(grace: 120) }
          wait_until(task, "heads-up GOAWAY waits for the frame guard") do
            fixture.resource.connection.guard_waiting?
          end
          await_due(task, fixture.clock, 20)
          fixture.clock.advance(20)
          await_behind_write(task, fixture.clock)
          fixture.clock.advance(10)
          gate.release.enqueue(true)
          wait_until(task, "drain PING; goaways #{client.wire.goaways}") do
            client.wire.pings == [KeepaliveClient::DRAIN_PING]
          end
          expect(client.wire.goaways).to eq([[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR,
                                              "graceful_stop"]])
          expect(fixture.resource).not_to be_finished
        ensure
          gate&.release&.enqueue(true)
          closing&.stop
        end
      end
    end

    it "ends a held write at the control frame deadline with frame_write_timeout: nil" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), frame_write_timeout: nil) do |fixture|
          client = fixture.client
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          send_ping(client, "bdp00001")
          await_due(task, fixture.clock, 20)
          fixture.clock.advance(19.9)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(0.1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(client.wire.acks).to eq([])
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    it "lets a held frame write outlast the bound with frame_write_timeout: nil" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil), frame_write_timeout: nil) do |fixture|
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          call = task.async { unary(fixture, "unbounded") }
          gate.started.dequeue
          expect(fixture.clock.sleepers).to eq([])
          fixture.clock.advance(600)
          expect(fixture.resource).not_to be_finished
          gate.release.enqueue(true)
          expect(call.wait).to eq([200, encoded("unbounded")])
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    it "lets a slow reader take up to the bound for each frame of one response" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          name = "s" * 40_000
          call = task.async { unary(fixture, name) }
          3.times do |index|
            gate.started.dequeue
            fixture.clock.advance(59.9)
            await_due(task, fixture.clock, (59.9 * index) + 60)
            expect(fixture.resource).not_to be_finished
            gate.armed = true if index < 2
            gate.release.enqueue(true)
          end
          expect(call.wait).to eq([200, encoded(name)])
          expect(fixture.resource).not_to be_finished
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    it "aborts a graceful drain whose stream frame write stalls past the bound" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          watch(fixture, "sibling", "one")
          gate = hold_frames(fixture.resource, Protocol::HTTP2::DataFrame::TYPE)
          fixture.log[:feeds]["sibling"].enqueue("two")
          gate.started.dequeue
          fixture.clock.advance(10)
          # The heads-up GOAWAY's own deadline, 20 s from now, waits for the
          # held write's bound.
          closing = task.async { fixture.host.close(grace: 5) }
          wait_until(task, "heads-up GOAWAY waits for the frame guard") do
            fixture.resource.connection.guard_waiting?
          end
          fixture.clock.advance(49.9)
          await_due(task, fixture.clock, 60)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(0.1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          expect(closing.wait).to be(false)
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end

    # The peer stays connected but never reads, so the server's socket send
    # buffer fills while a server stream with ample credit keeps writing.
    it "retires a connection whose peer stops reading a flooding server stream" do
      reactor do |task|
        client_options = { read: false, receive_buffer: 4096, settings: open_window }
        with_keepalive_host(task, keepalive: nil, max_requests: 1, client_options: client_options) do |fixture|
          client = fixture.client
          raw_window_update(client, 0, KeepaliveClient::MAXIMUM_WINDOW - 65_535)
          probe = WriteProbe.new(fixture.resource.socket)
          fixture.resource.connection.stream.instance_variable_set(:@io, probe)
          chunk = "f" * 65_536
          256.times { fixture.log[:feeds]["flood"].enqueue(chunk) }
          start_call(client, watch_method.path, "flood", content_type: "application/connect+proto")
          wait_until(task, "a server frame write blocked on the socket", within: 10) { probe.writing? }
          fixture.clock.advance(59.9)
          await_due(task, fixture.clock, 60)
          expect(fixture.resource).not_to be_finished
          fixture.clock.advance(0.1)
          wait_until(task, "connection retired") { retired?(fixture.resource) }
          other = keepalive_client(task, fixture.host.port)
          response = other.connection.call(connect_request(echo_path, "after"))
          expect([response.status, response.read]).to eq([200, encoded("after")])
        ensure
          close_keepalive_client(other)
        end
      end
    end
  end

  describe "graceful close" do
    def expect_graceful_drain(task, fixture, reason)
      client = fixture.client
      wire = client.wire
      wait_until(task, "heads-up GOAWAY and drain PING; goaways #{wire.goaways}") do
        wire.pings == [KeepaliveClient::DRAIN_PING]
      end
      expect(wire.goaways).to eq([[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR, reason]])
      expect(unary(fixture, "during")).to eq([200, encoded("during")])
      send_ping(client, KeepaliveClient::DRAIN_PING, ack: true)
      wait_until(task, "cutoff GOAWAY") { wire.goaways.size == 2 }
      expect(wire.goaways.last).to eq([3, KeepaliveClient::NO_ERROR, reason])
      refused = client.connection.next_stream_id
      raw_headers(client, refused, request_headers(echo_path), end_stream: true)
      wait_until(task, "refusal") { wire.resets[refused] == KeepaliveClient::REFUSED_STREAM }
    end

    [[:close, true], [:close, false], [:drain_tls, true]].each do |operation, keepalive|
      it "#{operation} sends the heads-up GOAWAY at once, dispatching until the cutoff " \
         "#{keepalive ? 'with' : 'without'} keepalive" do
        reactor do |task|
          options = { keepalive: keepalive ? server_keepalive(time: nil) : nil, tls: operation == :drain_tls }
          with_keepalive_host(task, **options) do |fixture|
            fixture.client.wire.ack_pings = false
            held = task.async { unary(fixture, "hold-drain") }
            wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-drain"] }
            draining = task.async do
              if operation == :close
                fixture.host.close(grace: 5)
              else
                fixture.host.reload_tls(**host_tls)
                fixture.host.drain_tls(grace: 5)
              end
            end
            expect_graceful_drain(task, fixture, operation == :close ? "graceful_stop" : "tls_rotation")
            fixture.log[:holds]["hold-drain"].enqueue(true)
            expect(held.wait).to eq([200, encoded("hold-drain")])
            expect(draining.wait).to be(true)
            expect(fixture.log[:invoked]).to eq(%w[hold-drain during])
          end
        end
      end
    end

    it "takes over an age drain before its ACK, keeping its reason and dispatching until the cutoff" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil,
                                                              max_connection_age: 100)) do |fixture|
          client = fixture.client
          wire = client.wire
          wire.ack_pings = false
          held = task.async { unary(fixture, "hold-age") }
          wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-age"] }
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          wait_until(task, "drain PING") { wire.pings == [KeepaliveClient::DRAIN_PING] }
          closing = task.async { fixture.host.close(grace: 5) }
          expect(unary(fixture, "during")).to eq([200, encoded("during")])
          expect(wire.goaways.size).to eq(1)
          send_ping(client, KeepaliveClient::DRAIN_PING, ack: true)
          wait_until(task, "cutoff GOAWAY") { wire.goaways.size == 2 }
          fixture.log[:holds]["hold-age"].enqueue(true)
          expect(held.wait).to eq([200, encoded("hold-age")])
          expect(closing.wait).to be(true)
          expect(wire.goaways).to eq([[KeepaliveClient::MAX_STREAM_ID, KeepaliveClient::NO_ERROR, "max_age"],
                                      [3, KeepaliveClient::NO_ERROR, "max_age"],
                                      [3, KeepaliveClient::NO_ERROR, ""]])
        end
      end
    end

    it "joins a drain that is already waiting for EOF without another GOAWAY" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil, min_time: nil,
                                                              max_connection_age: 100)) do |fixture|
          # A second descriptor keeps the socket open after the client closes.
          keeper = fixture.client.raw.dup
          await_due(task, fixture.clock, 100)
          fixture.clock.advance(100)
          wait_until(task, "server half-close") { fixture.client.eof? }
          await_due(task, fixture.clock, 101)
          closing = task.async { fixture.host.close(grace: 5) }
          # Close marks every connection draining just before joining its drain.
          wait_until(task, "close joined the drain") { fixture.resource.draining }
          keeper.close
          expect(closing.wait).to be(true)
          expect(fixture.client.wire.goaways.map(&:first)).to eq([KeepaliveClient::MAX_STREAM_ID, 0])
        ensure
          keeper&.close unless keeper&.closed?
        end
      end
    end

    it "aborts at once with grace 0, ending a pending request body wait" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive(time: nil)) do |fixture|
          client = fixture.client
          raw_headers(client, client.connection.next_stream_id, request_headers(echo_path))
          await_due(task, fixture.clock, 60)
          expect(fixture.host.close(grace: 0)).to be(false)
          expect(retired?(fixture.resource)).to be(true)
          wait_until(task, "body wait ended") { fixture.clock.sleepers.none? { |sleeper| sleeper.seconds == 60 } }
        end
      end
    end
  end
end
