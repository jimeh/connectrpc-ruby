# frozen_string_literal: true

require "English"
require_relative "connection_peer"
require_relative "frame_gate"
require_relative "keepalive_client"

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

  # Unary calls are logged and those named hold* wait for their release.
  # Watch streams every name the example feeds until it feeds nil.
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
    handlers = AsyncHelpers::ECHO.handlers(echo: echo, fail: ->(message, _context) { message })
    services = [AsyncHelpers::ECHO::SERVICE, ConnectRPC::Service.new(name: "test.Keepalive", methods: [watch_method])]
    ConnectRPC::Server.new(services: services, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                           handlers: handlers.merge(watch_method.path => watch))
  end

  def keepalive_log
    queues = -> { Hash.new { |hash, key| hash[key] = Async::Queue.new } }
    { invoked: [], finished: [], holds: queues.call, feeds: queues.call }
  end

  def with_keepalive_host(task, keepalive:, idle_timeout: nil)
    log = keepalive_log
    host = keepalive_host(server: keepalive_server(log), idle_timeout: idle_timeout, keepalive: keepalive)
    session = keepalive_session(host)
    clock = ConnectionPeer::FakeClock.new
    session.instance_variable_set(:@clock, clock)
    session.instance_variable_set(:@random, ConnectionPeer::SequenceRandom.new(0.5))
    client = keepalive_client(task, host.port)
    resource = nil
    wait_until(task, "server connection upkeep") do
      (resource = session.instance_variable_get(:@resources).first)&.upkeep
    end
    yield KeepaliveClient::Fixture.new(host, clock, client, resource, log)
  ensure
    log[:holds].each_value { |queue| queue.enqueue(true) }
    log[:feeds].each_value { |queue| queue.enqueue(nil) }
    close_keepalive_client(client)
    host&.close(grace: 0)
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

  def hold_frames(resource, frame_type)
    gate = FrameGate.new(resource.socket, frame_type: frame_type)
    resource.connection.stream.instance_variable_set(:@io, gate)
    gate.armed = true
    gate
  end

  def retired?(resource) = resource.send(:retirement_completed?)

  describe "PING enforcement" do
    it "answers PINGs 10 seconds apart with the defaults, with and without calls" do
      reactor do |task|
        with_keepalive_host(task, keepalive: server_keepalive) do |fixture|
          client = fixture.client
          5.times do |index|
            fixture.clock.advance(10) unless index.zero?
            ping_through(task, client, format("idle%04d", index))
          end
          held = task.async { unary(fixture, "hold-call") }
          wait_until(task, "held call invoked") { fixture.log[:invoked] == ["hold-call"] }
          5.times do |index|
            fixture.clock.advance(10)
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

    it "keeps reading while an outbound frame is held and ACKs once it finishes" do
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
  end
end
