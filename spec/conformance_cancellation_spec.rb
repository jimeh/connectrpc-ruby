# frozen_string_literal: true

require "tmpdir"
require_relative "../conformance/client"
require_relative "../conformance/server"
require_relative "../conformance/ledger"
require_relative "support/socket_peer"

RSpec.describe "conformance cancellation evidence" do
  include SocketPeer

  def cancellation_request(codec: :CODEC_JSON, compression: :COMPRESSION_IDENTITY, port: 1)
    profile = compression == :COMPRESSION_GZIP ? "gzip" : "identity"
    name = Conformance::Ledger::PROFILES.fetch(profile).fetch("client").find do |entry|
      entry.start_with?("Client Cancellation/") && entry.include?(codec.to_s)
    end
    message = Conformance::UnaryRequest.new(response_definition: {
                                              response_headers: [{ name: "x-custom-header", value: ["foo"] }],
                                              response_data: "test response",
                                              response_trailers: [{ name: "x-custom-trailer", value: ["bing"] }],
                                              response_delay_ms: 200
                                            })
    Conformance::ClientCompatRequest.new(test_name: name, http_version: :HTTP_VERSION_1,
                                         protocol: :PROTOCOL_CONNECT, codec: codec, compression: compression,
                                         host: "127.0.0.1", port: port, service: Conformance::SERVICE_NAME,
                                         method: "Unary", stream_type: :STREAM_TYPE_UNARY,
                                         cancel: { after_close_send_ms: 5 },
                                         request_headers: [{ name: "x-test-case-name", value: [name] }],
                                         request_messages: [Conformance.pack(message)])
  end

  def capture_events
    Dir.mktmpdir("cancel-observation-") do |temporary|
      previous = ENV.fetch("CONFORMANCE_TELEMETRY", nil)
      file = File.join(temporary, "telemetry.jsonl")
      ENV["CONFORMANCE_TELEMETRY"] = file
      yield
      File.readlines(file).map { |line| JSON.parse(line) }
    ensure
      ENV["CONFORMANCE_TELEMETRY"] = previous
    end
  end

  def run_adapter(adapter, request)
    input = StringIO.new("".b)
    Conformance::Framing.write(input, request)
    input.rewind
    output = StringIO.new("".b)
    adapter.run(input, output)
    output.rewind
    result = Conformance::Framing.read(output, Conformance::ClientCompatResponse)
    expect(result.error).to be_nil
    expect(result.response.error.code).to eq(:CODE_CANCELED)
  end

  def ledger(events, request)
    profile = request.compression == :COMPRESSION_GZIP ? "gzip" : "identity"
    count = Conformance::Ledger::PROFILES.fetch(profile).fetch("client").size
    raw = <<~LOG
      Computed #{count} test case permutation(s) across 1 server configuration(s).
      Filtered tests to 1 test case permutation(s) across 1 server configuration(s).
      Sending request for #{JSON.generate(request.test_name)}...
      Received response for #{JSON.generate(request.test_name)}...
      Total cases: 1
      1 passed, 0 failed
    LOG
    Conformance::Ledger.new(direction: "client", selected: request.test_name, profile: profile).parse(raw, events, 0)
  end

  it "positively accounts for the original timer firing before the real Client call without entering transport" do
    request = cancellation_request
    adapter = Conformance::ClientAdapter.new
    canceled = Queue.new
    # Gate only this regression probe's invocation on the real timer event.
    # The official adapter still invokes Client immediately after arming it.
    allow(adapter).to receive(:invoke).and_wrap_original do |original, *args, **options|
      registration = options.fetch(:cancellation).on_cancel { canceled << true }
      raise "Cancellation never reached invocation" unless canceled.pop(timeout: 2)

      original.call(*args, **options)
    ensure
      registration&.unregister
    end
    events = capture_events { run_adapter(adapter, request) }
    expect(events.find { |event| event["event"] == "call_finished" }).to include(
      "transport_entries" => 0, "transport_exits" => 0, "setup_entries" => 0,
      "exchange_entries" => 0, "code" => "canceled", "cause" => nil, "observer_complete" => true
    )
    expect(events.none? { |event| %w[dispatch request_written wire_failure].include?(event["event"]) }).to be(true)
    expect(ledger(events, request).fetch(:cancellation_stages)).to eq(request.test_name => "before_transport")
  end

  it "accounts for cancellation accepted during the real encoder without entering transport" do
    source = ConnectRPC::CancellationSource.new
    adapter = Conformance::ClientAdapter.new
    allow(adapter).to receive(:with_cancellation) { |_request, _instruction, &call| call.call(source.token) }
    allow(Conformance::BINDING).to receive(:encode).and_wrap_original do |encode, *args, **options|
      source.cancel
      encode.call(*args, **options)
    end
    events = capture_events { run_adapter(adapter, cancellation_request) }
    expect(Conformance::BINDING).to have_received(:encode).once
    expect(events.find { |event| event["event"] == "call_finished" }).to include(
      "transport_entries" => 0, "transport_exits" => 0, "code" => "canceled", "cause" => nil,
      "observer_complete" => true
    )
    expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
  end

  %i[before_setup during_setup].each do |phase|
    it "accounts for real cancellation #{phase} and sends no late request" do
      source = ConnectRPC::CancellationSource.new
      adapter = Conformance::ClientAdapter.new
      # Dedicated boundary test, separate from the official timer scenario.
      allow(adapter).to receive(:with_cancellation) { |_request, _instruction, &call| call.call(source.token) }
      transport = nil
      allow(Conformance::ObservedNetHTTP).to receive(:new).and_wrap_original do |original, *args, **options|
        transport = original.call(*args, **options)
        boundary = phase == :before_setup ? :outgoing_request : :connection
        allow(transport).to receive(boundary).and_wrap_original do |operation, *values|
          result = operation.call(*values)
          if phase == :before_setup
            source.cancel
          else
            allow(result).to receive(:start).and_wrap_original do |start|
              started = start.call
              source.cancel

              started
            end
          end
          result
        end
        transport
      end
      events = nil
      if phase == :during_setup
        peer = ->(socket) { expect(socket.read).to eq("") }
        with_peer(peer) do |url, listener|
          request = cancellation_request(port: URI(url).port)
          events = capture_events { run_adapter(adapter, request) }
          expect(events.find { |event| event["event"] == "call_finished" }).to include(
            "transport_entries" => 1, "setup_entries" => 1, "exchange_entries" => 0,
            "code" => "canceled", "cause" => nil, "observer_complete" => true
          )
          expect(events.find { |event| event["event"] == "socket_cleanup" }).to include(
            "closed" => true, "retired" => true, "attached" => 1
          )
          expect(listener.wait_readable(0)).to be_nil
        end
      else
        request = cancellation_request
        events = capture_events { run_adapter(adapter, request) }
        expect(events.find { |event| event["event"] == "call_finished" }).to include(
          "transport_entries" => 1, "setup_entries" => 0, "exchange_entries" => 0,
          "code" => "canceled", "cause" => nil, "observer_complete" => true
        )
      end
      expect(events.none? { |event| %w[dispatch request_written].include?(event["event"]) }).to be(true)
    end
  end

  %i[CODEC_JSON CODEC_PROTO].product(%i[COMPRESSION_IDENTITY COMPRESSION_GZIP]).each do |codec, compression|
    it "proves full real-wire cancellation for #{codec}/#{compression} with synchronized progress" do
      progress = Queue.new
      source = ConnectRPC::CancellationSource.new
      adapter = Conformance::ClientAdapter.new
      # This dedicated wire test deliberately cancels after peer request progress.
      # It does not implement the official 5ms scenario or qualify its timer.
      allow(adapter).to receive(:with_cancellation) do |_request, _instruction, &call|
        caller = Thread.new { call.call(source.token) }
        expect(progress.pop(timeout: 2)).to be(true)
        source.cancel
        raise "Caller leaked" unless caller.join(2)

        caller.value
      ensure
        source.cancel
        caller&.join(2)
      end
      peer = lambda do |socket|
        read_request(socket)
        progress << true
        expect(socket.read).to eq("")
      end
      with_peer(peer) do |url, listener|
        request = cancellation_request(codec: codec, compression: compression, port: URI(url).port)
        events = capture_events { run_adapter(adapter, request) }
        finish = events.find { |event| event["event"] == "call_finished" }
        expect(finish).to include("code" => "canceled", "cause" => nil, "transport_entries" => 1,
                                  "exchange_entries" => 1, "observer_complete" => true)
        write = events.find { |event| event["event"] == "request_written" }
        dispatch = events.find { |event| event["event"] == "dispatch" }
        expect(write).to include("headers_complete" => true, "observer_complete" => true,
                                 "timeout_header" => false, "request" => dispatch.fetch("request"))
        expect(write.fetch("attempts")).to eq(write.fetch("completed_writes"))
        expect(write.fetch("body").except("content_length")).to eq(dispatch.fetch("wire").except("content_length"))
        expect(events.find { |event| event["event"] == "socket_cleanup" }).to include(
          "closed" => true, "interrupted" => nil, "close_calls" => 1, "retired" => true, "attached" => 1
        )
        expect(events.count { |event| event["event"] == "dispatch" }).to eq(1)
        expect(events.count { |event| event["event"] == "wire_failure" }).to eq(1)
        expect(listener.wait_readable(0)).to be_nil
        expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
      end
    end
  end

  it "observes a complete real transport response before explicit Client cancellation at its pre-commit boundary" do
    source = ConnectRPC::CancellationSource.new
    adapter = Conformance::ClientAdapter.new
    allow(adapter).to receive(:with_cancellation) { |_request, _instruction, &call| call.call(source.token) }
    allow(Conformance::ObservedNetHTTP).to receive(:new).and_wrap_original do |original, *args, **options|
      transport = original.call(*args, **options)
      allow(transport).to receive(:call).and_wrap_original do |exchange, request|
        response = exchange.call(request)
        expect(response.status).to eq(200)
        expect(response.body).not_to be_empty
        expect(source.cancel).to be(true)
        response
      end
      transport
    end
    allow(Conformance::BINDING).to receive(:decode).and_call_original
    config = Conformance::ServerCompatRequest.new(protocol: :PROTOCOL_CONNECT, http_version: :HTTP_VERSION_1)
    host = EchoExample::Localhost.new(Conformance::ServerAdapter.new(config).app)
    request = cancellation_request(port: URI(host.url).port)
    events = capture_events { run_adapter(adapter, request) }
    %w[transport setup exchange].each do |phase|
      expect(events.find { |event| event["event"] == "call_finished" }).to include(
        "#{phase}_entries" => 1, "#{phase}_exits" => 1, "code" => "canceled", "cause" => nil
      )
      expect(events.find { |event| event["event"] == "#{phase}_finished" }).to include(
        "code" => nil, "error_class" => nil, "cause" => nil
      )
    end
    expect(events.find { |event| event["event"] == "wire_response" }).to include("status" => 200)
    expect(events.none? { |event| event["event"] == "wire_failure" }).to be(true)
    expect(events.find { |event| event["event"] == "socket_cleanup" }).to include(
      "attached" => 1, "closed" => true, "retired" => true, "interrupted" => false, "close_calls" => 0
    )
    expect(Conformance::BINDING).not_to have_received(:decode).with(anything, type: Conformance::UnaryResponse,
                                                                              codec: :json)
    expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
  ensure
    host&.close
  end

  it "keeps interruption unknown when NetHTTP closes the real socket while native cancellation close is pending" do
    progress = Queue.new
    send_response = Queue.new
    peer_closed = Queue.new
    close_entered = Queue.new
    release_close = Queue.new
    source = ConnectRPC::CancellationSource.new
    adapter = Conformance::ClientAdapter.new
    allow(adapter).to receive(:with_cancellation) { |_request, _instruction, &call| call.call(source.token) }
    # Pause at entry into the unchanged native close. NetHTTP remains able to
    # close its own socket after reading the peer's response. The peer's EOF,
    # not a private lifecycle flag or elapsed delay, establishes that closure.
    trace = TracePoint.new(:call) do |event|
      next unless event.defined_class == ConnectRPC::Transports::NetHTTP::SocketLifecycle && event.method_id == :close

      close_entered << true
      raise "Native close gate timed out" unless release_close.pop(timeout: 2)
    end
    peer = lambda do |socket|
      read_request(socket)
      progress << true
      raise "Response gate timed out" unless send_response.pop(timeout: 2)

      body = Conformance::UnaryResponse.new(payload: { data: "test response" }).to_json
      socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" \
                   "Content-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n#{body}")
      expect(socket.read).to eq("")
      peer_closed << true
    end
    caller = canceler = nil
    with_peer(peer) do |url, listener|
      request = cancellation_request(port: URI(url).port)
      trace.enable
      events = capture_events do
        caller = Thread.new { run_adapter(adapter, request) }
        expect(progress.pop(timeout: 2)).to be(true)
        canceler = Thread.new { source.cancel }
        expect(close_entered.pop(timeout: 2)).to be(true)
        send_response << true
        expect(peer_closed.pop(timeout: 2)).to be(true)
        release_close << true
        expect(canceler.join(2)).to equal(canceler)
        expect(canceler.value).to be(true)
        expect(caller.join(2)).to equal(caller)
        caller.value
      end
      trace.disable
      expect(events.find { |event| event["event"] == "socket_cleanup" }).to include(
        "attached" => 1, "closed" => true, "close_calls" => 1, "retired" => true, "interrupted" => nil
      )
      expect(events.find { |event| event["event"] == "call_finished" }).to include("code" => "canceled", "cause" => nil)
      expect(listener.wait_readable(0)).to be_nil
      expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
    ensure
      trace&.disable
      send_response << true
      release_close << true
      canceler&.join(2)
      caller&.join(2)
    end
  ensure
    trace&.disable
  end

  it "distinguishes a completed BufferedIO write from an attempted write whose byte delivery is unknown" do
    left, right = Socket.pair(:UNIX, :STREAM, 0)
    buffered = Net::BufferedIO.new(left)
    buffered.extend(Conformance::ObservedNetHTTP::WrittenBytes)
    buffered.observed_writes = []
    buffered.write_attempts = 0
    buffered.write("actual bytes")
    expect(right.read(12)).to eq("actual bytes")
    expect(buffered.observed_writes).to eq(["actual bytes"])
    left.close
    expect { buffered.write("unknown delivery") }.to raise_error(IOError)
    expect(buffered.write_attempts).to eq(2)
    expect(buffered.observed_writes).to eq(["actual bytes"])
  ensure
    left&.close unless left&.closed?
    right&.close unless right&.closed?
  end
end
