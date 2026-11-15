# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"

RSpec.describe "ThreadedTransport gRPC trailers through the exchange proxy" do
  include ThreadedStreamHelpers

  def grpc_host
    threaded_host(server: mixed_server(watch: lambda { |message, writer, context|
      case message.name
      when "ok"
        writer.send_message(message)
        context.response_trailers["x-tail"] = %w[a b]
      when "late"
        writer.send_message(message)
        raise ConnectRPC::Error.new(:not_found, "late", trailers: { "x-end" => %w[c d] })
      else
        raise ConnectRPC::Error.new(:not_found, "remote", trailers: { "x-end" => %w[e f] })
      end
    }))
  end

  # The Ruby Host always sends headers first, so a raw peer sends trailers-only.
  def trailers_only_peer(fields)
    threaded_recording_peer do |incoming|
      incoming.read
      headers = Protocol::HTTP::Headers[[["content-type", "application/grpc"], *fields]]
      incoming.send_response(Protocol::HTTP::Response[200, headers, nil])
    end
  end

  def grpc_request(origin)
    headers = { "content-type" => ["application/grpc+proto"], "te" => ["trailers"] }
    stream_request(origin, watch_method, headers: headers)
  end

  # Runs one raw gRPC stream on a plain thread and returns its observations.
  def raw_grpc(transport, origin, name)
    Thread.new do
      transport.open_grpc_stream(grpc_request(origin)) do |exchange|
        exchange.write(envelope(item(name)))
        exchange.close_send
        head = exchange.response_head
        early = capture_error { exchange.response_trailers }
        bytes = +"".b
        while (chunk = exchange.read(1024)) && !chunk.empty?
          bytes << chunk
        end
        trailers = exchange.response_trailers
        exchange.finish
        { head: head, early: early, bytes: bytes, trailers: trailers }
      end
    end
  end

  it "returns ordinary success trailers only after EOF, with repeated fields, separate from headers" do
    peer = grpc_host
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    result = value_within(raw_grpc(transport, origin, "ok"), "the ordinary stream")
    expect([result[:early].class, result[:early].message])
      .to eq([ConnectRPC::StreamStateError, "Trailers require response EOF"])
    expect([result[:head].end_stream, result[:head].headers.key?("x-tail"), result[:bytes]])
      .to eq([false, false, envelope(item("ok"))])
    trailers = result.fetch(:trailers)
    expect(trailers.slice("grpc-status", "x-tail")).to eq("grpc-status" => ["0"], "x-tail" => %w[a b])
    expect(trailers.key?("content-type")).to be(false)
    expect([trailers.frozen?, trailers["x-tail"].frozen?, trailers["x-tail"].first.frozen?]).to eq([true] * 3)
    expect(admitted(transport)).to eq(0)
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end

  it "returns ordinary error trailers after a message, with repeated fields" do
    peer = grpc_host
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    result = value_within(raw_grpc(transport, origin, "late"), "the ordinary error stream")
    expect(result[:early]).to be_a(ConnectRPC::StreamStateError)
    expect([result[:head].end_stream, result[:bytes]]).to eq([false, envelope(item("late"))])
    expect(result.fetch(:trailers).slice("grpc-status", "grpc-message", "x-end"))
      .to eq("grpc-status" => ["5"], "grpc-message" => ["late"], "x-end" => %w[c d])
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end

  {
    "success" => [%w[grpc-status 0], %w[x-tail g], %w[x-tail h]],
    "error" => [%w[grpc-status 5], %w[grpc-message only], %w[x-end e], %w[x-end f]]
  }.each do |kind, fields|
    it "reports a trailers-only #{kind} response in the head and nil trailers after EOF" do
      peer = trailers_only_peer(fields)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = threaded(origin: origin)
      result = value_within(raw_grpc(transport, origin, kind), "the trailers-only stream")
      head = result.fetch(:head)
      expected = fields.group_by(&:first).transform_values { |pairs| pairs.map(&:last) }
      expect([head.end_stream, head.headers.slice(*expected.keys), head.headers.frozen?])
        .to eq([true, expected, true])
      expect([result[:early].class, result[:bytes], result[:trailers]]).to eq([ConnectRPC::StreamStateError, "", nil])
    ensure
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  it "surfaces trailers-only and ordinary remote errors through generated gRPC clients" do
    peer = grpc_host
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin, protocol: :grpc)
    threads = %w[remote late].map do |name|
      Thread.new { capture_error { typed.watch(item(name)) { |reader| reader.each { nil } } } }
    end
    errors = threads.map { |thread| value_within(thread, "a remote gRPC error") }
    expect(errors.map { |error| [error.code, error.message] }).to eq([%w[not_found remote], %w[not_found late]])
    expect(errors.map { |error| error.trailers["x-end"] }).to eq([%w[e f], %w[c d]])
    expect(admitted(transport)).to eq(0)
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end
end
