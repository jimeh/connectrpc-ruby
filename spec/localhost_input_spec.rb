# frozen_string_literal: true

require "socket"
require_relative "../examples/echo_schema"
require_relative "../examples/localhost"
require_relative "../conformance/telemetry"

RSpec.describe EchoExample::Localhost do
  let(:arrivals) { Queue.new }
  let(:inputs) { Queue.new }
  let(:app) do
    server = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
                                    protocol: :grpc_web,
                                    handlers: { EchoExample::ECHO.path => ->(request, _) { request } })
    rack = ConnectRPC::Rack.new(server: server)
    lambda do |env|
      arrivals << env.fetch("CONTENT_TYPE")
      # Match the conformance adapter's replacement, including its read(length)
      # interface. Host cleanup must bypass this reader and its byte accounting.
      input = Conformance::Telemetry::Input.new(env.fetch("rack.input"))
      inputs << input
      env["rack.input"] = input
      rack.call(env)
    end
  end

  def with_socket(application = app)
    host = described_class.new(application)
    socket = TCPSocket.new("127.0.0.1", URI(host.url).port)
    socket.timeout = 3
    yield socket
  ensure
    socket&.close
    host&.close
  end

  def frame(payload, flag = 0)
    [flag, payload.bytesize].pack("CN") + payload
  end

  def send_request(socket, body, media: "application/grpc-web+proto", chunked: true, close: false)
    headers = "POST #{EchoExample::ECHO.path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: #{media}\r\n"
    headers << "Connection: close\r\n" if close
    if chunked
      # Send the complete chunk, including its terminator, before awaiting any
      # response. The regression suspends Rackup's Fiber inside this chunk.
      socket.write("#{headers}Transfer-Encoding: chunked\r\n\r\n#{body.bytesize.to_s(16)}\r\n#{body}\r\n0\r\n\r\n")
    else
      socket.write("#{headers}Content-Length: #{body.bytesize}\r\n\r\n#{body}")
    end
  end

  def read_response(socket)
    status = socket.gets("\r\n")
    expect(status).not_to be_nil, "Expected an HTTP response on the same socket, got EOF"
    headers = {}
    loop do
      line = socket.gets("\r\n")
      expect(line).not_to be_nil, "EOF within HTTP response headers"
      break if line == "\r\n"

      name, value = line.strip.split(":", 2)
      headers[name.downcase] = value.strip
    end
    length = Integer(headers.fetch("content-length"))
    body = socket.read(length)
    expect(body.bytesize).to eq(length)
    [status.split[1].to_i, headers, body]
  end

  def expect_compression_rejection(socket)
    status, headers, body = read_response(socket)
    expect(status).to eq(200)
    expect(headers.fetch("content-type")).to eq("application/grpc-web+proto")
    flag, length = body.unpack("CN")
    expect(flag).to eq(128)
    expect(body.bytesize).to eq(5 + length)
    expect(body.byteslice(5, length)).to include("grpc-status: 13\r\n")
    expect(arrivals.pop(timeout: 1)).to eq("application/grpc-web+proto")
    expect(inputs.pop(timeout: 1).bytes).to eq(5)
  end

  def expect_followups(socket)
    send_request(socket, frame("unsupported codec"), media: "application/grpc-web+json")
    status, headers, body = read_response(socket)
    expect(status).to eq(415)
    expect(headers.fetch("connection")).to eq("Keep-Alive")
    expect(body).to eq("")
    expect(arrivals.pop(timeout: 1)).to eq("application/grpc-web+json")
    expect(inputs.pop(timeout: 1).bytes).to eq(0)

    message = EchoExample::Message.new(name: "same socket")
    send_request(socket, frame(message.to_proto), chunked: false)
    status, _headers, body = read_response(socket)
    expect(status).to eq(200)
    expect(body).to eq(frame(message.to_proto) + frame("grpc-status: 0\r\n", 128))
    expect(arrivals.pop(timeout: 1)).to eq("application/grpc-web+proto")
    expect(inputs.pop(timeout: 1).bytes).to eq(5 + message.to_proto.bytesize)
  end

  [3, 200_000].each do |size|
    it "preserves the same keepalive socket after rejecting a #{size}-byte chunked compressed message" do
      with_socket do |socket|
        send_request(socket, frame("x" * size, 1))
        expect_compression_rejection(socket)
        expect_followups(socket)
      end
    end
  end

  it "preserves Content-Length rejection and followups on the same keepalive socket" do
    with_socket do |socket|
      send_request(socket, frame("x" * 200_000, 1), chunked: false)
      expect_compression_rejection(socket)
      expect_followups(socket)
    end
  end

  [true, false].each do |chunked|
    %w[application/grpc-web+proto application/grpc-web+json].each do |media|
      it "finishes #{chunked ? 'chunked' : 'Content-Length'} #{media} rejection with a clean connection close" do
        with_socket do |socket|
          send_request(socket, frame("x" * 200_000, 1), media: media, chunked: chunked, close: true)
          if media.end_with?("+proto")
            expect_compression_rejection(socket)
          else
            expect(read_response(socket).first).to eq(415)
            expect(arrivals.pop(timeout: 1)).to eq(media)
            expect(inputs.pop(timeout: 1).bytes).to eq(0)
          end
          expect(socket.read(1)).to be_nil
        end
      end
    end
  end

  it "cleans up a partial chunked input when the app raises a StandardError" do
    failures = Queue.new
    failing = lambda do |env|
      failures << env.fetch("rack.input").read(5)
      raise "ordinary app failure"
    end
    with_socket(failing) do |socket|
      body = frame("x" * 200_000)
      send_request(socket, body, close: true)
      expect(failures.pop(timeout: 1)).to eq(body.byteslice(0, 5))
      expect(read_response(socket).first).to eq(500)
      expect(socket.read(1)).to be_nil
    end
  end

  it "preserves already-consumed chunked and Content-Length success responses" do
    with_socket do |socket|
      [true, false].each do |chunked|
        message = EchoExample::Message.new(name: "consumed")
        send_request(socket, frame(message.to_proto), chunked: chunked)
        status, _headers, body = read_response(socket)
        expect(status).to eq(200)
        expect(body).to eq(frame(message.to_proto) + frame("grpc-status: 0\r\n", 128))
      end
    end
  end
end
