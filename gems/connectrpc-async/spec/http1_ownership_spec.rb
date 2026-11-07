# frozen_string_literal: true

require_relative "support/http1_client_helpers"

RSpec.describe ConnectRPC::Async::HTTP1Transport, "exclusive lease ownership" do
  include HTTP1ClientHelpers

  def error_code(code, &)
    expect(&).to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(code) }
  end

  it "exposes the flushed request head before response headers and aborts without a normal chunk terminator" do
    head = Async::Promise.new
    wire = Async::Promise.new
    peer = lambda do |native, socket, _raw, _index|
      incoming = native.read_request
      head.resolve(incoming[3])
      bytes = +"".b
      bytes << socket.readpartial(16_384) until socket.eof?
    rescue EOFError
      nil
    ensure
      wire.resolve(bytes)
    end
    with_raw_http1(peer: peer) do |transport, origin|
      transport.open_stream(stream_request(origin)) do |exchange|
        expect(head.wait).to eq("HTTP/1.1")
        exchange.write("payload")
        exchange.abort
      end
      expect(wire.wait).not_to end_with("0\r\n\r\n")
      expect(transport.instance_variable_get(:@leases)).to be_empty
    end
  end

  it "keeps a healthy independent lease available beside a long stream and rejects full admission" do
    started = Async::Promise.new
    release = Async::Promise.new
    peer = lambda do |native, socket, _raw, index|
      loop do
        incoming = native.read_request
        incoming.last&.each { |_| nil }
        if index == 1
          socket.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n")
          started.resolve(nil)
          release.wait
          socket.write("0\r\n\r\n")
        else
          socket.write(http1_wire)
        end
        native.send_end_stream!
      end
    end
    with_raw_http1(peer: peer, connection_limit: 2, max_requests: 2) do |transport, origin, accepted, task|
      stream = task.async do
        transport.open_stream(stream_request(origin)) do |exchange|
          exchange.close_send
          expect(exchange.read(1)).to eq("x")
          release.wait
          expect(exchange.read(1)).to be_nil
          exchange.finish
          exchange.abort # Completed finish remains reusable.
        end
      end
      started.wait
      queued = task.async { transport.call(request(origin)) }
      expect(queued.wait.body).to eq("ok")
      expect(transport.call(request(origin)).body).to eq("ok")
      expect(accepted.size).to eq(2)
      release.resolve(nil)
      stream.wait
      expect(transport.instance_variable_get(:@idle).size).to eq(2)
      expect(transport.close).to be(true)
    end
  end

  it "counts lease waits in admission and expires a queued call without another dial" do
    head = Async::Promise.new
    held = Async::Promise.new
    peer = lambda do |native, _socket, _raw, _index|
      native.read_request
      head.resolve(nil)
      held.wait
    end
    with_raw_http1(peer: peer, connection_limit: 1, max_requests: 2) do |transport, origin, accepted, task|
      active = task.async do
        transport.open_stream(stream_request(origin)) { held.wait }
      end
      head.wait
      queued = task.async do
        error_code("deadline_exceeded") { transport.call(request(origin, deadline: ConnectRPC::Deadline.new(0.05))) }
      end
      wait_until(task, "queued admission") { transport.instance_variable_get(:@calls).size == 2 }
      error_code("unavailable") { transport.call(request(origin)) }
      queued.wait
      expect(accepted.size).to eq(1)
      expect(transport.instance_variable_get(:@leases).size).to eq(1)
      held.resolve(nil)
      active.wait
      expect(transport.instance_variable_get(:@leases)).to be_empty
    end
  end

  [false, true].each do |tls|
    it "lets a complete unary error stop an unfinished upload over #{tls ? 'TLS' : 'TCP'}" do
      head = Async::Promise.new
      release = Async::Promise.new
      peer = lambda do |native, socket, _raw, _index|
        native.read_request
        head.resolve(nil)
        socket.write(http1_wire('{"code":"invalid_argument"}', fields: "Content-Type: application/json\r\n",
                                                               status: "HTTP/1.1 400 Bad Request"))
        release.wait
      end
      with_raw_http1(peer: peer, tls: tls) do |transport, origin|
        response = transport.call(request(origin, body: "x" * 16_777_216, deadline: ConnectRPC::Deadline.new(2)))
        expect(head.wait).to be_nil
        expect(response.status).to eq(400)
        expect(response.body).to eq('{"code":"invalid_argument"}')
        resource = transport.instance_variable_get(:@resources).last
        expect(resource.raw).to be_closed
        expect(transport.instance_variable_get(:@leases)).to be_empty
      ensure
        release.resolve(nil)
      end
    end
  end

  ["HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nx",
   "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nx",
   "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n"].each do |wire|
    it "rejects truncated native framing #{wire.inspect}" do
      peer = lambda { |native, socket, *, **|
        native.read_request
        socket.write(wire)
      }
      with_raw_http1(peer: peer) do |transport, origin|
        error_code("unavailable") { transport.call(request(origin)) }
      end
    end
  end

  [false, true].each do |tls|
    it "accepts clean close-delimited unary EOF over #{tls ? 'TLS' : 'TCP'} and never reuses it" do
      peer = lambda do |native, socket, _raw, _index|
        native.read_request
        socket.write("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nok")
        socket.close
      end
      with_raw_http1(peer: peer, tls: tls) do |transport, origin, accepted|
        2.times { expect(transport.call(request(origin)).body).to eq("ok") }
        expect(accepted.size).to eq(2)
      end
    end
  end

  it "rejects abrupt TLS EOF rather than accepting a close-delimited payload" do
    peer = lambda do |native, socket, raw, _index|
      native.read_request
      socket.write("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nok")
      raw.close
    end
    with_raw_http1(peer: peer, tls: true) do |transport, origin|
      error_code("unavailable") { transport.call(request(origin)) }
    end
  end

  it "accepts verified legacy TLS without ALPN" do
    peer = lambda do |native, socket, _raw, _index|
      native.read_request
      expect(socket.alpn_protocol).to be_nil
      socket.write(http1_wire)
      native.send_end_stream!
      socket.readpartial(1)
    end
    with_raw_http1(peer: peer, tls: true, alpn: nil) do |transport, origin|
      expect(transport.call(request(origin)).body).to eq("ok")
    end
  end

  it "rejects a malicious h2 ALPN selection before sending any request" do
    observed = []
    peer = lambda do |native, *_|
      incoming = native.read_request
      observed << incoming if incoming
    end
    with_raw_http1(peer: peer, tls: true, alpn: "h2") do |transport, origin|
      error_code("unavailable") { transport.call(request(origin)) }
      expect(observed).to be_empty
    end
  end
end
