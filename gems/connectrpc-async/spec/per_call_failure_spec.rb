# frozen_string_literal: true

require_relative "support/per_call_helpers"

RSpec.describe "PerCallTransport failures, error mapping and the refused resend" do
  include PerCallHelpers

  def goaway(server, last_stream_id)
    frame = Protocol::HTTP2::GoawayFrame.new
    frame.pack(last_stream_id, Protocol::HTTP2::Error::NO_ERROR, "")
    server.write_frame(frame)
  end

  # Refuses the first stream as `kind` says and answers later ones.
  def refusing_peer(kind)
    seen = Hash.new(0)
    threaded_recording_peer do |incoming|
      if (seen[incoming.path] += 1) > 1
        incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
      else
        case kind
        when :reset then incoming.stream.send_reset_stream(Protocol::HTTP2::Error::REFUSED_STREAM)
        when :goaway then goaway(incoming.stream.connection, 0)
        when :headers
          incoming.stream.send_headers([[":status", "200"]])
          incoming.stream.send_reset_stream(Protocol::HTTP2::Error::REFUSED_STREAM)
        end
      end
    end
  end

  # A TLS peer that selects HTTP/1.1 whatever the client offers.
  def http1_alpn_peer
    ThreadedHelpers::PeerThread.new do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      context = OpenSSL::SSL::SSLContext.new
      leaf = tls_material.fetch(:leaf)
      context.add_certificate(leaf.first, leaf.last, [])
      context.alpn_select_cb = ->(_offers) { "http/1.1" }
      acceptor = task.async do
        loop do
          raw, = listener.accept
          task.async do
            ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
            ssl.sync_close = true
            ssl.accept
            ssl.read(1)
          rescue OpenSSL::SSL::SSLError, IOError, SystemCallError
            nil
          ensure
            AsyncHelpers::INTERNAL.raw_close(raw)
          end
        end
      end
      acceptor.define_singleton_method(:port) { listener.local_address.ip_port }
      [acceptor, lambda {
        acceptor.stop
        bound.close
      }]
    end
  end

  def failure(path, transport, url)
    with_boundaries do |boundaries|
      error = on_path(path) { transport.call(request(url)) }
      observation = boundaries.take.first
      [error, observation]
    end
  end

  PerCallHelpers::PATHS.each do |path|
    context "on the #{path} path" do
      it "maps a refused connection to unavailable after one attempt" do
        socket = refusing_socket
        port = socket.local_address.ip_port
        error, observation = failure(path, per_call, "http://127.0.0.1:#{port}/refused")
        expect([error.class, error.code, error.message])
          .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport failed: connection refused"])
        expect(error.cause).to be_a(Errno::ECONNREFUSED)
        expect_released(observation, dials: 0)
      ensure
        socket&.close
      end

      it "fails fast on a connection the peer closes during setup, without a second attempt" do
        peer = ThreadedHelpers::PeerThread.new do |task|
          bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
          recording = ConnectionPeer::RecordingServer.new(task, bound.sockets.first, refuse: [1, 2])
          recording.define_singleton_method(:port) { bound.sockets.first.local_address.ip_port }
          [recording, lambda {
            recording.stop
            bound.close
          }]
        end
        error, observation = failure(path, per_call, "http://127.0.0.1:#{peer.peer.port}/closed")
        expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
        expect(error.message).to start_with("HTTP/2 transport failed")
        expect(peer.peer.accepted).to eq(1)
        expect_released(observation)
      ensure
        peer&.stop
      end

      it "maps an untrusted server certificate to TLS verification failed using system roots" do
        peer = threaded_host(server: server, tls: host_tls)
        error, observation = failure(path, per_call, "https://127.0.0.1:#{peer.peer.port}/untrusted")
        expect([error.class, error.code, error.message])
          .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport failed: TLS verification failed"])
        expect_released(observation)
      ensure
        peer&.stop
      end

      it "maps a server without h2 ALPN to ALPN negotiation failed" do
        peer = http1_alpn_peer
        transport = with_ca { |ca_file| per_call(ca_file: ca_file) }
        error, observation = failure(path, transport, "https://127.0.0.1:#{peer.peer.port}/alpn")
        expect([error.class, error.code, error.message])
          .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport failed: ALPN negotiation failed"])
        expect_released(observation)
      ensure
        peer&.stop
      end

      it "resends a plainly refused unary call once on the same connection" do
        peer = refusing_peer(:reset)
        origin = "http://127.0.0.1:#{peer.peer.port}"
        with_boundaries do |boundaries|
          response = on_path(path) { per_call.call(request("#{origin}/refused")) }
          expect([response.status, response.body]).to eq([200, "/refused"])
          expect([peer.peer.accepted, peer.peer.headers_frames.size]).to eq([1, 2])
          expect_released(boundaries.take.first)
        end
      ensure
        peer&.stop
      end

      it "resends a unary call cut off by GOAWAY once on a second connection and closes both" do
        peer = refusing_peer(:goaway)
        origin = "http://127.0.0.1:#{peer.peer.port}"
        with_boundaries do |boundaries|
          response = on_path(path) { per_call.call(request("#{origin}/cutoff")) }
          expect([response.status, response.body]).to eq([200, "/cutoff"])
          expect([peer.peer.accepted, peer.peer.headers_frames.size]).to eq([2, 2])
          expect_released(boundaries.take.first, dials: 2)
        end
      ensure
        peer&.stop
      end

      it "never resends a refusal after response headers" do
        peer = refusing_peer(:headers)
        origin = "http://127.0.0.1:#{peer.peer.port}"
        with_boundaries do |boundaries|
          error = on_path(path) { per_call.call(request("#{origin}/headers")) }
          expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
          expect([peer.peer.accepted, peer.peer.headers_frames.size]).to eq([1, 1])
          expect_released(boundaries.take.first)
        end
      ensure
        peer&.stop
      end
    end
  end
end
