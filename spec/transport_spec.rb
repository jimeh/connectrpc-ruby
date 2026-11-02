# frozen_string_literal: true

require "connectrpc/transports/net_http"
require_relative "support/socket_peer"

RSpec.describe ConnectRPC::Transports::NetHTTP do
  include SocketPeer

  let(:transport) { described_class.new }

  def owned_threads
    # Ruby 3.4 Net::HTTP lazily starts Timeout's process-wide service thread.
    # It is not a call-owned watchdog; every other new worker must disappear.
    Thread.list.reject { |thread| thread.name == "Timeout stdlib thread" }
  end

  it "captures separate repeated raw fields and explicitly asks for identity" do
    observed = Queue.new
    peer = lambda do |socket|
      observed << read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nX-Repeat: one,two\r\nX-Repeat: three\r\n" \
                   "Trailer-X-End: a\r\nTrailer-X-End: b\r\nContent-Length: 2\r\n\r\nok")
      expect(socket.read).to eq("")
    end
    with_peer(peer) do |url|
      result = transport.call(transport_request(url))
      expect(result.body).to eq("ok")
      expect(result.headers["x-repeat"]).to eq(["one,two", "three"])
      expect(result.headers["trailer-x-end"]).to eq(%w[a b])
      expect(observed.pop(timeout: 1)).to include("Accept-Encoding: identity\r\n", "Connection: close\r\n")
    end
  end

  it "keeps TLS peer/hostname verification enabled, ignores proxy env, and disables retries" do
    http = transport.send(:connection, URI("https://example.invalid"), ConnectRPC::Deadline.new(1))
    expect(http.proxy?).to be(false)
    expect(http.verify_mode).to eq(OpenSSL::SSL::VERIFY_PEER)
    expect(http.verify_hostname).to be(true)
    expect(http.max_retries).to eq(0)
    expect(http.ignore_eof).to be(false)
  end

  it "closes and stops reading an unknown-length response at its byte limit" do
    peer = lambda do |socket|
      read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nConnection: close\r\n\r\noversized")
      expect(socket.read).to eq("")
    end
    with_peer(peer) do |url|
      expect { transport.call(transport_request(url, limit: 3)) }.to raise_error(ConnectRPC::Error) do |error|
        expect(error.code).to eq("resource_exhausted")
      end
    end
  end

  it "rejects truncated bodies and does not follow redirects" do
    ["HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nx",
     "HTTP/1.1 302 Found\r\nLocation: http://example.invalid\r\nContent-Length: 0\r\n\r\n"].each do |wire|
      peer = lambda do |socket|
        read_request(socket)
        socket.write(wire)
      end
      with_peer(peer) do |url|
        if wire.include?("302")
          expect(transport.call(transport_request(url)).status).to eq(302)
        else
          expect { transport.call(transport_request(url)) }.to raise_error(ConnectRPC::Error) do |error|
            expect(error.code).to eq("unavailable")
          end
        end
      end
    end
  end

  it "expires a stalled response body and closes its connection and watchdog" do
    progress = Queue.new
    peer = lambda do |socket|
      read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nx")
      progress << :body_started
      expect(socket.read).to eq("")
    end
    before = owned_threads
    with_peer(peer) do |url|
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expect { transport.call(transport_request(url, timeout: 0.2)) }.to raise_error(ConnectRPC::Error) do |error|
        expect(error.code).to eq("deadline_exceeded")
      end
      expect(progress.pop(timeout: 1)).to eq(:body_started)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1.0
    end
    expect(owned_threads - before).to be_empty
  end

  it "bounds aggregate response-header trickle under one deadline" do
    progress = Queue.new
    peer = lambda do |socket|
      read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\n")
      progress << :headers_started
      # Elapsed time is the external I/O behavior under test. Readability here
      # detects client closure; its timeout paces real network header delivery.
      20.times do |index|
        break if socket.wait_readable(0.04)

        socket.write("X-Trickle-#{index}: value\r\n")
      end
      socket.write("Content-Length: 2\r\n\r\nok") unless socket.wait_readable(0)
      expect(socket.read).to eq("")
    end
    before = owned_threads
    with_peer(peer) do |url|
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expect { transport.call(transport_request(url, timeout: 0.2)) }.to raise_error(ConnectRPC::Error) do |error|
        expect(error.code).to eq("deadline_exceeded")
      end
      expect(progress.pop(timeout: 1)).to eq(:headers_started)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 0.65
    end
    expect(owned_threads - before).to be_empty
  end

  it "bounds an outbound write blocked by a peer that has started reading" do
    progress = Queue.new
    release = Queue.new
    peer = lambda do |socket|
      socket.readpartial(1024)
      progress << :request_started
      raise "Client never released peer" unless release.pop(timeout: 3)

      socket.read
    end
    before = owned_threads
    begin
      with_peer(peer) do |url|
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        expect do
          transport.call(transport_request(url, timeout: 0.2, body: "x" * (32 * 1024 * 1024)))
        end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
        expect(progress.pop(timeout: 1)).to eq(:request_started)
        expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1.0
        release << true
      end
    ensure
      release << true
    end
    expect(owned_threads - before).to be_empty
  end
end
