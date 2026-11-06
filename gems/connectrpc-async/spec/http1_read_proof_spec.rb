# frozen_string_literal: true

require_relative "../lib/connectrpc/async/http1_stream"

RSpec.describe "HTTP/1 independent response read proof" do
  [false, true].each do |tls|
    it "reads a response with an upload blocked over #{tls ? 'verified TLS' : 'TCP'}" do
      with_ca do |ca_file|
        reactor do |task|
          listener = TCPServer.new("127.0.0.1", 0)
          head = Async::Promise.new
          respond = Async::Promise.new
          sent = Async::Promise.new
          release = Async::Promise.new
          peer = task.async(finished: false) do
            raw = listener.accept
            socket = raw
            if tls
              snapshot = AsyncHelpers::INTERNAL::TLS.server(**host_tls, alpn_protocols: ["http/1.1"])
              socket = snapshot.socket(raw)
              socket.accept
            end
            bytes = +""
            bytes << socket.readpartial(1) until bytes.end_with?("\r\n\r\n")
            head.resolve(bytes)
            respond.wait
            socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            sent.resolve(nil)
            release.wait
          ensure
            raw&.close
          end
          raw = TCPSocket.new("127.0.0.1", listener.addr[1])
          raw.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF, 4096)
          socket = raw
          if tls
            snapshot = AsyncHelpers::INTERNAL::TLS.client(ca_file: ca_file, certificate_chain: nil, private_key: nil,
                                                          alpn_protocols: ["http/1.1"])
            socket = snapshot.socket(raw)
            socket.hostname = "localhost"
            socket.connect
            socket.post_connection_check("localhost")
            expect(socket.alpn_protocol).to eq("http/1.1")
          end
          stream = AsyncHelpers::INTERNAL::HTTP1Stream.new(socket)
          stream.write("POST / HTTP/1.1\r\nhost: localhost\r\ncontent-length: 16777216\r\n\r\n")
          stream.flush
          expect(head.wait).to start_with("POST / HTTP/1.1")
          writer = task.async(finished: false) do
            stream.write("x" * 16_777_216, flush: true)
          rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
            nil
          end
          respond.resolve(nil)
          sent.wait
          expect(writer).not_to be_finished
          result = task.with_timeout(1) { stream.read("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".bytesize) }
          expect(result).to eq("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
          expect(writer).not_to be_finished
        ensure
          raw&.shutdown(Socket::SHUT_RDWR) unless raw&.closed?
          raw&.close
          AsyncHelpers::INTERNAL.cancel(writer)
          AsyncHelpers::INTERNAL.join(writer)
          release&.resolve(nil)
          AsyncHelpers::INTERNAL.join(peer)
          listener&.close
        end
      end
    end
  end
end

RSpec.describe "HTTP/1 TLS read after a reported write failure probe" do
  it "records the actual OpenSSL result after a real peer close" do
    with_ca do |ca_file|
      reactor do |task|
        listener = TCPServer.new("127.0.0.1", 0)
        closed = Async::Promise.new
        peer = task.async(finished: false) do
          accepted = listener.accept
          ssl = AsyncHelpers::INTERNAL::TLS.server(**host_tls, alpn_protocols: ["http/1.1"]).socket(accepted)
          ssl.accept
          ssl.write("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
          ssl.close
          closed.resolve(nil)
        ensure
          accepted&.close unless accepted&.closed?
        end
        raw = TCPSocket.new("127.0.0.1", listener.addr[1])
        socket = AsyncHelpers::INTERNAL::TLS.client(ca_file: ca_file, certificate_chain: nil, private_key: nil,
                                                    alpn_protocols: ["http/1.1"]).socket(raw)
        socket.hostname = "localhost"
        socket.connect
        socket.post_connection_check("localhost")
        stream = AsyncHelpers::INTERNAL::HTTP1Stream.new(socket)
        closed.wait
        failure = nil
        begin
          1024.times { stream.write("x" * 16_384, flush: true) }
        rescue IOError, SystemCallError, OpenSSL::SSL::SSLError => e
          failure = e
        end
        expect(failure).to be_a(Exception)
        begin
          bytes = stream.readpartial(16_384)
          expect(bytes).to start_with("HTTP/1.1 400 Bad Request\r\n")
          warn "TLS WRITE-FAILURE PROBE Ruby #{RUBY_VERSION}: " \
               "write=#{failure.class}: #{failure.message}; read=#{bytes.inspect}"
        rescue IOError, SystemCallError, OpenSSL::SSL::SSLError => e
          warn "TLS WRITE-FAILURE PROBE Ruby #{RUBY_VERSION}: " \
               "write=#{failure.class}: #{failure.message}; read=#{e.class}: #{e.message}"
          expect(e).to be_a(IOError).or be_a(SystemCallError).or be_a(OpenSSL::SSL::SSLError)
        end
      ensure
        raw&.close unless raw&.closed?
        socket&.close
        AsyncHelpers::INTERNAL.join(peer)
        listener&.close
      end
    end
  end
end
