# frozen_string_literal: true

require_relative "../lib/connectrpc/async/http1_stream"

RSpec.describe AsyncHelpers::INTERNAL::HTTP1Stream, "TLS read failures" do
  def with_tls_peer(peer_behavior)
    with_ca do |ca_file|
      reactor do |task|
        listener = TCPServer.new("127.0.0.1", 0)
        release = Async::Promise.new
        peer = task.async(finished: false) do
          raw = listener.accept
          ssl = AsyncHelpers::INTERNAL::TLS.server(**host_tls, alpn_protocols: ["http/1.1"]).socket(raw)
          ssl.accept
          peer_behavior.call(ssl, raw)
          release.wait
        ensure
          raw&.close unless raw&.closed?
        end
        raw = TCPSocket.new("127.0.0.1", listener.addr[1])
        socket = AsyncHelpers::INTERNAL::TLS.client(ca_file: ca_file, certificate_chain: nil, private_key: nil,
                                                    alpn_protocols: ["http/1.1"]).socket(raw)
        socket.hostname = "localhost"
        socket.connect
        socket.post_connection_check("localhost")
        yield described_class.new(socket)
      ensure
        raw&.close unless raw&.closed?
        release&.resolve(nil)
        AsyncHelpers::INTERNAL.join(peer)
        listener&.close
      end
    end
  end

  it "raises a corrupt record instead of returning the data before it as a clean EOF" do
    peer = lambda do |ssl, raw|
      ssl.write("HTTP/1.1 200 OK\r\n\r\npartial")
      # A well-formed application-data record header whose payload cannot
      # authenticate under the negotiated session keys.
      raw.write("\x17\x03\x03\x00\x20".b + ("\x00".b * 32))
    end
    with_tls_peer(peer) do |stream|
      expect { stream.read(1_000) }.to raise_error(OpenSSL::SSL::SSLError)
    end
  end

  it "maps a peer close without close_notify to a connection reset" do
    with_tls_peer(->(_ssl, raw) { raw.close }) do |stream|
      expect { stream.read(1) }.to raise_error(IO::Stream::ConnectionResetError) { |error|
        expect(error.message).to eq("Connection reset by peer - Connection reset by peer!")
      }
    end
  end
end

RSpec.describe AsyncHelpers::INTERNAL::HTTP1Stream, "buffering semantics" do
  let(:pair) { Socket.pair(:UNIX, :STREAM, 0) }
  let(:socket) { pair.first }
  let(:peer) { pair.last }
  let(:stream) { described_class.new(socket) }

  after do
    pair.each { |end_point| end_point.close unless end_point.closed? }
  end

  it "finds a CRLF separator split across two socket reads" do
    reads = []
    allow(socket).to receive(:read_nonblock).and_wrap_original do |original, *arguments, **options|
      original.call(*arguments, **options).tap { |result| reads << result.dup if result.is_a?(String) }
    end
    reactor do |task|
      peer.write("abc\r")
      line = task.async { stream.gets("\r\n", 1_024) }
      wait_until(task, "first socket read") { reads.any? }
      peer.write("\ndef")
      expect(line.wait).to eq("abc\r\n")
      expect(reads.first).to eq("abc\r")
      expect(stream.read(3)).to eq("def")
    end
  end

  it "compares the separator start, not the line end, with the line limit" do
    peer.write("abc\r\nabcd\r\n")
    peer.close
    expect(stream.gets("\r\n", 4)).to eq("abc\r\n")
    expect(stream.gets("\r\n", 4)).to eq("abcd")
  end

  it "returns an unterminated line tail at EOF, then nil" do
    peer.write("tail")
    peer.close
    expect(stream.gets("\r\n", 1_024)).to eq("tail")
    expect(stream.gets("\r\n", 1_024)).to be_nil
  end

  it "returns a short sized-read tail at EOF, then nil, while a zero-length read stays empty" do
    peer.write("tail")
    peer.close
    expect(stream.read(10)).to eq("tail")
    expect(stream.read(10)).to be_nil
    expect(stream.read(0)).to eq("")
  end

  it "buffers non-ASCII UTF-8 and binary writes together as raw bytes" do
    text = "café"
    bytes = "\xFF\x00".b
    stream.write(text)
    stream.write(bytes, flush: true)
    expect(peer.read(text.bytesize + bytes.bytesize)).to eq(text.b + bytes)
  end

  it "caps each readpartial at 16 KiB and raises EOFError once drained" do
    peer.write("x" * 20_000)
    peer.close
    expect(stream.readpartial(65_536).bytesize).to eq(16_384)
    expect(stream.readpartial(65_536).bytesize).to eq(3_616)
    expect { stream.readpartial(65_536) }.to raise_error(EOFError)
  end
end
