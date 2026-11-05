# frozen_string_literal: true

require_relative "support/http1_client_helpers"

RSpec.describe ConnectRPC::Async::HTTP1Transport do
  include HTTP1ClientHelpers

  it "reuses one exclusively owned connection after fixed HTTP EOF and writer success" do
    with_http1_peer(response: http1_wire) do |transport, origin, accepted, requests|
      3.times { expect(transport.call(request("#{origin}/echo", body: "input")).body).to eq("ok") }
      expect(accepted.size).to eq(1)
      expect(requests.size).to eq(3)
      expect(requests.map { |entry| entry[3] }).to eq(["HTTP/1.1"] * 3)
    end
  end

  it "sends GET with no body or synthetic content headers" do
    with_http1_peer(response: http1_wire) do |transport, origin, _accepted, requests|
      expect(transport.call(request("#{origin}/safe?message=x", method: "GET")).body).to eq("ok")
      incoming = requests.fetch(0)
      expect(incoming[1..3]).to eq(["GET", "/safe?message=x", "HTTP/1.1"])
      expect(incoming[4].header.map(&:first)).not_to include("content-length", "transfer-encoding", "content-type")
      expect(incoming.last).to be_nil
    end
  end

  it "snapshots repeated leading headers before native chunked trailers mutate the carrier" do
    wire = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nX-Head: a\r\nX-Head: b\r\n\r\n" \
           "2\r\nok\r\n0\r\nX-Head: evil\r\nX-Trailer: tail\r\n\r\n"
    with_http1_peer(response: wire) do |transport, origin, accepted|
      2.times do
        response = transport.call(request(origin))
        expect(response.body).to eq("ok")
        expect(response.headers["x-head"]).to eq(%w[a b])
        expect(response.headers).not_to have_key("x-trailer")
      end
      expect(accepted.size).to eq(1)
    end
  end

  it "accepts exactly sixteen interim responses and counts only the final response" do
    wire = ("HTTP/1.1 100 Continue\r\n\r\n" * 16) + http1_wire
    with_http1_peer(response: wire) do |transport, origin|
      expect(transport.call(request(origin)).body).to eq("ok")
      expect(transport.instance_variable_get(:@idle).first.connection.count).to eq(1)
    end
  end

  it "uses native chunked framing for a case-insensitive transfer coding" do
    wire = "HTTP/1.1 200 OK\r\nTransfer-Encoding: ChUnKeD\r\n\r\n2\r\nok\r\n0\r\n\r\n"
    with_http1_peer(response: wire) do |transport, origin, accepted|
      2.times { expect(transport.call(request(origin)).body).to eq("ok") }
      expect(accepted.size).to eq(1)
    end
  end

  ["HTTP/1.0 200 OK", "HTTP/2.0 200 OK", "HTTP/1.1 +200 OK", "HTTP/1.1 0200 OK",
   "HTTP/1.1 0xc8 OK", "HTTP/1.1 99 OK", "HTTP/1.1 8000 OK", "HTTP/1.1\t200 OK",
   "HTTP/1.1 200OK", "HTTP/1.1 200", "HTTP/1.1 200 Bad\x00Reason", "HTTP/1.1 200 Bad\x01Reason",
   "HTTP/1.1 200 Bad\x7fReason", "HTTP/1.1 200 Bad\rReason", "HTTP/1.1 200 Bad\nReason",
   "HTTP/1.1 101 Switching Protocols"].each do |status|
    it "rejects status #{status.inspect} without reusing the lease" do
      with_http1_peer(response: http1_wire(status: status)) do |transport, origin, accepted|
        2.times do
          expect { transport.call(request(origin)) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
        end
        expect(accepted.size).to eq(2)
        expect(transport.instance_variable_get(:@leases)).to be_empty
      end
    end
  end

  it "accepts HTAB in the reason phrase and keeps the framed connection reusable" do
    with_http1_peer(response: http1_wire(status: "HTTP/1.1 200 A\tReason")) do |transport, origin, accepted|
      2.times { expect(transport.call(request(origin)).body).to eq("ok") }
      expect(accepted.size).to eq(1)
    end
  end

  [200, 400].each do |status|
    it "accepts an Upgrade advertisement on an ordinary #{status} response and permits subsequent healthy calls" do
      response = lambda do |count, _|
        if count == 1
          http1_wire("advertised", status: "HTTP/1.1 #{status} Ordinary",
                                   fields: "Connection: upgrade\r\nUpgrade: h2c\r\n")
        else
          http1_wire
        end
      end
      with_http1_peer(response: response) do |transport, origin, accepted|
        advertised = transport.call(request(origin))
        expect(advertised.status).to eq(status)
        expect(advertised.body).to eq("advertised")
        expect(transport.call(request(origin)).body).to eq("ok")
        expect(transport.call(request(origin)).body).to eq("ok")
        # Complete non-200 responses deliberately retire, independently of Upgrade.
        expect(accepted.size).to eq(status == 200 ? 1 : 2)
      end
    end
  end

  it "preserves three-digit extension statuses for core HTTP fallback" do
    with_http1_peer(response: http1_wire(status: "HTTP/1.1 800 Extension")) do |transport, origin|
      expect(transport.call(request(origin)).status).to eq(800)
    end
  end

  ["Content-Length: 2\r\nContent-Length: 2\r\n", "Content-Length: 2, 2\r\n",
   "Content-Length: 2\r\nTransfer-Encoding: chunked\r\n", "Transfer-Encoding: gzip, chunked\r\n",
   "Transfer-Encoding: chunked, chunked\r\n", "Transfer-Encoding: gzip\r\n",
   "Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n"].each do |fields|
    it "rejects ambiguous or unsupported framing #{fields.inspect}" do
      with_http1_peer(response: "HTTP/1.1 200 OK\r\n#{fields}\r\n2\r\nok\r\n0\r\n\r\n") do |transport, origin|
        expect { transport.call(request(origin)) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
      end
    end
  end

  it "rejects a seventeenth interim head" do
    with_http1_peer(response: ("HTTP/1.1 103 Early Hints\r\n\r\n" * 17) + http1_wire) do |transport, origin|
      expect { transport.call(request(origin)) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
    end
  end

  it "retires a framed response with extra buffered bytes rather than treating them as a later response" do
    with_http1_peer(response: "#{http1_wire}unsolicited") do |transport, origin, accepted|
      2.times { expect(transport.call(request(origin)).body).to eq("ok") }
      expect(accepted.size).to eq(2)
    end
  end

  it "rejects oversized declared and incremental bodies" do
    [http1_wire("large"), "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nlarge\r\n0\r\n\r\n"].each do |wire|
      with_http1_peer(response: wire) do |transport, origin|
        expect { transport.call(request(origin, limit: 4)) }.to raise_error(ConnectRPC::Error) { |e|
          expect(e.code).to eq("resource_exhausted")
        }
      end
    end
  end

  it "advertises only Connect SS/CS, with no gRPC entry points" do
    reactor do
      transport = described_class.new(origin: "http://localhost")
      expect(transport.streaming_capabilities).to eq(%i[server_stream client_stream])
      expect(transport).not_to respond_to(:open_grpc_stream, :grpc_capabilities)
      expect(transport).not_to be_a(ConnectRPC::Async::Transport)
    ensure
      transport&.close
    end
  end

  ["HTTP/1.1 200 #{'x' * 8192}\r\n\r\n",
   "HTTP/1.1 200 OK\r\nX-Long: #{'x' * 8192}\r\n\r\n",
   "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n#{'1' * 8192}\r\n"].each_with_index do |wire, index|
    it "retains the native 8192-byte #{%w[status header chunk][index]} line bound" do
      with_http1_peer(response: wire) do |transport, origin|
        expect { transport.call(request(origin)) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
      end
    end
  end
end
