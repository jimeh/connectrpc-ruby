# frozen_string_literal: true

require_relative "support/http1_client_helpers"

# io-stream 0.14 Buffered#sysread returns nil for non-EOF SSL errors, which a
# close-delimited body would read as a clean end. HTTP1Stream#sysread keeps them.
RSpec.describe "HTTP/1.1 client TLS read errors" do
  include HTTP1ClientHelpers

  it "fails a close-delimited response when TLS reports a non-EOF error instead of reading a clean EOF" do
    peer = lambda do |native, socket, raw, _index|
      native.read_request
      socket.write("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nok")
      socket.flush
      # An application-data record that cannot be decrypted: "bad record mac".
      raw.write("\x17\x03\x03\x00\x20".b + ("\x00".b * 32))
      raw.close
    end
    with_raw_http1(peer: peer, tls: true) do |transport, origin|
      expect { transport.call(request(origin)) }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("unavailable")
      }
    end
  end
end
