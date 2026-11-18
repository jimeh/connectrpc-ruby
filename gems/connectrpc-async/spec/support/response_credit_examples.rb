# frozen_string_literal: true

require_relative "ingress_peer"

# A host whose response outputs share a peer's connection window. The peer
# keeps protocol-http2's default 65,535-byte connection window and grants
# credit only by hand, so every response output waits on the same credit.
RSpec.shared_examples "hosted HTTP/2 shared response credit" do
  include IngressPeer

  it "sends only the connection credit granted to several waiting responses" do
    reactor do |task|
      size = 131_072
      app = ->(_body, _read_next, _route) { [200, {}, ["z" * size]] }
      with_ingress_host(app) do |_host, origin|
        with_ingress_client(task, origin, :h2c) do |connection, wire|
          write_frame = connection.method(:write_frame)
          withheld = true
          # Drops the peer's automatic WINDOW_UPDATEs until the end.
          connection.define_singleton_method(:write_frame) do |frame|
            next if withheld && frame.is_a?(Protocol::HTTP2::WindowUpdateFrame)

            super(frame)
          end
          responses = Array.new(3) do
            response = connection.create_response
            response.instance_variable_set(:@request, Protocol::HTTP::Request["GET", "/download", {}, nil])
            response.stream.send_headers([[":method", "GET"], [":scheme", "http"],
                                          [":authority", URI(origin).authority], [":path", "/download"]],
                                         Protocol::HTTP2::END_STREAM)
            response
          end
          responses.each { |response| connection.read_response(response) }
          sent = -> { wire[:data].values.sum }
          ingress_wait(task, "the connection window to fill", state: -> { wire }) { sent.call == 65_535 }
          # Every output now waits for connection credit. One frame's worth
          # wakes them all, and only one of them may spend it.
          ingress_barrier(task, connection, wire)
          credit = Protocol::HTTP2::WindowUpdateFrame.new(0)
          credit.pack(16_384)
          write_frame.call(credit)
          ingress_wait(task, "the granted frame", state: -> { wire }) do
            sent.call == 65_535 + 16_384 || !wire[:resets].empty?
          end
          ingress_barrier(task, connection, wire)
          expect([sent.call, wire[:resets]]).to eq([65_535 + 16_384, {}])

          withheld = false
          ([0] + responses.map { |response| response.stream.id }).each do |id|
            frame = Protocol::HTTP2::WindowUpdateFrame.new(id)
            frame.pack(1 << 20)
            write_frame.call(frame)
          end
          expect(responses.map { |response| response.read.bytesize }).to eq([size] * 3)
          expect(wire[:resets]).to be_empty
        end
      end
    end
  end
end
