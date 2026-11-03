# frozen_string_literal: true

require_relative "support/ingress_peer"

RSpec.describe "Native HTTP/2 bounded ingress" do
  include IngressPeer

  %i[request response].each do |direction|
    it "keeps a nonreading #{direction} bounded while a same-connection sibling and PING complete" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
          outgoing, incoming, body = ingress_stream(client_connection, server_connection, direction)
          3.times { ingress_data(sender, outgoing.id, "a" * 16_384) }
          ingress_data(sender, outgoing.id, "b" * 16_383)
          ingress_barrier(task, sender, wire)
          expect(incoming.local_window.available).to eq(1)
          expect(ingress_backlog(body).first).to eq(65_535)
          ingress_data(sender, outgoing.id, "c")
          ingress_barrier(task, sender, wire)
          expect(incoming.local_window.available).to eq(0)
          sibling, _stream, sibling_body = ingress_stream(client_connection, server_connection, direction)
          ingress_data(sender, sibling.id, "sibling", finish: true)
          ingress_barrier(task, sender, wire)
          expect(sibling_body.read).to eq("sibling")
          expect(sibling_body.read).to be_nil
          expect(wire[:credit][outgoing.id]).to eq(0)
          expect(ingress_backlog(body)).to eq([65_536, 4])
          ingress_data(sender, outgoing.id, "", finish: true)
          ingress_barrier(task, sender, wire)
          expect(body.each.to_a.join).to eq("#{'a' * 49_152}#{'b' * 16_383}c")
          expect(body.read).to be_nil
        end
      end
    end

    it "coalesces valid tiny and empty #{direction} frames without granting queued bytes" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
          outgoing, _incoming, body = ingress_stream(client_connection, server_connection, direction)
          2048.times do
            ingress_data(sender, outgoing.id, "x")
            ingress_data(sender, outgoing.id, "")
          end
          ingress_barrier(task, sender, wire)
          expect(ingress_backlog(body)).to eq([2048, 1])
          expect(wire[:credit][outgoing.id]).to eq(0)
          3.times { ingress_data(sender, outgoing.id, "y" * 16_384) }
          ingress_data(sender, outgoing.id, "z" * 14_336, finish: true)
          ingress_barrier(task, sender, wire)
          expect(ingress_backlog(body)).to eq([65_536, 4])
          chunks = body.each.to_a
          expect(chunks.map(&:bytesize)).to eq([16_384] * 4)
          expect(chunks.join).to eq(("x" * 2048) + ("y" * 49_152) + ("z" * 14_336))
        end
      end
    end

    it "accounts padding-only #{direction} credit and excludes padding from Content-Length" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
          outgoing, incoming, body = ingress_stream(client_connection, server_connection, direction, length: 3)
          512.times { ingress_data(sender, outgoing.id, "", padding: 255) }
          ingress_barrier(task, sender, wire)
          expect(ingress_backlog(body)).to eq([0, 0])
          expect(wire[:credit][outgoing.id]).to eq(33_024 * 3)
          expect(incoming.local_window.available).to eq(65_536 + (33_024 * 3) - (512 * 256))
          ingress_data(sender, outgoing.id, "abc", padding: 7, finish: true)
          ingress_barrier(task, sender, wire)
          expect(body.length).to eq(3)
          expect(body.each.to_a.join).to eq("abc")
        end
      end
    end

    it "rejects #{direction} stream over-credit before retention and preserves its sibling" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
          outgoing, incoming, body = ingress_stream(client_connection, server_connection, direction)
          4.times { ingress_data(sender, outgoing.id, "a" * 16_384) }
          ingress_barrier(task, sender, wire)
          ingress_data(sender, outgoing.id, "!")
          ingress_barrier(task, sender, wire)
          expect(wire[:resets][outgoing.id]).to eq(Protocol::HTTP2::Error::FLOW_CONTROL_ERROR)
          expect(incoming).to be_closed
          expect(ingress_backlog(body)).to eq([0, 0])
          expect { body.read }.to raise_error(Protocol::HTTP2::StreamError) { |error|
            expect(error.code).to eq(Protocol::HTTP2::Error::FLOW_CONTROL_ERROR)
          }
          sibling, _stream, sibling_body = ingress_stream(client_connection, server_connection, direction)
          ingress_data(sender, sibling.id, "alive", finish: true)
          ingress_barrier(task, sender, wire)
          expect(sibling_body.each.to_a.join).to eq("alive")
        end
      end
    end

    it "rejects #{direction} connection over-credit with connection FLOW_CONTROL_ERROR" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, receiver, wire|
          outgoing, _incoming, body = ingress_stream(client_connection, server_connection, direction)
          sibling, _stream, sibling_body = ingress_stream(client_connection, server_connection, direction)
          # The pinned connection window starts at 65535. Withhold automatic
          # connection replenishment through its native desired-capacity policy
          # so this raw peer can exhaust it independently of either stream.
          expect(receiver.local_window.available).to eq(65_535)
          receiver.local_window.desired = 1
          3.times { ingress_data(sender, outgoing.id, "a" * 16_384) }
          ingress_data(sender, outgoing.id, "a" * 16_383)
          ingress_barrier(task, sender, wire)
          expect(receiver.local_window.available).to eq(0)
          expect(wire[:credit][0]).to eq(0)
          ingress_data(sender, sibling.id, "!")
          ingress_wait(task, "connection GOAWAY", state: -> { wire }) { wire[:goaway] }
          expect(wire[:goaway][1]).to eq(Protocol::HTTP2::Error::FLOW_CONTROL_ERROR)
          expect(receiver).to be_closed
          expect { body.read }.to raise_error(Protocol::HTTP2::FlowControlError)
          expect { sibling_body.read }.to raise_error(Protocol::HTTP2::FlowControlError)
          expect(ingress_backlog(body)).to eq([0, 0])
        end
      end
    end

    it "discards early-closed #{direction} input once, accepts later DATA and ends with NO_ERROR" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
          outgoing, incoming, body = ingress_stream(client_connection, server_connection, direction)
          4.times { ingress_data(sender, outgoing.id, "a" * 16_384) }
          ingress_barrier(task, sender, wire)
          2.times { body.close }
          ingress_barrier(task, sender, wire)
          expect(ingress_backlog(body)).to eq([0, 0])
          expect(wire[:credit][outgoing.id]).to eq(65_536)
          4.times { ingress_data(sender, outgoing.id, "b" * 16_384) }
          ingress_barrier(task, sender, wire)
          expect(wire[:credit][outgoing.id]).to eq(114_688)
          expect(incoming.state).to eq(:open)
          if direction == :request
            incoming.send_headers([[":status", "200"]], Protocol::HTTP2::END_STREAM)
          else
            incoming.send_data(nil, Protocol::HTTP2::END_STREAM)
          end
          ingress_barrier(task, sender, wire)
          expect(wire[:resets][outgoing.id]).to eq(Protocol::HTTP2::Error::NO_ERROR)
          expect(incoming).to be_closed
          expect(body.instance_variable_get(:@stream)).to be_nil
        end
      end
    end

    [[0, "", nil], [2, "a", "short"], [0, "a", "over"], ["+3", "abc", nil]].each do |length, bytes, error|
      it "preserves #{direction} Content-Length #{length.inspect} for #{bytes.inspect}" do
        reactor do |task|
          ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
            outgoing, _incoming, body = ingress_stream(client_connection, server_connection, direction, length: length)
            ingress_data(sender, outgoing.id, bytes, padding: 3, finish: true)
            ingress_barrier(task, sender, wire)
            if error
              expect { body.each.to_a }.to raise_error(EOFError, /#{error}/)
            else
              expect(body.each.to_a.join).to eq(bytes)
            end
          end
        end
      end
    end

    it "keeps native malformed-padding errors for #{direction} input" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
          outgoing, _incoming, body = ingress_stream(client_connection, server_connection, direction)
          frame = Protocol::HTTP2::DataFrame.new(outgoing.id, Protocol::HTTP2::PADDED)
          frame.payload = "\x05x".b
          frame.length = 2
          sender.write_frame(frame)
          ingress_wait(task, "malformed padding GOAWAY", state: -> { wire }) { wire[:goaway] }
          expect(wire[:goaway][1]).to eq(Protocol::HTTP2::Error::PROTOCOL_ERROR)
          expect { body.read }.to raise_error(Protocol::HTTP2::ProtocolError, /padding/)
        end
      end
    end

    [Protocol::HTTP2::Error::CANCEL, Protocol::HTTP2::Error::NO_ERROR,
     Protocol::HTTP2::Error::REFUSED_STREAM].each do |code|
      it "wakes a blocked #{direction} reader on peer reset #{code} and retires its body" do
        reactor do |task|
          ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
            outgoing, incoming, body = ingress_stream(client_connection, server_connection, direction)
            token = incoming.processing_cancellation if direction == :request
            ingress_data(sender, outgoing.id, "prefix")
            ingress_barrier(task, sender, wire)
            expect(body.read).to eq("prefix")
            reading = Async::Queue.new
            reader = task.async do
              reading.enqueue(true)
              body.read
            rescue StandardError => e
              e
            end
            reading.dequeue
            ingress_wait(task, "blocked body read", state: -> { [body.inspect, reader.finished?, incoming.state] }) do
              body.instance_variable_get(:@queue).instance_variable_get(:@changed).waiting?
            end
            outgoing.send_reset_stream(code)
            ingress_barrier(task, sender, wire)
            result = reader.wait
            case code
            when Protocol::HTTP2::Error::NO_ERROR then expect(result).to be_nil
            when Protocol::HTTP2::Error::REFUSED_STREAM then expect(result).to be_a(Protocol::HTTP::RefusedError)
            else expect(result).to be_a(Protocol::HTTP2::StreamError)
            end
            expect(token.reason).to eq(code == Protocol::HTTP2::Error::CANCEL ? :canceled : nil) if token
            expect(ingress_backlog(body)).to eq([0, 0])
            expect(body.instance_variable_get(:@stream)).to be_nil
            expect(incoming).to be_closed
          end
        end
      end
    end

    it "drains queued #{direction} payload before peer NO_ERROR EOF" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
          outgoing, incoming, body = ingress_stream(client_connection, server_connection, direction)
          token = incoming.processing_cancellation if direction == :request
          ingress_data(sender, outgoing.id, "queued payload")
          ingress_barrier(task, sender, wire)
          outgoing.send_reset_stream(Protocol::HTTP2::Error::NO_ERROR)
          ingress_barrier(task, sender, wire)
          expect(body.each.to_a.join).to eq("queued payload")
          expect(body.read).to be_nil
          expect(token.reason).to be_nil if token
          expect(body.instance_variable_get(:@stream)).to be_nil
          expect(ingress_backlog(body)).to eq([0, 0])
          expect(wire[:credit][outgoing.id]).to eq(0)
        end
      end
    end

    it "drains #{direction} bytes before trailing HEADERS EOF without a late window update" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
          outgoing, _incoming, body = ingress_stream(client_connection, server_connection, direction)
          ingress_data(sender, outgoing.id, "first")
          outgoing.send_headers([%w[x-trailer end]], Protocol::HTTP2::END_STREAM)
          ingress_barrier(task, sender, wire)
          expect(body.each.to_a.join).to eq("first")
          expect(body.read).to be_nil
          ingress_barrier(task, sender, wire)
          expect(wire[:credit][outgoing.id]).to eq(0)
        end
      end
    end

    it "wakes a blocked #{direction} reader on owned connection loss without explicit cancellation" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, receiver, wire|
          outgoing, incoming, body = ingress_stream(client_connection, server_connection, direction)
          token = incoming.processing_cancellation if direction == :request
          ingress_data(sender, outgoing.id, "started")
          ingress_barrier(task, sender, wire)
          expect(body.read).to eq("started")
          reader = task.async do
            body.read
          rescue StandardError => e
            e
          end
          ingress_wait(task, "blocked reader before owner loss", state: -> { [body.inspect, reader.finished?] }) do
            body.instance_variable_get(:@queue).instance_variable_get(:@changed).waiting?
          end
          receiver.close(IOError.new("owned connection lost"))
          expect(reader.wait).to be_a(IOError)
          expect(token.reason).to be_nil if token
          expect(body.instance_variable_get(:@stream)).to be_nil
          expect(ingress_backlog(body)).to eq([0, 0])
        end
      end
    end

    it "replenishes only consumed #{direction} bytes across repeated slow-reader rounds" do
      reactor do |task|
        ingress_pair(task, direction) do |client_connection, server_connection, sender, _receiver, wire|
          outgoing, incoming, body = ingress_stream(client_connection, server_connection, direction)
          ingress_barrier(task, sender, wire)
          expect(incoming.local_window.capacity).to eq(65_536)
          4.times { ingress_data(sender, outgoing.id, "a" * 16_384) }
          ingress_barrier(task, sender, wire)
          expect(ingress_backlog(body)).to eq([65_536, 4])
          expect(wire[:credit][outgoing.id]).to eq(0)
          4.times do |round|
            expect(body.read).to eq("a" * 16_384)
            ingress_barrier(task, sender, wire)
            expect(wire[:credit][outgoing.id]).to eq((round + 1) * 16_384)
            ingress_data(sender, outgoing.id, "a" * 16_384)
            ingress_barrier(task, sender, wire)
            expect(ingress_backlog(body).first).to be <= 65_536
            expect(ingress_backlog(body).last).to be <= 5
          end
        end
      end
    end
  end
end
