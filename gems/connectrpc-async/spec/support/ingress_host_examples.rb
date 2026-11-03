# frozen_string_literal: true

require "digest"
require_relative "ingress_peer"

RSpec.shared_examples "hosted HTTP/2 ingress backpressure" do
  include IngressPeer

  %i[h2c tls mtls].each do |mode|
    it "bounds a gated request consumer and preserves a sibling over #{mode}" do
      reactor do |task|
        entered = Async::Queue.new
        commands = Async::Queue.new
        consumed = Async::Queue.new
        completed = []
        app = lambda do |body, read_next, route|
          next [200, {}, ["sibling"]] unless route == "/hold"

          entered.enqueue(body)
          bytes = +"".b
          loop do
            command = commands.dequeue
            if command == :drain
              while (chunk = read_next.call) && !chunk.empty?
                bytes << chunk
              end
              break
            end
            chunk = read_next.call
            bytes << chunk
            consumed.enqueue(chunk)
          end
          completed << bytes.bytesize
          [200, {}, [Digest::SHA256.hexdigest(bytes)]]
        end
        options = if mode == :h2c
                    nil
                  else
                    host_tls(anchors: mode == :mtls ? [tls_material[:client_ca]] : nil)
                  end
        with_ingress_host(app, tls: options) do |host, origin|
          with_ingress_client(task, origin, mode) do |connection, wire|
            response = connection.create_response
            response.instance_variable_set(:@request, Protocol::HTTP::Request["POST", "/hold", {}, nil])
            response.stream.send_headers([[":method", "POST"], [":scheme", URI(origin).scheme],
                                          [":authority", URI(origin).authority], [":path", "/hold"]])
            body = entered.dequeue
            id = response.stream.id
            4.times { ingress_data(connection, id, "a" * 16_384) }
            ingress_barrier(task, connection, wire)
            expect(ingress_backlog(body)).to eq([65_536, 4])
            expect(wire[:credit][id]).to eq(0)
            native = Protocol::HTTP::Request["GET", "/sibling", {}, nil,
                                             scheme: URI(origin).scheme, authority: URI(origin).authority]
            sibling = connection.call(native)
            expect(sibling.read).to eq("sibling")
            4.times do |round|
              commands.enqueue(:one)
              expect(consumed.dequeue).to eq("a" * 16_384)
              ingress_barrier(task, connection, wire)
              expect(wire[:credit][id]).to eq((round + 1) * 16_384)
              ingress_data(connection, id, "a" * 16_384)
              ingress_barrier(task, connection, wire)
              expect(ingress_backlog(body).first).to be <= 65_536
              expect(ingress_backlog(body).last).to be <= 5
            end
            ingress_data(connection, id, "", finish: true)
            commands.enqueue(:drain)
            connection.read_response(response)
            expect(response.read).to eq(Digest::SHA256.hexdigest("a" * 131_072))
            expect(completed).to eq([131_072])
            session = host.instance_variable_get(:@session)
            resource = session.instance_variable_get(:@resources).first
            state = -> { [resource.connection.streams.keys, resource.tasks.map(&:finished?), body.inspect] }
            ingress_wait(task, "host stream/task retirement", state: state) { resource.empty? }
            expect(body.instance_variable_get(:@stream)).to be_nil
            expect(ingress_backlog(body)).to eq([0, 0])
          end
        end
      end
    end
  end
end
