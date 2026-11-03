# frozen_string_literal: true

RSpec.describe "Native HTTP/2 GET" do
  %i[h2c tls mtls].product(%i[proto json]).each do |mode, codec|
    it "runs generated #{codec} GET over #{mode} with HEADERS END_STREAM and no DATA" do
      reactor do
        with_ca do |ca_file|
          endpoint = server(encoding: :gzip)
          incoming = []
          allow(endpoint).to receive(:call).and_wrap_original do |original, **arguments|
            incoming << arguments
            original.call(**arguments)
          end
          tls = if mode == :h2c
                  nil
                else
                  host_tls(anchors: mode == :mtls ? [tls_material.fetch(:client_ca)] : nil)
                end
          host = ConnectRPC::Async::Host.new(server: endpoint, tls: tls).start
          origin = "#{mode == :h2c ? 'http' : 'https'}://localhost:#{host.port}"
          identity = mode == :mtls ? host_tls(tls_material.fetch(:identity)).except(:client_ca) : {}
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: mode == :h2c ? nil : ca_file,
                                                       **identity)
          typed = client(transport, origin, codec: codec, use_get: true, encoding: codec == :json ? :gzip : :identity)
          message = AsyncHelpers::MESSAGE.new(name: "GET 雪 +", data: "\x00\xff".b)
          frames = []
          native_headers = []
          wire_probe = TracePoint.new(:call) do |event|
            if event.defined_class == Async::HTTP::Protocol::HTTP2::Client && event.method_id == :write_request
              native = event.binding.local_variable_get(:request)
              native_headers << native.headers.to_h
              expect(native.body).to be_nil
            end
          end
          probe = TracePoint.new(:return) do |event|
            if event.defined_class == Protocol::HTTP2::Framer && event.method_id == :write_frame
              frame = event.binding.local_variable_get(:frame)
              frames << [event.self, frame.type, frame.flags, frame.stream_id]
            end
          end
          response = wire_probe.enable { probe.enable { typed.echo(message, metadata: { "x-request" => %w[a b] }) } }
          expect(response.message).to eq(message)
          expect(response.metadata["x-echo"]).to eq(%w[a b])
          expect(response.trailers["x-tail"]).to eq(%w[three four])
          expect(response.metadata["vary"]).to eq(["Accept-Encoding"])
          resource = transport.instance_variable_get(:@resources).first
          framer = resource.connection.instance_variable_get(:@framer)
          rpc_frames = frames.filter_map { |owner, *values| values if owner.equal?(framer) && values.last.positive? }
          expect(rpc_frames.map(&:first)).to eq([Protocol::HTTP2::HeadersFrame::TYPE])
          expect(rpc_frames.first[1] & Protocol::HTTP2::END_STREAM).to eq(Protocol::HTTP2::END_STREAM)
          observed = incoming.last
          expect(native_headers.size).to eq(1)
          expect(native_headers.first.keys).not_to include("content-type", "content-encoding", "content-length",
                                                           "connect-protocol-version")
          expect(observed[:http_method]).to eq("GET")
          expect(observed[:query]).to include("encoding=#{codec}", "message=")
          expect(observed[:headers].keys).not_to include("content-type", "content-encoding", "connect-protocol-version")
          expect(typed.echo(AsyncHelpers::MESSAGE.new).message).to eq(AsyncHelpers::MESSAGE.new)
          expect { typed.fail(message) }.to raise_error(ConnectRPC::Error) { |error|
            expect(error.code).to eq("invalid_argument")
            expect(error.metadata["x-error"]).to eq(%w[a b])
            expect(error.trailers["x-end"]).to eq(%w[c d])
          }
          expect(incoming.last[:http_method]).to eq("POST")
          resource = transport.instance_variable_get(:@resources).first
          expect(resource.connection.version).to eq("HTTP/2")
          expect(resource.socket.alpn_protocol).to eq("h2") unless mode == :h2c
          expect(transport.close(grace: 1)).to be(true)
          expect(host.close(grace: 1)).to be(true)
        ensure
          probe&.disable
          wire_probe&.disable
          transport&.close(grace: 0)
          host&.close(grace: 0)
        end
      end
    end
  end
end
