# frozen_string_literal: true

require "zlib"

RSpec.describe "HTTP/2 gzip core integration" do
  # Observe the actual host input and returned encoded bytes without replacing
  # the bounded reader, compression, binding or transport behavior.
  def observe_host(endpoint)
    observations = []
    allow(endpoint).to receive(:call).and_wrap_original do |call, **arguments|
      bytes = +"".b
      input = arguments.fetch(:input)
      allow(input).to receive(:read).and_wrap_original do |read, length|
        read.call(length).tap { |chunk| bytes << chunk if chunk }
      end
      response = call.call(**arguments)
      observations << { headers: arguments.fetch(:headers), body: bytes, response: response }
      response
    end
    observations
  end

  def resource_exhausted(&)
    expect(&).to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
  end

  %i[proto json].each do |codec|
    %i[h2c tls].each do |mode|
      it "preserves #{codec} gzip wire bytes, identity defaults and structured errors over #{mode}" do
        reactor do
          with_ca do |ca_file|
            endpoint = server(encoding: :gzip)
            incoming = observe_host(endpoint)
            host = ConnectRPC::Async::Host.new(server: endpoint, tls: mode == :tls ? host_tls : nil).start
            origin = "#{mode == :tls ? 'https' : 'http'}://localhost:#{host.port}"
            transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: mode == :tls ? ca_file : nil)
            exchanges = []
            observed = lambda do |request|
              transport.call(request).tap { |response| exchanges << [request, response] }
            end
            typed = client(observed, origin, codec: codec, encoding: :gzip)
            message = AsyncHelpers::MESSAGE.new(name: "gzip" * 512, count: 42)
            response = typed.echo(message, metadata: { "x-request" => %w[a b] })
            expect(response.message).to eq(message)
            expect(response.metadata["x-echo"]).to eq(%w[a b])
            expect(response.trailers["x-tail"]).to eq(%w[three four])
            outgoing, received = exchanges.last
            expect(outgoing.headers["content-encoding"]).to eq(["gzip"])
            expect(incoming.last.fetch(:headers)["content-encoding"]).to eq(["gzip"])
            expect(incoming.last.fetch(:body)).to eq(outgoing.body)
            expect(received.headers["content-encoding"]).to eq(["gzip"])
            expect(received.body).to eq(incoming.last.fetch(:response).body)
            binding = ConnectRPC::Bindings::GoogleProtobuf.new
            [outgoing.body, received.body].each do |wire|
              expect(wire.byteslice(0, 2)).to eq("\x1f\x8b".b)
              expect(binding.decode(Zlib.gunzip(wire), type: AsyncHelpers::MESSAGE, codec: codec)).to eq(message)
            end

            empty = AsyncHelpers::MESSAGE.new
            expect(typed.echo(empty).message).to eq(empty)
            expect(exchanges.last.map { |wire| wire.body.bytesize }).to all(be_positive)
            expect { typed.fail(message) }.to raise_error(ConnectRPC::Error) { |error|
              expect(error.code).to eq("invalid_argument")
              expect(AsyncHelpers::MESSAGE.decode(error.details.first.value)).to eq(message)
            }
            expect(exchanges.last.last.headers).not_to have_key("content-encoding")
            identity = client(observed, origin, codec: codec)
            expect(identity.echo(message).message).to eq(message)
            expect(exchanges.last.first.headers["accept-encoding"]).to eq(["identity"])
            expect(exchanges.last.first.headers).not_to have_key("content-encoding")
            expect(exchanges.last.last.headers).not_to have_key("content-encoding")
            connection = transport.instance_variable_get(:@resources).first
            expect(connection.connection.version).to eq("HTTP/2")
            expect(connection.socket.alpn_protocol).to eq("h2") if mode == :tls
          ensure
            transport&.close
            host&.close
          end
        end
      end
    end

    it "enforces independent #{codec} host wire and inflated caps before invocation" do
      reactor do
        binding = ConnectRPC::Bindings::GoogleProtobuf.new
        message = AsyncHelpers::MESSAGE.new(name: "x" * 10_000)
        bytes = binding.encode(message, type: AsyncHelpers::MESSAGE, codec: codec)
        wire = Zlib.gzip(bytes)
        expect(wire.bytesize).to be < bytes.bytesize
        [{ request_wire_limit: wire.bytesize - 1 }, { request_limit: bytes.bytesize - 1 }].each do |limits|
          invoked = []
          echo = lambda do |request, _context|
            invoked << request
            request
          end
          endpoint = server(encoding: :gzip, echo: echo, **limits)
          incoming = observe_host(endpoint)
          host = ConnectRPC::Async::Host.new(server: endpoint).start
          origin = "http://127.0.0.1:#{host.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          typed = client(transport, origin, codec: codec, encoding: :gzip)
          resource_exhausted { typed.echo(message) }
          expect(invoked).to be_empty
          expect(incoming.last.fetch(:headers)["content-encoding"]).to eq(["gzip"])
          if limits.key?(:request_wire_limit)
            expect(incoming.last.fetch(:body)).to be_empty
          else
            expect(incoming.last.fetch(:body).bytesize).to eq(wire.bytesize)
          end
        ensure
          transport&.close
          host&.close
        end
      end
    end

    it "keeps the #{codec} transport wire cap distinct from client inflated and server output caps" do
      reactor do
        binding = ConnectRPC::Bindings::GoogleProtobuf.new
        message = AsyncHelpers::MESSAGE.new(name: "x" * 10_000)
        bytes = binding.encode(message, type: AsyncHelpers::MESSAGE, codec: codec)
        wire = Zlib.gzip(bytes)
        endpoint = server(encoding: :gzip, response_limit: bytes.bytesize, response_wire_limit: wire.bytesize)
        host = ConnectRPC::Async::Host.new(server: endpoint).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        headers = { "content-type" => ["application/#{codec}"], "content-encoding" => ["gzip"],
                    "accept-encoding" => ["gzip"] }
        raw = request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}", body: wire, limit: wire.bytesize, headers: headers)
        response = transport.call(raw)
        expect(response.status).to eq(200)
        expect(response.headers["content-encoding"]).to eq(["gzip"])
        expect(response.body.bytesize).to eq(wire.bytesize)
        expect(binding.decode(Zlib.gunzip(response.body), type: AsyncHelpers::MESSAGE, codec: codec)).to eq(message)
        raw.response_limit = wire.bytesize - 1
        resource_exhausted { transport.call(raw) }
        typed = client(transport, origin, codec: codec, encoding: :gzip,
                                          response_limit: bytes.bytesize - 1, response_wire_limit: wire.bytesize)
        resource_exhausted { typed.echo(message) }
        exact = client(transport, origin, codec: codec, encoding: :gzip,
                                          response_limit: bytes.bytesize, response_wire_limit: wire.bytesize)
        expect(exact.echo(message).message).to eq(message)
      ensure
        transport&.close
        host&.close
      end
    end
  end
end
