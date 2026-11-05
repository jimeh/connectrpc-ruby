# frozen_string_literal: true

require_relative "support/http1_client_helpers"

RSpec.describe ConnectRPC::Async::HTTP1Transport, "writer and completion arbitration" do
  include HTTP1ClientHelpers

  [false, true].each do |reported|
    label = reported ? "reported" : "previously unreported"
    it "#{reported ? 'permits' : 'rejects'} HTTP completion after a #{label} writer failure" do
      sent = Async::Promise.new
      peer = lambda do |native, socket, *_|
        native.read_request
        socket.write(http1_wire)
        socket.close
        sent.resolve(nil)
      end
      with_raw_http1(peer: peer) do |transport, origin, _accepted, task|
        transport.open_stream(stream_request(origin)) do |exchange|
          sent.wait
          # These fragments transfer ownership locally. The peer has closed;
          # the native writer must discover the socket failure independently.
          operation = exchange.instance_variable_get(:@operation)
          until operation[:writer_state] == :failed
            exchange.write("x" * 16_384)
            task.yield
          end
          wait_until(task, "native socket writer failure") { operation[:writer_state] == :failed }
          expect(operation[:writer_reported]).to be(false)
          expect { exchange.write("last") }.to raise_error(ConnectRPC::Error) if reported
          expect(exchange.close_send).to be_nil
          expect(exchange.close_send).to be_nil
          expect(exchange.response_head.status).to eq(200)
          expect(exchange.read(2)).to eq("ok")
          expect(exchange.read(1)).to be_nil
          if reported
            expect(exchange.finish(timeout: 1)).to be_nil
          else
            expect { exchange.finish(timeout: 1) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
          end
        end
        expect(transport.instance_variable_get(:@leases)).to be_empty
      end
    end
  end

  it "requires writer success for HTTP 200 even when the whole response is available" do
    release = Async::Promise.new
    peer = lambda { |native, socket, *_|
      native.read_request
      socket.write(http1_wire)
      release.wait
    }
    with_raw_http1(peer: peer) do |transport, origin|
      expect do
        transport.open_stream(stream_request(origin)) do |exchange|
          operation = exchange.instance_variable_get(:@operation)
          native_idle = false
          allow(operation.fetch(:connection)).to receive(:send_end_stream!).and_wrap_original do |original|
            original.call
            native_idle = operation.fetch(:connection).idle?
          end
          expect(exchange.read(2)).to eq("ok")
          expect(exchange.read(1)).to be_nil
          expect { exchange.finish(timeout: 0.02) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
          operation = exchange.instance_variable_get(:@operation)
          expect(operation.fetch(:writer)).to be_finished
          expect(native_idle).to be(true) # Native ensure is insufficient for success.
          expect(operation[:writer_state]).to eq(:failed)
          expect(operation.fetch(:resource).raw).to be_closed
        end
      end.to raise_error(ConnectRPC::Error)
    ensure
      release.resolve(nil)
    end
  end

  it "requires native HTTP EOF before finish" do
    with_http1_peer(response: http1_wire) do |transport, origin|
      expect do
        transport.open_stream(stream_request(origin)) do |exchange|
          exchange.close_send
          expect(exchange.response_head.status).to eq(200)
          expect { exchange.finish }.to raise_error(ConnectRPC::Error)
        end
      end.to raise_error(ConnectRPC::Error)
      expect(transport.instance_variable_get(:@leases)).to be_empty
    end
  end

  it "fails and retires if owned body cleanup fails after a complete response" do
    with_http1_peer(response: http1_wire) do |transport, origin|
      expect do
        transport.open_stream(stream_request(origin)) do |exchange|
          exchange.close_send
          expect(exchange.read(2)).to eq("ok")
          expect(exchange.read(1)).to be_nil
          operation = exchange.instance_variable_get(:@operation)
          failure = IOError.new("owned cleanup failed")
          allow(operation.fetch(:input_body)).to receive(:close).and_raise(failure)
          expect { exchange.finish(timeout: 1) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
          expect(operation[:cleanup_error]).to equal(failure)
          expect(operation.fetch(:resource).raw).to be_closed
        end
      end.to raise_error(ConnectRPC::Error)
      expect(transport.instance_variable_get(:@leases)).to be_empty
    end
  end

  it "includes cleanup within the supplied finish timeout" do
    with_http1_peer(response: http1_wire) do |transport, origin|
      expect do
        transport.open_stream(stream_request(origin)) do |exchange|
          exchange.close_send
          expect(exchange.read(2)).to eq("ok")
          expect(exchange.read(1)).to be_nil
          operation = exchange.instance_variable_get(:@operation)
          entered = false
          allow(operation.fetch(:input_body)).to receive(:close) do
            entered = true
            Async::Condition.new.wait
          end
          started = AsyncHelpers::INTERNAL.now
          expect { exchange.finish(timeout: 0.05) }.to raise_error(ConnectRPC::Error)
          expect(entered).to be(true)
          expect(AsyncHelpers::INTERNAL.now - started).to be < 0.12
          expect(operation.fetch(:resource).raw).to be_closed
        end
      end.to raise_error(ConnectRPC::Error)
    end
  end

  outcomes = [[false, false, "unavailable"], [false, true, "unavailable"],
              [true, false, "unavailable"], [true, true, "not_found"], [:handled, false, nil]]
  outcomes.each do |reported, remote_error, code|
    it "arbitrates core CS with reported=#{reported}, remote_error=#{remote_error}, result=#{code || 'success'}" do
      payload = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "reply"))
      terminal = remote_error ? '{"error":{"code":"not_found","message":"missing"}}' : "{}"
      bytes = (remote_error ? +"".b : [0, payload.bytesize].pack("CN") + payload) +
              [2, terminal.bytesize].pack("CN") + terminal
      closed = Async::Promise.new
      peer = lambda do |native, socket, *_|
        native.read_request
        socket.write(http1_wire(bytes, fields: "Content-Type: application/connect+proto\r\n"))
        socket.close
        closed.resolve(nil)
      end
      with_raw_http1(peer: peer) do |transport, origin, _accepted, task|
        method = ConnectRPC::ClientStreamMethod.new(service: "h1.Failure", name: "Upload",
                                                    request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
        core = ConnectRPC::Client.new(base_url: origin, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                      transport: transport)
        invoke = lambda do
          core.client_stream(method) do |writer|
            closed.wait
            operation = transport.instance_variable_get(:@calls).keys.first
            until operation[:writer_state] == :failed
              writer.send_message(AsyncHelpers::MESSAGE.new(name: "x" * 16_000))
              task.yield
            end
            expect(operation[:writer_reported]).to be(false)
            next unless reported

            begin
              writer.send_message(AsyncHelpers::MESSAGE.new(name: "last"))
            rescue ConnectRPC::Error => e
              expect(e.code).to eq("unavailable")
              raise unless reported == :handled
            end
          end
        end
        if code
          expect(&invoke).to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq(code) }
        else
          expect(invoke.call.message.name).to eq("reply")
        end
        expect(transport.instance_variable_get(:@leases)).to be_empty
      end
    end
  end

  it "preserves reuse after a fully delivered remote error and core abandonment" do
    terminal = '{"error":{"code":"not_found","message":"missing"}}'
    body = [2, terminal.bytesize].pack("CN") + terminal
    wire = http1_wire(body, fields: "Content-Type: application/connect+proto\r\n")
    with_http1_peer(response: wire) do |transport, origin, accepted|
      method = ConnectRPC::ServerStreamMethod.new(service: "h1.Error", name: "Watch",
                                                  request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
      core = ConnectRPC::Client.new(base_url: origin, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                    transport: transport)
      2.times do
        expect do
          core.server_stream(method, AsyncHelpers::MESSAGE.new) { |reader| reader.each { |_| nil } }
        end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("not_found") }
      end
      expect(accepted.size).to eq(1)
    end
  end

  it "applies core's absolute tail policy to a withheld HTTP EOF even with a nil RPC deadline" do
    release = Async::Promise.new
    terminal = "#{[2, 2].pack('CN')}{}"
    peer = lambda do |native, socket, *_|
      incoming = native.read_request
      incoming.last&.each { |_| nil }
      socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/connect+proto\r\nTransfer-Encoding: chunked\r\n\r\n" \
                   "#{terminal.bytesize.to_s(16)}\r\n#{terminal}\r\n")
      release.wait
    end
    with_raw_http1(peer: peer) do |transport, origin|
      method = ConnectRPC::ServerStreamMethod.new(service: "h1.Tail", name: "Watch",
                                                  request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
      core = ConnectRPC::Client.new(base_url: origin, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                    transport: transport)
      expect do
        core.server_stream(method, AsyncHelpers::MESSAGE.new, timeout: nil) { |reader| reader.each { |_| nil } }
      end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
      expect(transport.instance_variable_get(:@leases)).to be_empty
    ensure
      release.resolve(nil)
    end
  end
end
