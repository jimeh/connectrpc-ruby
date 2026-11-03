# frozen_string_literal: true

RSpec.describe "Native streaming terminal completion" do
  def rpc
    ConnectRPC::BidiStreamMethod.new(service: "completion.API", name: "Chat", request_type: AsyncHelpers::MESSAGE,
                                     response_type: AsyncHelpers::MESSAGE)
  end

  %i[remote_error remote_success handled_send_failure].each do |scenario|
    it "arbitrates an actual early #{scenario} after native peer response and request rejection" do
      reactor do
        declaration = rpc
        handler = lambda do |_duplex, _|
          raise ConnectRPC::Error.new(:not_found, "early") if scenario == :remote_error
        end
        target = ConnectRPC::Server.new(
          services: [ConnectRPC::Service.new(name: declaration.service, methods: [declaration])],
          binding: ConnectRPC::Bindings::GoogleProtobuf.new, handlers: { declaration.path => handler }
        )
        host = ConnectRPC::Async::Host.new(server: target).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new)
        send_failure = nil
        result = begin
          core.bidi_stream(declaration, timeout: nil) do |duplex|
            duplex.headers
            begin
              duplex.send_message(AsyncHelpers::MESSAGE.new(name: "x" * 500_000))
            rescue ConnectRPC::Error => e
              send_failure = e
              raise unless scenario == :handled_send_failure
            end
            duplex.receive
          end
        rescue ConnectRPC::Error => e
          e
        end
        expect(send_failure).to be_a(ConnectRPC::Error)
        expect(send_failure.code).to eq("unavailable")
        if scenario == :handled_send_failure
          expect(result).to be_nil
        else
          expect(result.code).to eq(scenario == :remote_error ? "not_found" : "unavailable")
          expect(result.cause).to be_nil
        end
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
      ensure
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end

  scenarios = [[:nil_deadline, "{}", false, :unavailable], [:deadline, "{}", false, :deadline_exceeded],
               [:token, "{}", false, :canceled], [:nil_deadline, '{"error":{"code":"not_found"}}', false, :unavailable],
               [:eof, '{"error":{"code":"not_found"}}', true, :not_found], [:extra, "{}", true, :internal]]
  scenarios.each do |mode, terminal, eof, code|
    it "verifies #{mode}/#{code} through an actual terminal and HTTP tail with original deadline policy" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        tail = Async::Queue.new
        body = AsyncHelpers::INTERNAL::OutputBody.new
        peer = task.async(finished: false) do
          raw, = bound.sockets.first.accept
          connection = Async::HTTP::Protocol::HTTP2.server(raw)
          incoming = connection.requests.dequeue
          incoming.send_response(Protocol::HTTP::Response[200, { "content-type" => "application/connect+proto" }, body])
          bytes = [2, terminal.bytesize].pack("CN") + terminal
          bytes += "x" if mode == :extra
          body.write(bytes)
          body.close_write if eof
          Async::Condition.new.wait
        ensure
          connection&.close
          AsyncHelpers::INTERNAL.raw_close(raw)
        end
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new)
        exchange_type = AsyncHelpers::INTERNAL::StreamExchange
        allow_any_instance_of(exchange_type).to receive(:read).and_wrap_original do |original, length, **options|
          tail.enqueue(options[:timeout]) if length == 1
          original.call(length, **options)
        end
        source = ConnectRPC::CancellationSource.new
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        call = task.async do
          core.bidi_stream(rpc, timeout: mode == :deadline ? 0.1 : nil, cancellation: source.token, &:receive)
        rescue ConnectRPC::Error => e
          e
        end
        allowance = tail.dequeue
        expect(allowance.positive?).to be(true)
        expect(allowance).to be <= 1.0
        source.cancel if mode == :token
        error = call.wait
        expect(error.code).to eq(code.to_s)
        expect(error.cause).to be_nil
        if mode == :nil_deadline
          # Elapsed time is the external native tail policy under test; readiness
          # was established above by the actual one-byte transport read.
          expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be >= 0.9
        end
      ensure
        body&.close
        transport&.close(grace: 0)
        peer&.cancel
        peer&.wait
        bound&.close
      end
    end
  end
end
