# frozen_string_literal: true

RSpec.describe "Client cancellation" do
  # A real client cancels while the handler runs; the handler then observes the
  # cancellation, so the server records canceled and its delivery aborts.
  [%i[async connect], %i[falcon connect], %i[async grpc]].each do |backend, protocol|
    it "keeps the #{backend} #{protocol} server transaction cancelled, not failed, without an error event" do
      sentry(isolation: :fiber)
      instrumentation = bus
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        source = ConnectRPC::CancellationSource.new
        context = nil
        handler = lambda do |message, current|
          context = current
          entered.enqueue(true)
          release.dequeue
          current.check!
          message
        end
        target = echo_server(instrumentation: instrumentation, echo: handler, protocol: protocol)
        runner = lambda do |_host, origin|
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          typed = client(transport, origin, protocol: protocol)
          call = task.async(finished: false) do
            typed.echo(MESSAGE.new(name: "cancel"), timeout: nil, cancellation: source.token)
          end
          entered.dequeue
          source.cancel
          expect { call.wait }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
          wait_until(task, "server Context cancellation") { context.cancelled? }
          release.enqueue(true)
          wait_until(task, "server transaction") { rpc_transactions("rpc.server").size == 1 }
          trace = trace_of(rpc_transactions("rpc.server").first)
          expect(trace).to include(status: "cancelled")
          expect(trace[:data]).to include("connectrpc.delivery" => "aborted",
                                          "rpc.response.status_code" => protocol == :grpc ? "CANCELLED" : "canceled")
          expect(error_events).to be_empty
        ensure
          release.enqueue(true)
          transport&.close(grace: 0)
        end
        if backend == :async
          host = ConnectRPC::Async::Host.new(server: target).start
          runner.call(host, "http://127.0.0.1:#{host.port}")
        else
          falcon(server: target, &runner)
        end
      ensure
        host&.close(grace: 0)
      end
    end
  end
end
