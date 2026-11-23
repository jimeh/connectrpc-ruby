# frozen_string_literal: true

RSpec.describe "HTTP/2 client cleanup allowance" do
  # CPU starvation, such as another Ruby thread holding the GVL, can block the
  # whole reactor thread after a call's cleanup has computed its expiry
  # (#187). The stall is longer than the former 100 ms allowance.
  stall = 0.25
  allowance = AsyncHelpers::INTERNAL::CLEANUP_SECONDS

  it "keeps the connection when the reactor stalls inside a cancelled call's cleanup for less than the allowance" do
    expect(stall).to be < allowance / 2
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      echo = lambda do |message, _|
        if message.name == "held"
          entered.enqueue(true)
          release.dequeue
        end
        message
      end
      host = deadline_host(server: server(echo: echo))
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      echo_request = lambda do |name|
        request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}",
                headers: { "content-type" => ["application/proto"] },
                body: AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: name)))
      end
      expect(transport.call(echo_request.call("warm")).status).to eq(200)
      resources = transport.instance_variable_get(:@resources)
      expect(resources.size).to eq(1)
      resource = resources.first
      source = ConnectRPC::CancellationSource.new
      held = echo_request.call("held")
      held.cancellation = source.token
      stalled = nil
      allow(transport).to receive(:retire_stopped_output).and_wrap_original do |original, operation, expires|
        # The transport passes on its own copy of the request.
        if operation[:request].body == held.body && stalled.nil?
          # Blocks the reactor thread itself, which a fiber sleep would not.
          stalled = AsyncHelpers::INTERNAL.now
          Fiber.blocking { sleep(stall) }
          stalled = AsyncHelpers::INTERNAL.now - stalled
        end
        original.call(operation, expires)
      end
      call = task.async(finished: false) { transport.call(held) }
      entered.dequeue
      source.cancel
      expect { call.wait }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
      expect(stalled).to be >= stall
      expect(resource).not_to be_finished
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(lease_usage(transport)).to eq(0)
      release.enqueue(true)
      expect(transport.call(echo_request.call("after")).status).to eq(200)
      expect(transport.instance_variable_get(:@resources)).to eq([resource])
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
