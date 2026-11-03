# frozen_string_literal: true

RSpec.describe "Streaming cancellation before writable and response-head progress" do
  def stream_method
    ConnectRPC::ServerStreamMethod.new(service: "test.Admission", name: "Watch",
                                       request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
  end

  def streaming_client(transport, origin)
    ConnectRPC::Client.new(base_url: origin, transport: transport, binding: ConnectRPC::Bindings::GoogleProtobuf.new)
  end

  def streaming_host(entered, release, observed, **)
    method = stream_method
    target = ConnectRPC::Server.new(
      services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
      binding: ConnectRPC::Bindings::GoogleProtobuf.new,
      handlers: { method.path => lambda { |message, writer, _|
        observed << message.name
        if message.name == "hold"
          entered.enqueue(true)
          release.dequeue
        end
        writer.send_message(message)
      } }
    )
    ConnectRPC::Async::Host.new(server: target, **).start
  end

  def expect_interrupted(call, reason)
    expect { call.wait }.to raise_error(ConnectRPC::Error) do |error|
      expect([error.code, error.cause]).to eq([reason == :token ? "canceled" : "deadline_exceeded", nil])
    end
  end

  %i[token deadline].each do |reason|
    it "retires an actual TLS setup wait through #{reason} with no call or acquiring scopes left" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        listener = bound.sockets.first
        entered = Async::Queue.new
        peer = task.async do
          raw, = listener.accept
          expect(raw.readpartial(1024)).not_to be_empty
          entered.enqueue(true)
          # Observe actual EOF without depending on an unbounded read-all buffer.
          expect { raw.readpartial(1) }.to raise_error(EOFError)
        ensure
          raw&.close
        end
        origin = "https://127.0.0.1:#{listener.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        source = ConnectRPC::CancellationSource.new
        call = task.async(finished: false) do
          streaming_client(transport, origin).server_stream(stream_method, AsyncHelpers::MESSAGE.new,
                                                            timeout: reason == :deadline ? 0.2 : nil,
                                                            cancellation: source.token, &:receive)
        end
        entered.dequeue
        Thread.new { source.cancel }.value if reason == :token
        expect_interrupted(call, reason)
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@acquiring)).to be_empty
        expect(transport.instance_variable_get(:@resources).map(&:raw)).to all(be_closed)
        resources = transport.instance_variable_get(:@resources)
        expect(resources.map(&:task)).to all(be_finished), resources.map { |resource|
          { raw_closed: resource.raw.closed?, socket_closed: resource.socket&.closed?,
            aborted: resource.finished?, setup: resource.task.backtrace }
        }.inspect
        peer.wait
      ensure
        transport&.close(grace: 0)
        peer&.cancel
        bound&.close
      end
    end

    %i[headers pool].each do |phase|
      it "interrupts a streaming #{phase} waiter through #{reason} and prevents late dispatch" do
        reactor do |task|
          entered = Async::Queue.new
          release = Async::Queue.new
          observed = []
          host = streaming_host(entered, release, observed, max_requests: phase == :pool ? 1 : 128)
          origin = "http://127.0.0.1:#{host.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin, connection_limit: 1)
          core = streaming_client(transport, origin)
          source = ConnectRPC::CancellationSource.new
          holder = if phase == :pool
                     task.async do
                       core.server_stream(stream_method, AsyncHelpers::MESSAGE.new(name: "hold"),
                                          timeout: nil) do |reader|
                         reader.each { |_| nil }
                       end
                     end
                   end
          entered.dequeue if holder
          call = task.async(finished: false) do
            core.server_stream(stream_method, AsyncHelpers::MESSAGE.new(name: phase == :headers ? "hold" : "queued"),
                               timeout: reason == :deadline ? 0.2 : nil, cancellation: source.token) do |reader|
              reader.headers
              reader.each { |_| nil }
            end
          end
          if phase == :headers
            entered.dequeue
          else
            wait_until(task, "streaming pool admission waiter") do
              transport.instance_variable_get(:@acquiring).size == 1
            end
          end
          Thread.new { source.cancel }.value if reason == :token
          expect_interrupted(call, reason)
          expect(holder).not_to be_finished if holder
          release.enqueue(true)
          holder&.wait
          core.server_stream(stream_method, AsyncHelpers::MESSAGE.new(name: "after")) do |reader|
            expect(reader.receive.name).to eq("after")
            expect(reader.receive).to be_nil
          end
          expect(observed).to eq(%w[hold after])
          expect(transport.instance_variable_get(:@calls)).to be_empty
          expect(transport.instance_variable_get(:@acquiring)).to be_empty
          expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
          expect(transport.instance_variable_get(:@resources).size).to eq(1)
        ensure
          release&.enqueue(true)
          transport&.close(grace: 0)
          host&.close(grace: 0)
        end
      end
    end
  end
end
