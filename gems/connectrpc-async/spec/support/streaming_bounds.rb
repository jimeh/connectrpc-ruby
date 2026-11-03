# frozen_string_literal: true

RSpec.shared_examples "native streaming bounds and cancellation" do |protocol = :connect|
  %i[request response].product(%i[token deadline]).each do |direction, reason|
    it "bounds nonreading #{direction} ownership and advances a same-connection sibling through #{reason}" do
      reactor do |task|
        ss, cs, bidi = stream_methods
        entered = Async::Queue.new
        release = Async::Queue.new
        bodies = []
        allow(AsyncHelpers::INTERNAL::OutputBody).to receive(:new).and_wrap_original do |original|
          original.call.tap { |body| bodies << body }
        end
        handlers = {
          ss.path => lambda { |_, writer, _|
            20.times do
              writer.send_message(AsyncHelpers::MESSAGE.new(name: "x" * 200_000))
            end
          },
          cs.path => lambda { |reader, _|
            reader.each { |_| nil }
            AsyncHelpers::MESSAGE.new(name: "healthy")
          },
          bidi.path => lambda { |duplex, context|
            context.send_headers
            entered.enqueue(context)
            release.dequeue
            duplex.each { |_| nil }
          }
        }
        host, origin, listener = build_stream_host(streaming_server(handlers), nil, "http")
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: protocol,
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new)
        source = ConnectRPC::CancellationSource.new
        call = task.async do
          if direction == :request
            core.bidi_stream(bidi, timeout: reason == :deadline ? 0.2 : nil, cancellation: source.token) do |duplex|
              duplex.headers
              20.times { duplex.send_message(AsyncHelpers::MESSAGE.new(name: "x" * 200_000)) }
              duplex.close_send
              duplex.each { |_| nil }
            end
          else
            core.server_stream(ss, AsyncHelpers::MESSAGE.new, timeout: reason == :deadline ? 0.2 : nil,
                                                              cancellation: source.token) do |reader|
              reader.headers
              entered.enqueue(true)
              release.dequeue
              reader.each { |_| nil }
            end
          end
        rescue ConnectRPC::Error => e
          e
        end
        entered.dequeue
        wait_until(task, "bounded output queue full while producer is blocked") do
          bodies.any? { |body| body.queued_bytes == 16_384 }
        end
        expect(call).not_to be_finished
        expect(bodies.map(&:maximum_queued_bytes)).to all(be <= 16_384)
        resources = transport.instance_variable_get(:@resources)
        expect(resources.size).to eq(1)
        if direction == :request
          receiver = host.instance_variable_get(:@session).instance_variable_get(:@resources).first
          queues = receiver.connection.streams.values.filter_map { |stream| stream.instance_variable_get(:@input) }
        else
          queues = resources.first.connection.streams.values.filter_map do |stream|
            stream.instance_variable_get(:@input)
          end
        end
        retained = queues.map { |queue| queue.instance_variable_get(:@queue).instance_variable_get(:@chunks) }
        expect(retained).not_to be_empty
        expect(retained.map { |chunks| chunks.sum(&:bytesize) }).to all(be <= 65_536)
        expect(retained.map(&:size)).to all(be <= 5)
        expect(retained.flatten.map(&:bytesize)).to all(be <= 16_384)
        expect(core.client_stream(cs, timeout: nil) { nil }.message.name).to eq("healthy")
        expect(resources.size).to eq(1)
        source.cancel if reason == :token
        if direction == :response
          if reason == :deadline
            outgoing = transport.instance_variable_get(:@calls).keys.first.fetch(:request)
            wait_until(task, "real streaming deadline elapsed while application retained the reader") do
              outgoing.deadline.expired?
            end
          end
          release.enqueue(true)
        end
        expect(call.wait.code).to eq(reason == :token ? "canceled" : "deadline_exceeded")
        release.enqueue(true) if direction == :request
        expect(core.client_stream(cs) { nil }.message.name).to eq("healthy")
        expect(transport.instance_variable_get(:@resources).size).to eq(1)
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
      ensure
        release&.enqueue(true)
        transport&.close(grace: 0)
        host&.close(grace: 0)
        listener&.close
      end
    end
  end

  %i[token deadline].each do |reason|
    it "interrupts an actual blocked streaming receive through #{reason} and preserves its sibling" do
      reactor do |task|
        ss, cs, = stream_methods
        entered = Async::Queue.new
        release = Async::Queue.new
        contexts = []
        handlers = {
          ss.path => lambda { |message, writer, context|
            contexts << context
            writer.send_message(message)
            entered.enqueue(true)
            release.dequeue
          },
          cs.path => lambda { |reader, _|
            reader.each { |_| nil }
            AsyncHelpers::MESSAGE.new(name: "healthy")
          }
        }
        host, origin, listener = build_stream_host(streaming_server(handlers), nil, "http")
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: protocol,
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new)
        source = ConnectRPC::CancellationSource.new
        progress = Async::Queue.new
        call = task.async do
          core.server_stream(ss, AsyncHelpers::MESSAGE.new(name: "progress"),
                             timeout: reason == :deadline ? 0.1 : nil, cancellation: source.token) do |reader|
            expect(reader.receive.name).to eq("progress")
            progress.enqueue(true)
            reader.receive
          end
        rescue ConnectRPC::Error => e
          e
        end
        entered.dequeue
        progress.dequeue
        expect(core.client_stream(cs) { nil }.message.name).to eq("healthy")
        source.cancel if reason == :token
        error = call.wait
        expect(error.code).to eq(reason == :token ? "canceled" : "deadline_exceeded")
        expect(error.cause).to be_nil
        if reason == :token
          wait_until(task, "actual peer CANCEL reached the live streaming Context") do
            contexts.first.cancellation_reason == :canceled
          end
        end
        release.enqueue(true)
        expect(core.client_stream(cs) { nil }.message.name).to eq("healthy")
        expect(transport.instance_variable_get(:@resources).size).to eq(1)
        expect(transport.instance_variable_get(:@calls)).to be_empty
      ensure
        release&.enqueue(true)
        transport&.close(grace: 0)
        host&.close(grace: 0)
        listener&.close
      end
    end
  end
end
