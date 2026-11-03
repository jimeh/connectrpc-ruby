# frozen_string_literal: true

RSpec.shared_examples "native streaming bounds and cancellation" do |protocol = :connect|
  %i[request response].product(%i[token deadline]).each do |direction, reason|
    it "bounds nonreading #{direction} ownership and advances a same-connection sibling through #{reason}" do
      reactor do |task|
        ss, cs, bidi = stream_methods
        entered = Async::Queue.new
        release = Async::Queue.new
        bodies = []
        contexts = []
        invocations = Hash.new(0)
        allow(AsyncHelpers::INTERNAL::OutputBody).to receive(:new).and_wrap_original do |original|
          original.call.tap { |body| bodies << body }
        end
        handlers = {
          ss.path => lambda { |_, writer, context|
            contexts << context
            invocations[:response] += 1
            20.times do
              writer.send_message(AsyncHelpers::MESSAGE.new(name: "x" * 200_000))
            end
          },
          cs.path => lambda { |reader, _|
            reader.each { |_| nil }
            AsyncHelpers::MESSAGE.new(name: "healthy")
          },
          bidi.path => lambda { |duplex, context|
            contexts << context
            invocations[:request] += 1
            context.send_headers
            entered.enqueue(context)
            release.dequeue
            duplex.each { |_| nil }
          }
        }
        observations = []
        subscriber = Object.new
        subscriber.define_singleton_method(:start) do |call|
          record = { role: call.role, path: call.method.path }
          observations << record
          Object.new.tap do |observer|
            observer.define_singleton_method(:processing_finished) { |outcome| record[:outcome] = outcome }
            observer.define_singleton_method(:finished) { |completion| record[:completion] = completion }
          end
        end
        bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
        host, origin, listener = build_stream_host(streaming_server(handlers, instrumentation: bus), nil, "http")
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: protocol,
                                      instrumentation: bus, binding: ConnectRPC::Bindings::GoogleProtobuf.new)
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
        resources = transport.instance_variable_get(:@resources)
        expect(resources.size).to eq(1)
        client_resource = resources.first
        outgoing = transport.instance_variable_get(:@calls).keys.first
        rpc_request = outgoing.fetch(:request)
        client_stream = outgoing.fetch(:response).stream
        receiver = host.instance_variable_get(:@session).instance_variable_get(:@resources).first
        server_stream = receiver.connection.streams.fetch(client_stream.id)
        sender, receiving = direction == :request ? [client_stream, server_stream] : [server_stream, client_stream]
        input = receiving.instance_variable_get(:@input)
        chunks = input.instance_variable_get(:@queue).instance_variable_get(:@chunks)
        state = nil
        begin
          task.with_timeout(2) do
            loop do
              state = { sent_credit: sender.remote_window.available, receive_credit: receiving.local_window.available,
                        retained: chunks.sum(&:bytesize), queued: bodies.map(&:queued_bytes) }
              break if state[:sent_credit].zero? && state[:receive_credit].zero? && state[:retained] == 65_536 &&
                       state[:queued].include?(16_384)

              task.yield
            end
          end
        rescue Async::TimeoutError
          raise "Wire stream did not saturate before cancellation; last state: #{state.inspect}"
        end
        expect(call).not_to be_finished
        expect(rpc_request.deadline).not_to be_expired if reason == :deadline
        expect(bodies.map(&:maximum_queued_bytes)).to all(be <= 16_384)
        queues = [input]
        retained = queues.map { |queue| queue.instance_variable_get(:@queue).instance_variable_get(:@chunks) }
        expect(retained).not_to be_empty
        expect(retained.map { |chunks| chunks.sum(&:bytesize) }).to all(be <= 65_536)
        expect(retained.map(&:size)).to all(be <= 5)
        expect(retained.flatten.map(&:bytesize)).to all(be <= 16_384)
        expect(core.client_stream(cs, timeout: nil) { nil }.message.name).to eq("healthy")
        expect(resources.size).to eq(1)
        # The healthy sibling must finish while this stream still has no wire
        # credit. This also orders the bound checks after actual saturation.
        expect(sender.remote_window.available).to eq(0)
        expect(receiving.local_window.available).to eq(0)
        expect(chunks.sum(&:bytesize)).to eq(65_536)
        expect(chunks.size).to be <= 5
        expect(chunks.map(&:bytesize)).to all(be <= 16_384)
        expect(bodies.map(&:maximum_queued_bytes)).to all(be <= 16_384)
        expect(rpc_request.deadline).not_to be_expired if reason == :deadline
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
        error = call.wait
        expect(error.code).to eq(reason == :token ? "canceled" : "deadline_exceeded")
        expect(error.cause).to be_nil
        if reason == :token
          wait_until(task, "peer CANCEL reached saturated handler Context") do
            contexts.first.cancellation_reason == :canceled
          end
        end
        method = direction == :request ? bidi : ss
        client_facts = observations.find { |record| record[:role] == :client && record[:path] == method.path }
        expect(client_facts.fetch(:outcome).code).to eq(error.code)
        expect(client_facts.fetch(:completion).delivery).to eq(:aborted)
        expect(invocations[direction]).to eq(1)
        expect(chunks.sum(&:bytesize)).to be <= 65_536
        expect(bodies.map(&:maximum_queued_bytes)).to all(be <= 16_384)
        release.enqueue(true) if direction == :request
        expect(core.client_stream(cs) { nil }.message.name).to eq("healthy")
        server_facts = observations.find { |record| record[:role] == :server && record[:path] == method.path }
        wait_until(task, "saturated server processing and delivery retired") { server_facts[:completion] }
        expect(server_facts.fetch(:completion).delivery).to eq(:aborted)
        server_outcome = server_facts.fetch(:outcome)
        if direction == :response
          # Reset may stop the blocked processor before a terminal RPC status
          # can be prepared. Context cancellation is independent of that fact.
          expect([nil, contexts.first.cancellation_reason.to_s]).to include(server_outcome.code)
          expect(server_outcome.error).not_to be_nil
        else
          expect(server_outcome.code).to eq(contexts.first.cancellation_reason.to_s)
        end
        expect(transport.instance_variable_get(:@resources)).to eq([client_resource])
        expect(client_resource.finished?).to be(false)
        expect(receiver.finished?).to be(false)
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
