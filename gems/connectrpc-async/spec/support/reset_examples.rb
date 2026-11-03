# frozen_string_literal: true

RSpec.shared_examples "HTTP/2 peer reset during headers" do
  %i[h2c tls].each do |mode|
    it "skips response body output after a peer reset during #{mode} response headers" do
      reactor do |task|
        with_ca do |ca_file|
          entered = Async::Queue.new
          release_handler = Async::Queue.new
          contexts = {}
          host = cleanup_host(server: server(echo: lambda { |message, context|
            contexts[message.name] = context
            if message.name == "sibling"
              entered.enqueue(true)
              release_handler.dequeue
            end
            message
          }), tls: mode == :tls ? host_tls : nil)
          origin = "#{mode == :tls ? 'https' : 'http'}://localhost:#{host.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: mode == :tls ? ca_file : nil)
          typed = client(transport, origin)
          sibling = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "sibling"), timeout: nil) }
          entered.dequeue
          resource = cleanup_session(host).instance_variable_get(:@resources).first
          gate = FrameGate.new(resource.socket, frame_type: 1)
          resource.connection.stream.instance_variable_set(:@io, gate)
          gate.armed = true
          call = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "cancel"), timeout: nil) }
          gate.started.dequeue
          expect(contexts.fetch("cancel").cancellation_reason).to be_nil
          writer = resource.connection.writers.keys.first
          stream = resource.connection.streams.values.max_by(&:id)
          call.cancel
          call.wait
          wait_until(task, "peer reset during header flush") { stream.closed? }
          expect(contexts.fetch("cancel").cancellation_reason).to be_nil
          expect(stream.processing_cancellation).to be_nil
          gate.release.enqueue(true)
          wait_until(task, "response header writer completed") { writer.finished? }
          release_handler.enqueue(true)
          expect(sibling.wait.message.name).to eq("sibling")
          expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
          expect(cleanup_session(host).instance_variable_get(:@resources)).to eq([resource])
          expect(resource.raw).not_to be_closed
          # reactor also asserts that no background task emitted an unhandled failure.
        ensure
          gate&.release&.enqueue(true)
          release_handler&.enqueue(true)
          transport&.close(grace: 0)
          host&.close(grace: 0)
        end
      end
    end
  end
end
