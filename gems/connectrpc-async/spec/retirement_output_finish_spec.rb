# frozen_string_literal: true

require_relative "support/retirement_helpers"

# Retirement closes raw TCP, and Resource#abort marks the resource finished,
# before close! marks the connection closed. An output task that is already
# ready runs in that window and must finish locally without writing.
RSpec.describe "Output finishing inside the retirement window" do
  include RetirementHelpers

  def chat_host(entered, release)
    method = ConnectRPC::BidiStreamMethod.new(service: "direct.Streams", name: "Chat", request_type: String,
                                              response_type: String)
    binding = Object.new
    binding.define_singleton_method(:encode) { |message, **| message }
    binding.define_singleton_method(:decode) { |message, **| message }
    server = ConnectRPC::Server.new(
      services: [ConnectRPC::Service.new(name: method.service, methods: [method])], binding: binding,
      handlers: { method.path => lambda { |duplex, _|
        # Start response output so the server stream owns a live output task.
        duplex.send_message("started")
        entered.enqueue(true)
        release.dequeue
      } }
    )
    ConnectRPC::Async::Host.new(server: server).start
  end

  def open_chat(task)
    entered = Async::Queue.new
    release = Async::Queue.new
    host = chat_host(entered, release)
    origin = "http://127.0.0.1:#{host.port}"
    transport = ConnectRPC::Async::Transport.new(origin: origin)
    outgoing = ConnectRPC::StreamRequest.new(url: URI("#{origin}/direct.Streams/Chat"), http_method: "POST",
                                             headers: { "content-type" => ["application/connect+proto"] })
    transport.open_stream(outgoing) do |exchange|
      task.with_timeout(2) { entered.dequeue }
      yield host, exchange
    end
  ensure
    release&.enqueue(true)
    transport&.close(grace: 0)
    host&.close(grace: 0)
  end

  # Records [connection closed, resource finished, raw failed] when the
  # stream's output first finishes, proving which retirement window it hit.
  def finish_states(stream, &)
    states = []
    trace = TracePoint.new(:call) do |event|
      next unless event.method_id == :finish_output && event.self.equal?(stream) && states.empty?

      connection = stream.connection
      states << [connection.closed?, connection.resource.finished?, connection.resource.send(:raw_failed?)]
    end
    trace.enable { yield states }
    states
  end

  def server_output(host, failure)
    resource = host.instance_variable_get(:@session).instance_variable_get(:@resources).first
    stream = resource.connection.streams.values.first
    output = stream.response_task
    expect(output).not_to be_finished
    # Wake the output task with a body failure; it runs once the caller yields.
    stream.instance_variable_get(:@output).instance_variable_get(:@body).body.close(failure)
    [resource, stream, output]
  end

  it "completes a client request body that ends inside the window without a background failure" do
    reactor do |task|
      open_chat(task) do |_host, exchange|
        operation = exchange.instance_variable_get(:@operation)
        resource = operation.fetch(:resource)
        stream = operation.fetch(:response).stream
        output = stream.output_task
        expect(output).not_to be_finished
        states = finish_states(stream) do
          # Wake the output task, then start retirement before it can run.
          exchange.instance_variable_get(:@body).close_write
          resource.abort
        end
        expect(states).to eq([[false, true, true]])
        expect(retirement_complete?(resource)).to be(true)
        expect(output).to be_completed
      end
    end
  end

  it "keeps a server response body's own failure inside the window rather than attempting a reset" do
    reactor do |task|
      open_chat(task) do |host, _exchange|
        failure = IOError.new("response body failed")
        resource, stream, output = server_output(host, failure)
        states = finish_states(stream) { resource.abort }
        expect(states).to eq([[false, true, true]])
        expect(retirement_complete?(resource)).to be(true)
        expect { output.wait }.to raise_error(equal(failure))
      end
    end
  end

  it "keeps a server response body's own failure after session raw failure and before resource abort" do
    reactor do |task|
      open_chat(task) do |host, _exchange|
        failure = IOError.new("response body failed")
        resource, stream, output = server_output(host, failure)
        session = host.instance_variable_get(:@session)
        states = finish_states(stream) do |seen|
          # Hold the window between session raw failure and resource abort
          # open until the output finishes; the failed socket's own reader
          # would otherwise abort the resource first.
          allow(resource).to receive(:abort).and_wrap_original do |abort, *args|
            deadline = Async::Clock.now + 2
            Async::Task.current.yield while seen.empty? && Async::Clock.now < deadline
            abort.call(*args)
          end
          session.abort
        end
        expect(states).to eq([[false, false, true]])
        expect(retirement_complete?(resource)).to be(true)
        expect { output.wait }.to raise_error(equal(failure))
      end
    end
  end
end
