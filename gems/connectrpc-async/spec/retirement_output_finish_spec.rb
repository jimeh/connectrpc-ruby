# frozen_string_literal: true

require_relative "support/retirement_helpers"

# Resource#abort marks the resource finished, then retirement closes raw TCP and
# yields before it marks the connection closed. An output task that is already
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

  it "completes a client request body that ends inside the window without a background failure" do
    reactor do |task|
      open_chat(task) do |_host, exchange|
        operation = exchange.instance_variable_get(:@operation)
        resource = operation.fetch(:resource)
        output = operation.fetch(:response).stream.output_task
        expect(output).not_to be_finished
        # Wake the output task, then start retirement before it can run.
        exchange.instance_variable_get(:@body).close_write
        resource.abort
        expect(retirement_complete?(resource)).to be(true)
        expect(output).to be_completed
      end
    end
  end

  it "keeps a server response body's own failure inside the window rather than attempting a reset" do
    reactor do |task|
      open_chat(task) do |host, _exchange|
        resource = host.instance_variable_get(:@session).instance_variable_get(:@resources).first
        stream = resource.connection.streams.values.first
        output = stream.response_task
        expect(output).not_to be_finished
        failure = IOError.new("response body failed")
        stream.instance_variable_get(:@output).instance_variable_get(:@body).body.close(failure)
        resource.abort
        expect(retirement_complete?(resource)).to be(true)
        expect { output.wait }.to raise_error(equal(failure))
      end
    end
  end
end
