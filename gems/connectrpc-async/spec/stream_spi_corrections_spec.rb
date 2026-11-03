# frozen_string_literal: true

RSpec.describe "Direct streaming SPI cancellation composition" do
  def stream_request(origin, deadline:, cancellation:)
    ConnectRPC::StreamRequest.new(url: URI("#{origin}/direct.Streams/Chat"), http_method: "POST",
                                  headers: { "content-type" => ["application/connect+proto"] },
                                  deadline: deadline, cancellation: cancellation)
  end

  %i[deadline token both expired_and_canceled prelatched].each do |variant|
    it "arbitrates #{variant} before setup without dispatch or source completion" do
      reactor do
        listener = TCPServer.new("127.0.0.1", 0)
        origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        source = ConnectRPC::CancellationSource.new
        clock = 0.0
        deadline = ConnectRPC::Deadline.new(1, clock: -> { clock })
        cancellation = source.token unless variant == :deadline
        if variant == :prelatched
          cancellation = ConnectRPC::CallCancellation.new(deadline: deadline, cancellation: source.token)
          source.cancel
          expect(cancellation.reason).to eq(:canceled)
        end
        clock = 2.0 unless variant == :token
        source.cancel if %i[token expired_and_canceled].include?(variant)
        outgoing = stream_request(origin, deadline: deadline, cancellation: cancellation)
        expect do
          transport.open_stream(outgoing) { raise "must not yield" }
        end.to raise_error(ConnectRPC::Error) { |error|
          expect(error.code).to eq(%i[token prelatched].include?(variant) ? "canceled" : "deadline_exceeded")
          expect(error.cause).to be_nil
        }
        expect(outgoing.deadline).to equal(deadline)
        expect(outgoing.cancellation).to equal(cancellation)
        expect(listener.accept_nonblock(exception: false)).to eq(:wait_readable)
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@acquiring)).to be_empty
      ensure
        transport&.close(grace: 0)
        listener&.close
      end
    end
  end

  [false, true].each do |token|
    it "honors actual deadline while waiting for real response headers, raw token #{token}" do
      reactor do
        entered = Async::Queue.new
        release = Async::Queue.new
        method = ConnectRPC::BidiStreamMethod.new(service: "direct.Streams", name: "Chat", request_type: String,
                                                  response_type: String)
        binding = Object.new
        binding.define_singleton_method(:encode) { |message, **| message }
        binding.define_singleton_method(:decode) { |message, **| message }
        target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
                                        binding: binding,
                                        handlers: { method.path => lambda { |_, _|
                                          entered.enqueue(true)
                                          release.dequeue
                                        } })
        host = ConnectRPC::Async::Host.new(server: target).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        source = ConnectRPC::CancellationSource.new
        outgoing = stream_request(origin, deadline: ConnectRPC::Deadline.new(0.1),
                                          cancellation: token ? source.token : nil)
        expect do
          transport.open_stream(outgoing) do |exchange|
            exchange.close_send
            entered.dequeue
            exchange.response_head
          end
        end.to raise_error(ConnectRPC::Error) { |error|
          expect(error.code).to eq("deadline_exceeded")
          expect(error.cause).to be_nil
        }
        expect(source.token.reason).to be_nil
        expect(transport.instance_variable_get(:@calls)).to be_empty
      ensure
        release&.enqueue(true)
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end

  %i[token deadline deadline_only core].each do |owner|
    it "keeps #{owner} arbitration open through transport cleanup and preserves the shared source" do
      reactor do
        host = ConnectRPC::Async::Host.new(server: server).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        source = ConnectRPC::CancellationSource.new
        clock = 0.0
        deadline = ConnectRPC::Deadline.new(1, clock: -> { clock }) unless owner == :token
        cancellation = case owner
                       when :core then ConnectRPC::CallCancellation.new(cancellation: source.token)
                       when :deadline_only then nil
                       else source.token
                       end
        outgoing = stream_request(origin, deadline: deadline, cancellation: cancellation)
        allow(transport).to receive(:cleanup).and_wrap_original do |original, *args|
          result = original.call(*args)
          %i[deadline deadline_only].include?(owner) ? clock = 2.0 : source.cancel
          result
        end
        work = lambda do
          transport.open_stream(outgoing) do |exchange|
            expect(outgoing.deadline).to equal(deadline)
            exchange.close_send
          end
        end
        if owner == :core
          work.call
          expect { cancellation.commit! }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
        else
          expect(&work).to raise_error(ConnectRPC::Error) { |error|
            expect(error.code).to eq(%i[deadline deadline_only].include?(owner) ? "deadline_exceeded" : "canceled")
          }
        end
        expect(source.token.reason).to eq(%i[deadline deadline_only].include?(owner) ? nil : :canceled)
        expect(transport.instance_variable_get(:@calls)).to be_empty
      ensure
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end
  %i[read write finish].each do |boundary|
    it "checks raw token plus actual deadline at the #{boundary} boundary" do
      reactor do
        host = ConnectRPC::Async::Host.new(server: server).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        source = ConnectRPC::CancellationSource.new
        clock = 0.0
        deadline = ConnectRPC::Deadline.new(1, clock: -> { clock })
        outgoing = stream_request(origin, deadline: deadline, cancellation: source.token)
        outgoing.url.path = AsyncHelpers::ECHO::ECHO.path
        expect do
          transport.open_stream(outgoing) do |exchange|
            expect(exchange.response_head.status).to eq(200)
            clock = 2.0
            case boundary
            when :read then exchange.read(1)
            when :write then exchange.write("must not send")
            else exchange.finish
            end
          end
        end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
        expect(source.token.reason).to be_nil
        expect(transport.instance_variable_get(:@calls)).to be_empty
      ensure
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end

  [false, true].each do |token|
    it "keeps direct nil deadline actual through headers/read/finish, raw token #{token}" do
      reactor do
        host = ConnectRPC::Async::Host.new(server: server).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        source = ConnectRPC::CancellationSource.new
        outgoing = stream_request(origin, deadline: nil, cancellation: token ? source.token : nil)
        outgoing.url.path = AsyncHelpers::ECHO::ECHO.path
        transport.open_stream(outgoing) do |exchange|
          expect(outgoing.deadline).to be_nil
          exchange.close_send
          expect(exchange.response_head.status).to eq(200)
          bytes = +""
          while (chunk = exchange.read(16_384))
            bytes << chunk
          end
          expect(JSON.parse(bytes.byteslice(5..)).dig("error", "code")).to eq("unimplemented")
          expect(exchange.read(1)).to be_nil
          exchange.finish
        end
        expect(source.token.reason).to be_nil
        expect(transport.instance_variable_get(:@calls)).to be_empty
      ensure
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end
end
