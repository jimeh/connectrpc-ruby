# frozen_string_literal: true

RSpec.describe "gRPC owned cleanup outcome" do
  def cleanup_call(core, method, **, &block) # rubocop:disable Metrics/CyclomaticComplexity
    message = AsyncHelpers::MESSAGE.new(name: "reply")
    case method.cardinality
    when :unary
      core.call(method, message, **)
    when :server_stream
      core.server_stream(method, message, **) do |reader|
        block ? block.call(reader) : reader.each { |_| nil }
      end
    when :client_stream
      core.client_stream(method, **) do |writer|
        block ? block.call(writer) : writer.send_message(message)
      end
    else
      core.bidi_stream(method, **) do |duplex|
        block ? block.call(duplex) : duplex.each { |_| nil }
      end
    end
  end

  def with_cleanup_peer(kind, failure:, remote_error: false, source: nil, expire: false) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    reactor do |_task|
      method = kind.new(service: "test.Cleanup", name: "Call", request_type: AsyncHelpers::MESSAGE,
                        response_type: AsyncHelpers::MESSAGE)
      handler = lambda do |*args|
        raise ConnectRPC::Error.new(:not_found, "remote") if remote_error

        message = AsyncHelpers::MESSAGE.new(name: "reply")
        case method.cardinality
        when :server_stream then args[1].send_message(message)
        when :bidi_stream then args[0].send_message(message)
        else message
        end
      end
      outcomes = []
      completions = []
      observer = Object.new
      observer.define_singleton_method(:processing_finished) { |value| outcomes << value }
      observer.define_singleton_method(:finished) { |value| completions << value }
      subscriber = Object.new
      subscriber.define_singleton_method(:start) { |_| observer }
      bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
      target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
                                      handlers: { method.path => handler }, protocol: :grpc,
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      original = IOError.new("private owned cleanup failure")
      close_calls = release_calls = 0
      operations = []
      allow(transport).to receive(:dispatch_stream).and_wrap_original do |dispatch, operation, body|
        dispatch.call(operation, body)
        operations << operation
        if failure == :task
          teardown = Object.new
          teardown.define_singleton_method(:finished?) { true }
          teardown.define_singleton_method(:current?) { false }
          teardown.define_singleton_method(:wait) { raise original }
          operation.fetch(:tasks)[teardown] = true
        end
        response = operation.fetch(:response)
        allow(response).to receive(:close).and_wrap_original do |close|
          close_calls += 1
          close.call
          source&.cancel
          allow(operation.fetch(:request).deadline).to receive(:remaining).and_return(0.0) if expire
          raise original if failure == :close

          # Hold the owned close until the real 100ms cleanup budget expires.
          Async::Queue.new.dequeue if failure == :timeout
        end
      end
      pool = transport.instance_variable_get(:@pool)
      allow(pool).to receive(:release).and_wrap_original do |release, connection|
        release_calls += 1
        raise original if failure == :release_before

        release.call(connection)
        raise original if failure == :release
      end
      core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: :grpc,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new, instrumentation: bus)
      yield core, method, original, outcomes, completions
      expect(close_calls).to eq(failure == :task ? 0 : 1)
      expect(release_calls).to eq(1)
      expect(operations.first[:cleaned]).to be(true)
      expect(operations.first[:connection]).to be_nil
      expect(operations.first.fetch(:tasks).keys).to all(be_finished)
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(pool.resources.values.sum).to eq(0)
      expect(transport.instance_variable_get(:@resources).first.finished?).to eq(failure != :none)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  [ConnectRPC::Method, ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod,
   ConnectRPC::BidiStreamMethod].each do |kind|
    %i[none close release release_before timeout task].each do |failure|
      it "commits #{kind} only after #{failure} owned cleanup and retires every owner" do
        with_cleanup_peer(kind, failure: failure) do |core, method, original, outcomes, completions|
          if failure == :none
            cleanup_call(core, method, timeout: nil)
            expect(outcomes.last.code).to eq("OK")
            expect(completions.last.delivery).to eq(:completed)
          else
            expect { cleanup_call(core, method, timeout: nil) }.to raise_error(ConnectRPC::Error) { |error|
              expect(error.code).to eq("unavailable")
              expect(error.cause).to be_nil
              expect(error.message).not_to include(original.message)
              expect(outcomes.last.normalized_error).to equal(error)
            }
            if failure == :timeout
              expect(outcomes.last.error).to be_a(Async::TimeoutError)
            else
              expect(outcomes.last.error).to equal(original)
            end
            expect(completions.last.delivery).to eq(:aborted)
          end
        end
      end
    end
  end

  %i[none close release].each do |failure|
    it "preserves valid remote failure through #{failure} cleanup and distinguishes delivery abort" do
      with_cleanup_peer(ConnectRPC::Method, failure: failure,
                                            remote_error: true) do |core, method, _, outcomes, completions|
        expect { cleanup_call(core, method, timeout: nil) }.to raise_error(ConnectRPC::Error) { |error|
          expect(error.code).to eq("not_found")
          expect(error.cause).to be_nil
        }
        expect(outcomes.last.code).to eq("not_found")
        expect(completions.last.delivery).to eq(failure == :none ? :completed : :aborted)
        expect(completions.last.error).to be_a(IOError) unless failure == :none
      end
    end
  end

  it "accepts cancellation arriving during failed cleanup before committing candidate success" do
    source = ConnectRPC::CancellationSource.new
    with_cleanup_peer(ConnectRPC::Method, failure: :close,
                                          source: source) do |core, method, original, outcomes, completions|
      expect do
        cleanup_call(core, method, timeout: nil, cancellation: source.token)
      end.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("canceled")
        expect(error.cause).to be_nil
      }
      expect(outcomes.last.error).to equal(original)
      expect(completions.last.delivery).to eq(:aborted)
    end
  end

  it "accepts deadline expiry during failed cleanup before committing candidate success" do
    with_cleanup_peer(ConnectRPC::Method, failure: :close,
                                          expire: true) do |core, method, original, outcomes, completions|
      expect { cleanup_call(core, method, timeout: 30) }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("deadline_exceeded")
        expect(error.cause).to be_nil
      }
      expect(outcomes.last.error).to equal(original)
      expect(completions.last.delivery).to eq(:aborted)
    end
  end

  it "preserves a nonlocal block exit through failed cleanup" do
    with_cleanup_peer(ConnectRPC::BidiStreamMethod, failure: :close) do |core, method, _, _, completions|
      result = cleanup_call(core, method, timeout: nil) do |duplex|
        duplex.receive
        break :left
      end
      expect(result).to eq(:left)
      expect(completions.last.delivery).to eq(:aborted)
    end
  end

  [RuntimeError, ConnectRPC::Error, Interrupt].each do |type|
    it "preserves independent user #{type} identity and backtrace through cleanup failure" do
      with_cleanup_peer(ConnectRPC::BidiStreamMethod, failure: :close) do |core, method, _, _, completions|
        original = type == ConnectRPC::Error ? type.new(:unknown, "user") : type.new("user")
        original.set_backtrace(["application.rb:42"])
        expect do
          cleanup_call(core, method, timeout: nil) do |duplex|
            duplex.receive
            raise original
          end
        end.to(raise_error do |error|
          expect(error).to equal(original)
          expect(error.backtrace).to eq(["application.rb:42"])
        end)
        expect(completions.last.delivery).to eq(:aborted)
      end
    end
  end
end
