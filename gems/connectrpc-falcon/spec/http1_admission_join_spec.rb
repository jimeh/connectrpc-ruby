# frozen_string_literal: true

require_relative "support/http1_helpers"

RSpec.describe "Connect H1 terminal admission join" do
  include ConnectH1Helpers

  def join_maps(session)
    [session.instance_variable_get(:@admitted).dup, session.instance_variable_get(:@h1_wire).dup]
  end

  def owned_tasks_finished?(session)
    session.instance_variable_get(:@resources).all? do |resource|
      resource.finished? && resource.task.finished? && resource.instance_variable_get(:@tasks).all?(&:finished?)
    end
  end

  def observe_retirement(&observation)
    owner_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
    allow(owner_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
      constructor.call(*args, **options).tap do |owner|
        allow(owner).to receive(:on_close).and_wrap_original do |register, &completion|
          register.call { observation.call(completion) }
        end
      end
    end
  end

  %i[drain abort].each do |operation|
    it "joins prepublication processing and retirement during natural TLS #{operation}" do
      reactor do |task|
        ss, = methods
        entered = Async::Queue.new
        finished = Async::Queue.new
        handler = lambda do |message, writer, context|
          if message.name == "held"
            entered.enqueue(true)
            Async::Condition.new.wait
          else
            writer.send_message(message)
          end
        ensure
          finished.enqueue(context.cancelled?)
        end
        with_host(target({ ss.path => handler }), true, max_requests: 1) do |host, peer, origin|
          bytes = frame("held")
          peer.write(request_head(ss, :fixed, length: bytes.bytesize) + bytes)
          entered.dequeue
          session = host.instance_variable_get(:@session)
          expect(join_maps(session).map(&:values)).to eq([[false], []])
          if operation == :drain
            host.reload_tls(**host_tls)
            host.drain_tls(grace: 0)
          else
            host.abort
          end
          expect(finished.dequeue).to be(false)
          wait_until(task, "original connection and processor tasks stopped") { owned_tasks_finished?(session) }
          expect(join_maps(session)).to eq([{}, {}])
          next if operation == :abort

          other = ConnectH1Wire.new(origin, context: tls_context)
          bytes = frame("fresh")
          other.write(request_head(ss, :fixed, length: bytes.bytesize) + bytes)
          expect(other.head.first).to eq(200)
          expect(other.body).to include("fresh")
          expect(finished.dequeue).to be(false)
          wait_until(task, "fresh max_requests:1 admission released") { join_maps(session) == [{}, {}] }
        ensure
          other&.close
        end
      end
    end
  end

  [false, true].each do |secure|
    label = secure ? "TLS" : "TCP"

    %i[application construction].each do |failure|
      it "retains admission until retirement after #{failure} fails before publication over #{label}" do
        reactor do |task|
          ss, = methods
          session = nil
          observed = Async::Queue.new
          observe_retirement do |completion|
            observed.enqueue(join_maps(session).map(&:values))
            completion.call
          end
          application_type = ConnectRPC::Async::Application
          if failure == :application
            allow_any_instance_of(application_type).to receive(:call_with_publication)
              .and_raise(RuntimeError, "external application failure before publication")
          else
            allow_any_instance_of(application_type).to receive(:publication?)
              .and_raise(RuntimeError, "failure before processor construction")
          end
          with_host(target({ ss.path => ->(*) { raise "handler invoked" } }), secure) do |host, peer, _|
            session = host.instance_variable_get(:@session)
            bytes = frame("failed")
            peer.write(request_head(ss, :fixed, length: bytes.bytesize + 10) + bytes)
            before_close = observed.dequeue
            wait_until(task, "failed connection and all processors finished") { owned_tasks_finished?(session) }
            expect(before_close).to eq([[true], []])
            expect(join_maps(session)).to eq([{}, {}])
          end
        end
      end
    end

    it "retains an active processor after exceptional dispatch and retirement over #{label}" do
      reactor do |task|
        ss, = methods
        session = nil
        retired = Async::Queue.new
        release_processor = Async::Queue.new
        release_close = Async::Queue.new
        processing_finished = Async::Queue.new
        observe_retirement do |completion|
          before_close = join_maps(session).map(&:values)
          completion.call
          retired.enqueue([before_close, join_maps(session).map(&:values)])
          release_close.dequeue
        end
        allow_any_instance_of(ConnectRPC::Async::Application).to receive(:call_with_publication) do |_, _request,
                                                                                                   publication:, **|
          publication.complete(RuntimeError.new("publication failed while processing remains active"))
          release_processor.dequeue
        ensure
          processing_finished.enqueue(true)
        end
        with_host(target({ ss.path => ->(*) { raise "handler invoked" } }), secure) do |host, peer, _|
          session = host.instance_variable_get(:@session)
          bytes = frame("active")
          peer.write(request_head(ss, :fixed, length: bytes.bytesize + 10) + bytes)
          expect(retired.dequeue).to eq([[[false], []], [[false], [true]]])
          release_processor.enqueue(true)
          processing_finished.dequeue
          wait_until(task, "original processor joined already finished retirement") { join_maps(session) == [{}, {}] }
          release_close.enqueue(true)
          wait_until(task, "failed connection tasks stopped without reinsertion") { owned_tasks_finished?(session) }
          expect(join_maps(session)).to eq([{}, {}])
        ensure
          release_processor.enqueue(true)
          release_close.enqueue(true)
        end
      end
    end
  end
end
