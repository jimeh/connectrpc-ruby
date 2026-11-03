# frozen_string_literal: true

RSpec.describe "Streaming SDK context and native completion" do
  def methods
    @methods ||= [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod]
                 .zip(%w[Watch Collect Chat]).map do |type, name|
      type.new(service: "trace.Streams", name: name, request_type: AsyncHelpers::MESSAGE,
               response_type: AsyncHelpers::MESSAGE)
    end
  end

  def target(handlers, instrumentation:, binding: ConnectRPC::Bindings::GoogleProtobuf.new)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: "trace.Streams", methods: methods)],
                           handlers: handlers, binding: binding, instrumentation: instrumentation)
  end

  def host_for(backend, server, **)
    if backend == :async
      host = ConnectRPC::Async::Host.new(server: server, **).start
      [host, "https://127.0.0.1:#{host.port}", nil]
    else
      bound = ConnectRPC::Falcon::Endpoint.parse("https://127.0.0.1:0").bound
      host = ConnectRPC::Falcon::Server.new(server: server, endpoint: bound, **)
      host.run
      [host, "https://127.0.0.1:#{bound.sockets.first.local_address.ip_port}", bound]
    end
  end

  %i[async falcon].each do |backend|
    it "retains #{backend} processor identity and SDK context through codecs/cleanup for all shapes under mTLS" do
      provider, exporter, bus = tracing
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        records = []
        host = nil
        observe = lambda do |phase|
          records << [phase, Async::Task.current, current_span.context.span_id, host.peer_certificate&.serial&.to_i]
        end
        original = ConnectRPC::Bindings::GoogleProtobuf.new
        binding = Object.new
        %i[encode decode].each do |operation|
          binding.define_singleton_method(operation) do |value, **options|
            observe.call(operation)
            original.public_send(operation, value, **options)
          ensure
            observe.call(:codec_cleanup)
          end
        end
        handler = lambda do |context|
          processor = Async::Task.current
          observe.call(:handler)
          task.async do
            expect(host.peer_certificate).to be_nil
            expect(current_span.context).not_to be_valid
          end.wait
          context.send_headers(metadata: { "x-early" => "yes" })
          entered.enqueue([processor, context])
          release.dequeue
          expect(context.cancellation_reason).to be_nil
          observe.call(:handler_cleanup)
        end
        handlers = {
          methods[0].path => lambda { |message, writer, context|
            handler.call(context)
            writer.send_message(message)
          },
          methods[1].path => lambda { |reader, context|
            handler.call(context)
            message = reader.receive
            expect(reader.receive).to be_nil
            message
          },
          methods[2].path => lambda { |duplex, context|
            handler.call(context)
            duplex.each { |message| duplex.send_message(message) }
          }
        }
        host, origin, bound = host_for(backend, target(handlers, binding: binding, instrumentation: bus),
                                       tls: host_tls(anchors: [tls_material.fetch(:client_ca)]))
        with_ca do |ca_file|
          certificate, key = tls_material.fetch(:identity)
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file,
                                                       certificate_chain: [certificate.to_pem],
                                                       private_key: key.private_to_pem)
          core = ConnectRPC::Client.new(base_url: origin, transport: transport, instrumentation: bus,
                                        binding: original)
          calls = [
            task.async do
              core.server_stream(methods[0], AsyncHelpers::MESSAGE.new(name: "ss")) do |r|
                r.each do |_|
                  nil
                end
              end
            end,
            task.async { core.client_stream(methods[1]) { |w| w.send_message(AsyncHelpers::MESSAGE.new(name: "cs")) } },
            task.async do
              core.bidi_stream(methods[2]) do |duplex|
                duplex.send_message(AsyncHelpers::MESSAGE.new(name: "bidi"))
                duplex.close_send
                duplex.each { |_| nil }
              end
            end
          ]
          processors = 3.times.map { entered.dequeue.first }
          expect(processors.uniq.size).to eq(3)
          expect(rpc_spans(exporter)).to be_empty
          3.times { release.enqueue(true) }
          calls.each(&:wait)
          wait_until(task, "six streaming SDK spans") { rpc_spans(exporter).size == 6 }
          receivers = rpc_spans(exporter).select { |span| span.kind == :server }
          expect(receivers.map(&:span_id).uniq.size).to eq(3)
          records.group_by { |record| record[1] }.each do |processor, events|
            expect(processors).to include(processor)
            expect(events.map(&:last).uniq).to eq([certificate.serial.to_i])
            expect(events.map { |record| record[2] }.uniq.size).to eq(1)
            expect(receivers.map(&:span_id)).to include(events.first[2])
            expect(events.map(&:first)).to include(:handler, :handler_cleanup, :encode, :decode, :codec_cleanup)
          end
          expect(receivers.map { |span| span.attributes["connectrpc.delivery"] }).to eq(["completed"] * 3)
          expect(rpc_spans(exporter).map { |span| Array(span.events).size }).to eq([0] * 6)
          transport.instance_variable_get(:@resources).each do |resource|
            stack = resource.connection.reader_task.fiber.opentelemetry_context
            expect(stack.nil? || stack.empty?).to be(true)
          end
          expect(host.peer_certificate).to be_nil
          expect(current_span.context).not_to be_valid
        ensure
          transport&.close(grace: 0)
        end
      ensure
        3.times { release&.enqueue(true) }
        host&.close(grace: 0)
        bound&.close
        provider.shutdown
      end
    end

    [false, true].each do |handler_failure|
      it "gates #{backend} peer-reset observation on original cleanup, handler failure #{handler_failure}" do
        facts = []
        cleanup_done = false
        processor = nil
        context = nil
        host = nil
        observer = Object.new
        observer.define_singleton_method(:start) { |_| observer }
        observer.define_singleton_method(:processing_finished) do |outcome|
          facts << [:processing, cleanup_done, Async::Task.current, OpenTelemetry::Trace.current_span.context,
                    host.peer_certificate&.serial&.to_i, outcome]
        end
        observer.define_singleton_method(:finished) do |completion|
          facts << [:finished, cleanup_done, Async::Task.current, OpenTelemetry::Trace.current_span.context,
                    host.peer_certificate&.serial&.to_i, completion]
        end
        provider, exporter, bus = tracing(subscribers: [observer])
        reactor do |task|
          entered = Async::Queue.new
          release = Async::Queue.new
          resets = Async::Queue.new
          original_error = ConnectRPC::Error.new(:not_found, "original handler failure")
          handlers = { methods[0].path => lambda { |message, writer, active|
            context = active
            writer.send_message(message)
            raise original_error if handler_failure
          } }
          host, origin, bound = host_for(backend, target(handlers, instrumentation: bus),
                                         tls: host_tls(anchors: [tls_material.fetch(:client_ca)]))
          session = host_session(host)
          application = session.instance_variable_get(:@application)
          allow(application).to receive(:call_with_publication).and_wrap_original do |original, *args, **options|
            processor = Async::Task.current
            stream = args.first.stream
            allow(stream).to receive(:receive_reset_stream).and_wrap_original do |receive, frame|
              result = receive.call(frame)
              resets.enqueue(frame.unpack)
              result
            end
            begin
              original.call(*args, **options)
            ensure
              entered.enqueue([Async::Task.current, current_span.context, host.peer_certificate&.serial&.to_i])
              release.dequeue
              expect(Async::Task.current).to equal(processor)
              expect(current_span.context).to be_valid
              expect(host.peer_certificate.serial.to_i).to eq(201)
              cleanup_done = true
            end
          end
          with_ca do |ca_file|
            certificate, key = tls_material.fetch(:identity)
            transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file,
                                                         certificate_chain: [certificate.to_pem],
                                                         private_key: key.private_to_pem)
            core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                          binding: ConnectRPC::Bindings::GoogleProtobuf.new)
            source = ConnectRPC::CancellationSource.new
            payload = Async::Queue.new
            call = task.async do
              message = AsyncHelpers::MESSAGE.new(name: "reset")
              core.server_stream(methods[0], message, timeout: nil, cancellation: source.token) do |reader|
                expect(reader.receive.name).to eq("reset")
                payload.enqueue(true)
                reader.receive
              end
            rescue ConnectRPC::Error => e
              e
            end
            cleanup_entry = entered.dequeue
            payload.dequeue
            expect(cleanup_entry[0]).to equal(processor)
            expect(cleanup_entry[1]).to be_valid
            expect(cleanup_entry[2]).to eq(201)
            source.cancel
            expect(call.wait.code).to eq("canceled")
            expect(resets.dequeue).to eq(Protocol::HTTP2::Error::CANCEL)
            # Call completion and actual peer reset establish the negative boundary.
            expect(facts).to be_empty
            expect(rpc_spans(exporter)).to be_empty
            expect(processor).not_to be_finished
            release.enqueue(true)
            processor.wait
            wait_until(task, "exactly one server completion after original cleanup") { facts.size == 2 }
            expect(facts.map(&:first)).to eq(%i[processing finished])
            expect(facts.map { |entry| entry[1] }).to eq([true, true])
            expect(facts.map { |entry| entry[2] }).to eq([processor, processor])
            expect(facts.map { |entry| entry[3].span_id }).to eq([cleanup_entry[1].span_id] * 2)
            expect(facts.map { |entry| entry[4] }).to eq([201, 201])
            outcome = facts.first.last
            expect(outcome.code).to eq(handler_failure ? "not_found" : "OK")
            expect(outcome.error).to equal(handler_failure ? original_error : nil)
            expect(facts.last.last.delivery).to eq(:aborted)
            expect(context).to be_a(ConnectRPC::StreamingContext)
            expect(rpc_spans(exporter).size).to eq(1)
            expect(Array(rpc_spans(exporter).first.events)).to be_empty
            expect(current_span.context).not_to be_valid
          ensure
            release.enqueue(true)
            transport&.close(grace: 0)
          end
        ensure
          release&.enqueue(true)
          host&.close(grace: 0)
          bound&.close
          provider.shutdown
        end
      end
    end

    it "retains #{backend} SDK spans and managed graceful drain through an actual final DATA flush" do
      provider, exporter, bus = tracing
      reactor do |task|
        handlers = { methods[0].path => ->(message, writer, _) { writer.send_message(message) } }
        host, origin, bound = host_for(backend, target(handlers, instrumentation: bus), tls: host_tls)
        with_ca do |ca_file|
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
          core = ConnectRPC::Client.new(base_url: origin, transport: transport, instrumentation: bus,
                                        binding: ConnectRPC::Bindings::GoogleProtobuf.new)
          message = AsyncHelpers::MESSAGE.new(name: "tail")
          core.server_stream(methods[0], message) { |reader| reader.each { |_| nil } }
          wait_until(task, "warm streaming spans") { rpc_spans(exporter).size == 2 }
          exporter.reset
          resource = host_session(host).instance_variable_get(:@resources).first
          gate_type = Class.new(FrameGate) do
            def write(buffer)
              bytes = buffer.to_s
              final_data = bytes.getbyte(3).zero? && bytes.getbyte(4).anybits?(1)
              return __getobj__.write(buffer) unless final_data

              super
            end
          end
          gate = gate_type.new(resource.socket, frame_type: 0)
          resource.connection.stream.instance_variable_set(:@io, gate)
          gate.armed = true
          call = task.async do
            core.server_stream(methods[0], message, timeout: nil) { |reader| reader.each { |_| nil } }
          end
          gate.started.dequeue
          expect(call).not_to be_finished
          expect(rpc_spans(exporter)).to be_empty
          expect(host_session(host).instance_variable_get(:@admitted).size).to eq(1)
          session = host_session(host)
          closing = task.async { host.close(grace: 2) }
          wait_until(task, "managed streaming drain started") { session.instance_variable_get(:@state) != :running }
          expect(closing).not_to be_finished
          expect(session.instance_variable_get(:@admitted).size).to eq(1)
          gate.release.enqueue(true)
          expect(call.wait).to be_nil
          closing.wait
          wait_until(task, "exactly two spans after final DATA flush") { rpc_spans(exporter).size == 2 }
          expect(rpc_spans(exporter).map { |span| span.attributes["connectrpc.delivery"] }).to eq(["completed"] * 2)
          expect(current_span.context).not_to be_valid
        ensure
          gate&.release&.enqueue(true)
          transport&.close(grace: 0)
        end
      ensure
        host&.close(grace: 0)
        bound&.close
        provider.shutdown
      end
    end

    %i[explicit nonlocal].each do |exit_kind|
      it "exports #{backend} #{exit_kind} abandonment as canceled without completing the shared application source" do
        provider, exporter, bus = tracing
        reactor do |task|
          entered = Async::Queue.new
          release = Async::Queue.new
          handlers = { methods[0].path => lambda { |message, writer, context|
            writer.send_message(message)
            if message.name == "abandon"
              entered.enqueue(context)
              release.dequeue
              context.check!
            end
          } }
          host, origin, bound = host_for(backend, target(handlers, instrumentation: bus), tls: host_tls)
          with_ca do |ca_file|
            source = ConnectRPC::CancellationSource.new
            transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
            core = ConnectRPC::Client.new(base_url: origin, transport: transport, instrumentation: bus,
                                          binding: ConnectRPC::Bindings::GoogleProtobuf.new)
            message = AsyncHelpers::MESSAGE.new(name: "abandon")
            result = core.server_stream(methods[0], message, timeout: nil, cancellation: source.token) do |reader|
              expect(reader.receive.name).to eq("abandon")
              break :local_return if exit_kind == :nonlocal

              reader.abandon
            end
            expect(result).to eq(exit_kind == :nonlocal ? :local_return : nil)
            context = entered.dequeue
            wait_until(task, "abandonment sent actual peer CANCEL") { context.cancellation_reason == :canceled }
            expect(source.token.reason).to be_nil
            release.enqueue(true)
            wait_until(task, "two canceled abandonment SDK spans") { rpc_spans(exporter).size == 2 }
            codes = rpc_spans(exporter).map { |span| span.attributes["rpc.response.status_code"] }
            expect(codes).to eq(%w[canceled canceled])
            expect(rpc_spans(exporter).map { |span| span.attributes["connectrpc.delivery"] }).to eq(%w[aborted aborted])
            caller = rpc_spans(exporter).find { |span| span.kind == :client }
            expect(caller.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
            expect(current_span.context).not_to be_valid
            message = AsyncHelpers::MESSAGE.new(name: "healthy")
            core.server_stream(methods[0], message, timeout: nil, cancellation: source.token) do |reader|
              reader.each { |_| nil }
            end
            wait_until(task, "one healthy call reused the shared source") { rpc_spans(exporter).size == 4 }
            expect(rpc_spans(exporter).count { |span| span.attributes["rpc.response.status_code"] == "OK" }).to eq(2)
          ensure
            transport&.close(grace: 0)
          end
        ensure
          release&.enqueue(true)
          host&.close(grace: 0)
          bound&.close
          provider.shutdown
        end
      end
    end

    %i[grace_expiry fatal_abort].each do |stop|
      it "retires #{backend} original processing cleanup on #{stop} with one aborted SDK completion" do
        provider, exporter, bus = tracing
        reactor do |task|
          entered = Async::Queue.new
          release = Async::Queue.new
          cleanup = []
          context = nil
          handlers = { methods[0].path => lambda { |message, writer, current|
            context = current
            writer.send_message(message)
          } }
          host, origin, bound = host_for(backend, target(handlers, instrumentation: bus), tls: host_tls)
          session = host_session(host)
          application = session.instance_variable_get(:@application)
          allow(application).to receive(:call_with_publication).and_wrap_original do |original, *args, **options|
            processor = Async::Task.current
            begin
              result = original.call(*args, **options)
              entered.enqueue(processor)
              release.dequeue
              result
            ensure
              cleanup << [Async::Task.current, current_span.context]
            end
          end
          with_ca do |ca_file|
            transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
            core = ConnectRPC::Client.new(base_url: origin, transport: transport, instrumentation: bus,
                                          binding: ConnectRPC::Bindings::GoogleProtobuf.new)
            received = Async::Queue.new
            call = task.async do
              core.server_stream(methods[0], AsyncHelpers::MESSAGE.new(name: "stop"), timeout: nil) do |reader|
                expect(reader.receive.name).to eq("stop")
                received.enqueue(true)
                reader.receive
              end
            rescue ConnectRPC::Error => e
              e
            end
            processor = entered.dequeue
            received.dequeue
            expect(rpc_spans(exporter)).to be_empty
            expect(session.instance_variable_get(:@admitted).size).to eq(1)
            stop == :fatal_abort ? session.abort : host.close(grace: 0.03)
            expect(call.wait.code).to eq("unavailable")
            wait_until(task, "aborted streaming spans and processor joined") do
              rpc_spans(exporter).size == 2 && processor.finished?
            end
            expect(context.cancellation_reason).to be_nil
            expect(cleanup.size).to eq(1)
            expect(cleanup.first.first).to equal(processor)
            expect(cleanup.first.last).to be_valid
            expect(session.instance_variable_get(:@admitted)).to be_empty
            expect(session.instance_variable_get(:@resources)).to all(be_finished)
            server_span = rpc_spans(exporter).find { |span| span.kind == :server }
            expect(server_span.attributes["connectrpc.delivery"]).to eq("aborted")
            expect(current_span.context).not_to be_valid
          ensure
            transport&.close(grace: 0)
          end
        ensure
          release&.enqueue(true)
          host&.close(grace: 0)
          bound&.close
          provider.shutdown
        end
      end
    end

    [false, true].each do |late_failure|
      it "holds #{backend} successful EOF until original processor cleanup, late failure #{late_failure}" do
        provider, exporter, bus = tracing
        reactor do |task|
          cleaned = Async::Queue.new
          release = Async::Queue.new
          handlers = { methods[0].path => ->(message, writer, _) { writer.send_message(message) } }
          host, origin, bound = host_for(backend, target(handlers, instrumentation: bus), tls: host_tls)
          session = host_session(host)
          application = session.instance_variable_get(:@application)
          processor = nil
          allow(application).to receive(:call_with_publication).and_wrap_original do |original, *args, **options|
            processor = Async::Task.current
            result = original.call(*args, **options)
            cleaned.enqueue(current_span.context)
            release.dequeue
            expect(Async::Task.current).to equal(processor)
            expect(current_span.context).to be_valid
            raise "late processor cleanup failure" if late_failure

            result
          end
          with_ca do |ca_file|
            transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
            core = ConnectRPC::Client.new(base_url: origin, transport: transport, instrumentation: bus,
                                          binding: ConnectRPC::Bindings::GoogleProtobuf.new)
            received = Async::Queue.new
            call = task.async do
              core.server_stream(methods[0], AsyncHelpers::MESSAGE.new(name: "cleanup"), timeout: nil) do |reader|
                expect(reader.receive.name).to eq("cleanup")
                received.enqueue(true)
                reader.receive
              end
            rescue ConnectRPC::Error => e
              e
            end
            span_context = cleaned.dequeue
            received.dequeue
            expect(call).not_to be_finished
            expect(rpc_spans(exporter)).to be_empty
            expect(session.instance_variable_get(:@admitted).size).to eq(1)
            release.enqueue(true)
            result = call.wait
            if late_failure
              expect(result).to be_a(ConnectRPC::Error)
              expect(result.code).to eq("unavailable")
            else
              expect(result).to be_nil
            end
            wait_until(task, "late processing span completion") { rpc_spans(exporter).size == 2 }
            server_span = rpc_spans(exporter).find { |span| span.kind == :server }
            expect(server_span.span_id).to eq(span_context.span_id)
            expect(server_span.attributes["connectrpc.delivery"]).to eq(late_failure ? "aborted" : "completed")
            expect(server_span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR) if late_failure
          ensure
            transport&.close(grace: 0)
          end
        ensure
          release&.enqueue(true)
          host&.close(grace: 0)
          bound&.close
          provider.shutdown
        end
      end
    end
  end
end
