# frozen_string_literal: true

require_relative "../../connectrpc-falcon/spec/support/http1_helpers"

RSpec.describe "Connect HTTP/1 original processor SDK ownership" do
  include ConnectH1Helpers

  it "retains real SDK propagation, immutable mTLS identity and trust through codecs and cleanup" do
    provider, exporter, bus = tracing(request_metadata: ["x-t"])
    reactor do |task|
      _, cs, = methods
      records = []
      entered = Async::Queue.new
      release = Async::Queue.new
      calls = 0
      host = nil
      observe = lambda do |phase|
        records << [phase, Async::Task.current, current_span.context.span_id, host.peer_certificate&.serial&.to_i]
        certificate = host.peer_certificate
        certificate.serial = 999 if certificate
      end
      binding = ConnectRPC::Bindings::GoogleProtobuf.new
      %i[encode decode].each do |operation|
        allow(binding).to receive(operation).and_wrap_original do |original, *args, **options|
          observe.call(operation)
          original.call(*args, **options)
        ensure
          observe.call(:codec_cleanup)
        end
      end
      handler = lambda do |reader, context|
        observe.call(:handler)
        message = reader.receive
        expect(reader.receive).to be_nil
        expect(context.request_metadata["x-t"]).to eq(["leading"])
        expect(context.deadline).to be_nil
        calls += 1
        if calls == 1
          context.send_headers(metadata: { "x-ready" => "yes" })
          entered.enqueue(true)
          release.dequeue
        end
        message
      ensure
        observe.call(:handler_cleanup)
      end
      implementation = target({ cs.path => handler }, instrumentation: bus, binding: binding)
      certificate, key = tls_material.fetch(:identity)
      context = tls_context
      context.cert = certificate
      context.key = key
      falcon(server: implementation, scheme: "https",
             tls: host_tls(anchors: [tls_material.fetch(:client_ca)])) do |active, origin|
        host = active
        application = host.instance_variable_get(:@session).instance_variable_get(:@application)
        allow(application).to receive(:call_with_publication).and_wrap_original do |original, *args, **options|
          original.call(*args, **options)
        ensure
          observe.call(:application_cleanup)
        end
        peer = ConnectH1Wire.new(origin, context: context)
        trace = "00-#{'1' * 32}-#{'2' * 16}-01"
        bytes = frame("identity")
        peer.write(request_head(cs, :chunked, extra: "traceparent: #{trace}\r\nx-t: leading\r\n") +
                   upload(bytes, :chunked) + "0\r\nx-t: trailer\r\nconnect-timeout-ms: 0\r\n" \
                                             "traceparent: 00-#{'3' * 32}-#{'4' * 16}-01\r\n\r\n")
        expect(peer.head.first).to eq(200)
        entered.dequeue
        # Revoke this client's CA while its original processor is still active.
        # Accepted connections retain their compiled trust and exact-task identity.
        host.reload_tls(**host_tls(anchors: [tls_material.fetch(:root)]))
        release.enqueue(true)
        expect(peer.body).to include("identity")
        wait_until(task, "real SDK server span exported") { rpc_spans(exporter).size == 1 }
        span = rpc_spans(exporter).first
        expect(span.hex_trace_id).to eq("1" * 32)
        expect(span.parent_span_id.unpack1("H*")).to eq("2" * 16)
        expect(span.attributes).to include("connectrpc.delivery" => "completed",
                                           "connectrpc.request.metadata.x-t" => ["leading"])
        expect(records.map(&:first)).to include(:decode, :encode, :handler, :handler_cleanup, :codec_cleanup,
                                                :application_cleanup)
        expect(records.map { |entry| entry[1] }.uniq.size).to eq(1)
        expect(records.map { |entry| entry[2] }.uniq).to eq([span.span_id])
        expect(records.map { |entry| entry[3] }.uniq).to eq([201])
        expect(host.peer_certificate).to be_nil
        expect(current_span.context).not_to be_valid
        # The consumed connection also retains that identity on its next request.
        peer.write(request_head(cs, :fixed, length: bytes.bytesize, extra: "x-t: leading\r\n") + bytes)
        expect(peer.head.first).to eq(200)
        expect(peer.body).to include("identity")
        expect(records.map { |entry| entry[3] }.uniq).to eq([201])
      ensure
        release.enqueue(true)
        peer&.close
      end
    ensure
      provider.shutdown
    end
  end

  [false, true].each do |failure|
    it "gates HTTP terminal on application cleanup, cleanup failure #{failure}" do
      provider, exporter, bus = tracing
      reactor do |task|
        ss, = methods
        entered = Async::Queue.new
        release = Async::Queue.new
        handler = ->(message, writer, _) { writer.send_message(message) }
        with_host(target({ ss.path => handler }, instrumentation: bus), false) do |host, peer, _|
          application = host.instance_variable_get(:@session).instance_variable_get(:@application)
          allow(application).to receive(:call_with_publication).and_wrap_original do |original, *args, **options|
            result = original.call(*args, **options)
            entered.enqueue([Async::Task.current, current_span.context])
            release.dequeue
            expect(current_span.context).to be_valid
            raise "application cleanup failed" if failure

            result
          end
          bytes = frame("cleanup")
          peer.write(request_head(ss, :fixed, length: bytes.bytesize) + bytes)
          expect(peer.head.first).to eq(200)
          processor, span_context = entered.dequeue
          completed = task.async do
            peer.body
          rescue IOError, SystemCallError
            :aborted
          end
          # Wait for the consumer's read to suspend while the processor cleanup
          # is held. No terminal is allowed before that ordering boundary.
          task.yield
          expect(completed).not_to be_finished
          expect(rpc_spans(exporter)).to be_empty
          release.enqueue(true)
          result = completed.wait
          expect(failure ? result == :aborted : result.include?("cleanup")).to be(true)
          wait_until(task, "SDK completion after original cleanup") { rpc_spans(exporter).size == 1 }
          span = rpc_spans(exporter).first
          expect(span.span_id).to eq(span_context.span_id)
          expect(processor).to be_finished
          expect(span.attributes["connectrpc.delivery"]).to eq(failure ? "aborted" : "completed")
        ensure
          release.enqueue(true)
        end
      ensure
        provider.shutdown
      end
    end
  end

  it "keeps completed SDK delivery after later invalid-trailer hygiene failure" do
    provider, exporter, bus = tracing
    reactor do |task|
      _, cs, = methods
      with_host(target({ cs.path => lambda { |*|
        AsyncHelpers::MESSAGE.new(name: "early")
      } }, instrumentation: bus), false) do |host, peer, _|
        peer.write(request_head(cs, :chunked))
        expect(peer.head.first).to eq(200)
        expect(peer.body).to include("early")
        wait_until(task, "SDK delivery observed before hygiene completes") { rpc_spans(exporter).size == 1 }
        span = rpc_spans(exporter).first
        expect(span.attributes["connectrpc.delivery"]).to eq("completed")
        expect(host.instance_variable_get(:@session).instance_variable_get(:@admitted).size).to eq(1)
        peer.write("0\r\ncontent-length: 9\r\n\r\n")
        wait_until(task, "hygiene retires admission") do
          host.instance_variable_get(:@session).instance_variable_get(:@admitted).empty?
        end
        expect(rpc_spans(exporter)).to eq([span])
        expect(span.attributes["connectrpc.delivery"]).to eq("completed")
        expect(span.status.code).to eq(OpenTelemetry::Trace::Status::UNSET)
      end
    ensure
      provider.shutdown
    end
  end
end
