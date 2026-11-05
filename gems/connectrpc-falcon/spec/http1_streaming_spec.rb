# frozen_string_literal: true

require_relative "support/http1_helpers"

RSpec.describe "Falcon Connect HTTP/1.1 streaming" do
  include ConnectH1Helpers

  [false, true].product(%i[fixed chunked]).each do |secure, framing|
    label = "#{secure ? 'TLS' : 'TCP'}/#{framing}"
    it "delivers SS incrementally before handler exit and reuses consumed input on #{label}" do
      reactor do
        ss, = methods
        release = Async::Queue.new
        handler = lambda do |message, writer, _|
          writer.send_message(message)
          release.dequeue
          writer.send_message(message)
        end
        with_host(target({ ss.path => handler }), secure) do |_host, peer, _|
          bytes = frame("incremental")
          2.times do
            peer.write(request_head(ss, framing, length: bytes.bytesize) + upload(bytes, framing, finish: true))
            expect(peer.head.first).to eq(200)
            received = +"".b
            received << peer.chunk while received.bytesize < bytes.bytesize
            expect(received).to eq(bytes)
            release.enqueue(true)
            expect(peer.body).to eq("#{bytes}#{[2, 15].pack('CN')}{\"metadata\":{}}")
          end
        end
      ensure
        release.enqueue(true)
      end
    end

    it "publishes headers during CS input reads, receives before upload ends on #{label}" do
      reactor do
        _, cs, = methods
        received = Async::Queue.new
        handler = lambda do |reader, context|
          context.send_headers(metadata: { "x-before-input" => "yes" })
          first = reader.receive
          received.enqueue(first.name)
          expect(reader.receive.name).to eq("last")
          expect(reader.receive).to be_nil
          first
        end
        with_host(target({ cs.path => handler }), secure) do |_host, peer, _|
          bytes = frame("before-upload-completion")
          last = frame("last")
          peer.write(request_head(cs, framing, length: bytes.bytesize + last.bytesize))
          status, headers = peer.head
          expect(status).to eq(200)
          expect(headers["x-before-input"]).to eq(["yes"])
          expect(headers["connection"]).to eq(["close"])
          peer.write(upload(bytes, framing))
          expect(received.dequeue).to eq("before-upload-completion")
          peer.write(upload(last, framing, finish: true))
          expect(peer.body).to eq("#{bytes}#{[2, 15].pack('CN')}{\"metadata\":{}}")
        end
      end
    end

    [false, true].each do |error|
      it "delivers complete early #{error ? 'error' : 'success'} before unread #{label} hygiene ends" do
        reactor do |task|
          _, cs, = methods
          retired = {}
          owners = []
          hygiene = Async::Queue.new
          owner = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
          allow(owner).to receive(:new).and_wrap_original do |constructor, *args, **options|
            constructor.call(*args, **options).tap do |instance|
              owners << instance
              allow(instance).to receive(:discard).and_wrap_original do |discard|
                hygiene.enqueue(true)
                discard.call
              end
              allow(instance).to receive(:close).and_wrap_original do |close, *values|
                close.call(*values)
              ensure
                retired[instance] = true
              end
            end
          end
          handler = lambda do |_reader, _context|
            raise ConnectRPC::Error.new(:invalid_argument, "early") if error

            AsyncHelpers::MESSAGE.new(name: "early")
          end
          with_host(target({ cs.path => handler }), secure, max_requests: 1) do |host, peer, origin|
            peer.write(request_head(cs, framing))
            status, headers = peer.head
            expect(status).to eq(200)
            expect(headers["connection"]).to eq(["close"])
            bytes = peer.body
            expect(bytes).to include(error ? '"code":"invalid_argument"' : "early")
            hygiene.dequeue
            expect(retired.fetch(owners.first, false)).to be(false)
            expect(host.instance_variable_get(:@session).instance_variable_get(:@admitted).size).to eq(1)
            # Unrelated connection can make progress while this one retires.
            other = ConnectH1Wire.new(origin, context: secure ? tls_context : nil)
            other.write(request_head(cs, :fixed, length: 0))
            expect(other.head.first).to eq(503)
            expect(retired.fetch(owners.first, false)).to be(false)
            wait_until(task, "bounded retirement") { retired[owners.first] }
            expect(host.instance_variable_get(:@session).instance_variable_get(:@admitted)).to be_empty
          ensure
            other&.close
          end
        end
      end
    end
  end

  it "returns one bare bidi 505/close before native protocol, method or codec validation" do
    reactor do
      _, _, bidi = methods
      implementation = target({ bidi.path => ->(*) { raise "handler invoked" } }, protocol: :grpc)
      binding = implementation.instance_variable_get(:@binding)
      expect(binding).not_to receive(:decode)
      expect(binding).not_to receive(:encode)
      %w[application/connect+proto application/grpc nonsense].product(%w[GET POST]).each do |media, verb|
        with_host(implementation, false) do |_host, peer, _|
          peer.write(request_head(bidi, :fixed, length: 0, media: media, method: verb))
          status, fields = peer.head
          expect(status).to eq(505)
          expect(fields["connection"]).to eq(["close"])
          expect(peer.body).to eq("")
        end
      end
    end
  end

  it "ignores real chunked trailers in CS metadata and deadlines" do
    reactor do
      _, cs, = methods
      seen = nil
      handler = lambda do |reader, context|
        message = reader.receive
        expect(reader.receive).to be_nil
        seen = [context.request_metadata.to_h, context.deadline]
        message
      end
      with_host(target({ cs.path => handler }), false) do |_host, peer, _|
        bytes = frame("trailer-isolation")
        head = request_head(cs, :chunked, extra: "x-t: leading\r\n")
        trailers = "0\r\nx-t: trailer\r\nconnect-timeout-ms: 0\r\n\r\n"
        peer.write("#{head}#{upload(bytes, :chunked)}#{trailers}")
        expect(peer.head.first).to eq(200)
        expect(peer.body).to include("trailer-isolation")
        expect(seen).to eq([{ "x-t" => ["leading"] }, nil])
      end
    end
  end
end
