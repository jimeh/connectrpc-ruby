# frozen_string_literal: true

require_relative "support/http1_helpers"

RSpec.describe "Connect H1 review corrections" do
  include ConnectH1Helpers

  [false, true].each do |secure|
    label = secure ? "TLS" : "TCP"

    it "releases failed SS admission before a fresh request over #{label}" do
      reactor do |task|
        ss, = methods
        finished = Async::Queue.new
        retired_admission = []
        session = nil
        owner_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
        allow(owner_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
          constructor.call(*args, **options).tap do |owner|
            allow(owner).to receive(:on_close).and_wrap_original do |register, &completion|
              register.call do
                completion.call
                retired_admission << session.instance_variable_get(:@admitted).values.dup
              end
            end
          end
        end
        handler = lambda do |message, writer, context|
          if message.name.start_with?("x")
            loop { writer.send_message(message) }
          else
            writer.send_message(message)
          end
        ensure
          finished.enqueue(context.cancelled?)
        end
        with_host(target({ ss.path => handler }), secure, max_requests: 1) do |host, peer, origin|
          bytes = frame("x" * 1000)
          peer.write(request_head(ss, :fixed, length: bytes.bytesize) + bytes)
          expect(peer.head.first).to eq(200)
          expect(peer.chunk).not_to be_empty
          session = host.instance_variable_get(:@session)
          expect(session.instance_variable_get(:@admitted).values).to eq([false])
          peer.close
          expect(finished.dequeue).to be(false)
          wait_until(task, "failed connection and original processor finished") do
            session.instance_variable_get(:@resources).all?(&:finished?)
          end
          expect(retired_admission.first).to eq([false])
          expect(session.instance_variable_get(:@admitted)).to be_empty
          expect(session.instance_variable_get(:@h1_wire)).to be_empty
          other = ConnectH1Wire.new(origin, context: secure ? tls_context : nil)
          bytes = frame("healthy")
          other.write(request_head(ss, :fixed, length: bytes.bytesize) + bytes)
          expect(other.head.first).to eq(200)
          expect(other.body).to include("healthy")
          wait_until(task, "healthy processor and delivery released admission") do
            session.instance_variable_get(:@admitted).empty?
          end
        ensure
          other&.close
        end
      end
    end

    it "closes raw TCP before parser read teardown after delivered response and #{label} EOF" do
      reactor do |task|
        _, cs, = methods
        events = []
        with_host(target({ cs.path => lambda { |*|
          AsyncHelpers::MESSAGE.new(name: "delivered")
        } }), secure) do |host, peer, _|
          peer.write(request_head(cs, :chunked))
          expect(peer.head.first).to eq(200)
          expect(peer.body).to include("delivered")
          session = host.instance_variable_get(:@session)
          resource = session.instance_variable_get(:@resources).first
          raw = resource.raw
          # Observe the lower buffered/TLS boundary, below our connection guard.
          stream = resource.connection.instance_variable_get(:@stream)
          allow(raw).to receive(:close).and_wrap_original do |close|
            events << :raw_close
            close.call
          end
          allow(stream).to receive(:close_read).and_wrap_original do |close_read|
            events << [:buffered_close_read, raw.closed?]
            close_read.call
          end
          peer.write("9\r\nx")
          secure ? peer.socket.close_write : peer.raw.shutdown(Socket::SHUT_WR)
          wait_until(task, "EOF retirement and admission release") do
            resource.finished? && session.instance_variable_get(:@admitted).empty?
          end
          expect(events).to include([:buffered_close_read, true])
          expect(events.first).to eq(:raw_close)
        end
      end
    end

    [405, 415, 505].product(%i[fixed chunked]).each do |status, framing|
      it "delivers unread-input #{status} before bounded #{label}/#{framing} hygiene" do
        reactor do |task|
          ss, _, bidi = methods
          rpc = status == 505 ? bidi : ss
          verb = status == 405 ? "PUT" : "POST"
          media = status == 415 ? "nonsense" : "application/connect+proto"
          entered = Async::Queue.new
          owner_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
          allow(owner_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
            constructor.call(*args, **options).tap do |owner|
              allow(owner).to receive(:discard).and_wrap_original do |discard|
                entered.enqueue(owner)
                discard.call
              end
            end
          end
          implementation = target({ rpc.path => ->(*) { raise "handler invoked" } })
          expect(implementation.instance_variable_get(:@binding)).not_to receive(:decode)
          with_host(implementation, secure) do |host, peer, _|
            peer.write(request_head(rpc, framing, method: verb, media: media))
            code, fields = peer.head
            expect(code).to eq(status)
            expect(fields["connection"]).to eq(["close"])
            expect(peer.body).to eq("")
            entered.dequeue
            session = host.instance_variable_get(:@session)
            expect(session.instance_variable_get(:@admitted).size).to eq(1)
            wait_until(task, "bounded preflight input retirement") do
              session.instance_variable_get(:@admitted).empty? &&
                session.instance_variable_get(:@resources).all?(&:finished?)
            end
          end
        end
      end
    end
  end

  %w[application/connect+proto application/connect+json].each do |media|
    it "preserves safe unary GET query controls despite irrelevant #{media} POST fields" do
      reactor do
        query = URI.encode_www_form(connect: "v1", encoding: "json", message: '{"name":"get-ok"}')
        falcon do |_host, origin|
          headers = { "content-type" => media, "connect-protocol-version" => "bogus", "content-encoding" => "bogus" }
          status, _, bytes = http1(origin, "#{AsyncHelpers::ECHO::ECHO.path}?#{query}", method: "GET", body: nil,
                                                                                        headers: headers)
          expect(status).to eq(200)
          expect(JSON.parse(bytes)).to eq("name" => "get-ok")
        end
      end
    end
  end

  %w[HTTP/1.0 HTTP/1.1].product(%w[GET POST],
                                %w[application/json application/connect+proto
                                   application/connect+json]).each do |version, verb, media|
    it "preserves portable SS/CS status for #{version} #{verb} #{media}" do
      reactor do
        methods.first(2).each do |rpc|
          with_host(target({ rpc.path => ->(*) { raise "handler invoked" } }), false) do |_host, peer, _|
            head = request_head(rpc, :fixed, length: 0, method: verb, media: media).sub("HTTP/1.1", version)
            peer.write(head)
            expected = if version == "HTTP/1.0" && verb == "POST" && media.start_with?("application/connect+")
                         505
                       elsif verb == "GET"
                         405
                       elsif media == "application/json"
                         501
                       else
                         200
                       end
            expect(peer.head.first).to eq(expected)
          end
        end
      end
    end
  end
end
