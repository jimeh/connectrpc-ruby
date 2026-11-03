# frozen_string_literal: true

require "stringio"

RSpec.describe "Native predispatch cancellation boundary" do
  %i[grpc connect].each do |protocol|
    kinds = [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod]
    kinds.unshift(ConnectRPC::Method) if protocol == :grpc
    kinds.each do |kind|
      %i[success canceled deadline_exceeded].each do |reason|
        it "checks #{protocol} #{kind} Context before a no-I/O handler for #{reason}" do
          binding = Object.new
          binding.define_singleton_method(:encode) { |value, **| value }
          binding.define_singleton_method(:decode) { |value, **| value }
          binding.define_singleton_method(:encode_status) { |_| "" }
          binding.define_singleton_method(:decode_status) { |*, **| ConnectRPC::Error.new(:unknown) }
          method = kind.new(service: "test.Dispatch", name: "Call", request_type: String, response_type: String)
          invocations = []
          handler = lambda { |*args|
            invocations << args.last
            "reply"
          }
          observer = Object.new
          outcomes = []
          observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
          subscriber = Object.new
          subscriber.define_singleton_method(:start) { |_| observer }
          source = ConnectRPC::CancellationSource.new
          source.cancel if reason == :canceled
          target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
                                          binding: binding, protocol: protocol,
                                          instrumentation: ConnectRPC::Instrumentation.new(subscribers: [subscriber]),
                                          handlers: kind == ConnectRPC::Method ? {} : { method.path => handler },
                                          invoker: kind == ConnectRPC::Method ? handler : nil)
          headers = { "content-type" => [protocol == :grpc ? "application/grpc" : "application/connect+proto"] }
          headers["grpc-timeout"] = ["0n"] if protocol == :grpc && reason == :deadline_exceeded
          # Connect requires a positive wire timeout. Expire its actual Context
          # clock before dispatch without yielding or relying on elapsed time.
          if protocol == :connect && reason == :deadline_exceeded
            clock = 0.0
            expired = ConnectRPC::Deadline.new(1, clock: -> { clock })
            allow(target).to receive(:request_deadline) {
              clock = 2.0
              expired
            }
          end
          input = StringIO.new("#{[0, 1].pack('CN')}q")
          input.define_singleton_method(:read) { |n, **| super(n) }
          bytes = StringIO.new
          terminal = nil
          bytes.define_singleton_method(:write) do |value, **|
            super(value)
            nil
          end
          bytes.define_singleton_method(:finish) do |headers:|
            terminal = headers
            nil
          end
          bytes.define_singleton_method(:abort) { nil }
          target.public_send(protocol == :grpc ? :call_grpc : :call_stream,
                             path: method.path, http_method: "POST", headers: headers, input: input,
                             output: bytes, publish: ->(_) {}, http2: true, cancellation: source.token)
          expect(invocations.size).to eq(reason == :success ? 1 : 0)
          outcome = outcomes.fetch(0)
          expect(outcome.code).to eq(reason == :success ? "OK" : reason.to_s)
          if protocol == :grpc
            expect(terminal["grpc-status"]).to eq([{ success: "0", canceled: "1",
                                                     deadline_exceeded: "4" }.fetch(reason)])
          elsif reason != :success
            expect(JSON.parse(bytes.string.byteslice(5..)).dig("error", "code")).to eq(reason.to_s)
          end
          expect(outcome.normalized_error&.cause).to be_nil
          expect(source.token.reason).to eq(reason == :canceled ? :canceled : nil)
          if reason == :success
            expect(invocations.first).to be_a(kind == ConnectRPC::Method ? ConnectRPC::Context : ConnectRPC::StreamingContext)
          end
        end
      end
    end
  end
end
