# frozen_string_literal: true

require "socket"
require "stringio"

RSpec.describe "Unary interceptors" do
  let(:rpc) do
    ConnectRPC::Method.new(service: "intercept.Test", name: "Call", request_type: String, response_type: String,
                           idempotency: :no_side_effects)
  end
  let(:service) { ConnectRPC::Service.new(name: rpc.service, methods: [rpc]) }
  let(:binding) do
    Class.new do
      def encode(message, type:, codec:)
        raise ConnectRPC::CodecError unless message.is_a?(type) && codec == :proto

        "wire:#{message}"
      end

      def decode(bytes, type:, codec:)
        raise ConnectRPC::CodecError unless type == String && codec == :proto && bytes.start_with?("wire:")

        bytes.delete_prefix("wire:")
      end
    end.new
  end
  let(:outcomes) { [] }
  let(:completions) { [] }
  let(:sent) { [] }
  let(:bus) do
    outcomes = self.outcomes
    completions = self.completions
    sent = self.sent
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |call|
      call.carrier["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" if call.role == :client
      Object.new.tap do |observer|
        observer.define_singleton_method(:attach) { Thread.current[:interceptor_spec_rpc] = call }
        observer.define_singleton_method(:detach) { |_| Thread.current[:interceptor_spec_rpc] = nil }
        observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
        observer.define_singleton_method(:finished) { |completion| completions << completion }
        observer.define_singleton_method(:message_sent) { |bytes| sent << bytes }
      end
    end
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end
  let(:dispatches) { [] }

  # Bodies run with the example as self; the continuation arrives as a block.
  def layer(role, &body)
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(role) { |call, &continuation| body.call(call, &continuation) }
    end
  end

  def only(list)
    expect(list.size).to eq(1)
    list.first
  end

  def reply(message = "hello", headers = {})
    ConnectRPC::TransportResponse.new(
      status: 200, body: "wire:#{message}",
      headers: { "content-type" => ["application/proto"], "x-reply" => ["leading"], "trailer-x-end" => ["tail"] }
               .merge(headers)
    )
  end

  # Records every dispatch; the default answers with a fixed reply.
  def recording(&answer)
    answer ||= ->(_request) { reply }
    dispatches = self.dispatches
    ->(request) { dispatches << request and answer.call(request) }
  end

  def connect_client(interceptors, transport: recording, instrumentation: bus, **)
    ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport,
                           instrumentation: instrumentation, interceptors: interceptors, **)
  end

  def server(interceptors: [], handlers: { rpc.path => ->(request, _context) { "re:#{request}" } }, **)
    ConnectRPC::Server.new(services: [service], binding: binding, handlers: handlers, instrumentation: bus,
                           interceptors: interceptors, **)
  end

  # A Connect transport that serves requests in process through a Server.
  def in_process(target)
    lambda do |request|
      target.call(path: request.url.path, http_method: request.http_method, headers: request.headers,
                  input: StringIO.new(request.body), query: request.url.query.to_s)
    end
  end

  def expect_frozen(response)
    expect(response).to be_frozen
    expect(response.metadata).to be_frozen
    expect(response.trailers).to be_frozen
    expect { response.metadata = ConnectRPC::Metadata.new }.to raise_error(FrozenError)
    expect { response.metadata["x-late"] = "value" }.to raise_error(FrozenError)
    expect { response.trailers["x-late"] = "value" }.to raise_error(FrozenError)
  end

  def cached(message = "cached")
    ConnectRPC::Response.new(message: message, metadata: ConnectRPC::Metadata.new("x-cache" => "hit"),
                             trailers: ConnectRPC::Metadata.new)
  end

  def wait_until(seconds = 5)
    limit = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    until yield
      raise "Condition not observed within #{seconds}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > limit

      Thread.pass
    end
  end

  describe "construction" do
    it "rejects an element without the method for the role on both roles" do
      expect { connect_client([Object.new]) }.to raise_error(ArgumentError, /client_unary/)
      expect { connect_client([layer(:server_unary) { |_call, &continuation| continuation.call }]) }
        .to raise_error(ArgumentError, /client_unary/)
      expect { server(interceptors: [Object.new]) }.to raise_error(ArgumentError, /server_unary/)
      expect { server(interceptors: [layer(:client_unary) { |_call, &continuation| continuation.call }]) }
        .to raise_error(ArgumentError, /server_unary/)
    end

    it "rejects stream-only and mixed unary and stream interceptors until stream chains exist" do
      pass = ->(_call, &continuation) { continuation.call }
      client_mixed = layer(:client_unary, &pass).tap { |object| object.define_singleton_method(:client_stream, &pass) }
      server_mixed = layer(:server_unary, &pass).tap { |object| object.define_singleton_method(:server_stream, &pass) }
      expect { connect_client([layer(:client_stream, &pass)]) }.to raise_error(ArgumentError, /not supported yet/)
      expect { connect_client([client_mixed]) }.to raise_error(ArgumentError, /not supported yet/)
      expect { server(interceptors: [layer(:server_stream, &pass)]) }.to raise_error(ArgumentError, /not supported yet/)
      expect { server(interceptors: [server_mixed]) }.to raise_error(ArgumentError, /not supported yet/)
    end

    it "requires an Array" do
      expect { connect_client(nil) }.to raise_error(ArgumentError, /Array/)
      expect { server(interceptors: {}) }.to raise_error(ArgumentError, /Array/)
    end
  end

  describe "ordering" do
    it "runs the first interceptor outermost on both roles, around dispatch and the handler" do
      log = []
      tracing = lambda do |role, name|
        layer(role) do |_call, &continuation|
          log << "#{name}>"
          result = continuation.call
          log << "<#{name}"
          result
        end
      end
      handler = lambda do |request, _context|
        log << "handler"
        "re:#{request}"
      end
      target = server(interceptors: %w[s1 s2 s3].map { |name| tracing.call(:server_unary, name) },
                      handlers: { rpc.path => handler })
      client = connect_client(%w[c1 c2 c3].map { |name| tracing.call(:client_unary, name) },
                              transport: in_process(target), instrumentation: nil)
      expect(client.call(rpc, "hi").message).to eq("re:hi")
      expect(log).to eq(%w[c1> c2> c3> s1> s2> s3> handler <s3 <s2 <s1 <c3 <c2 <c1])
    end
  end

  describe "client Connect unary" do
    it "freezes responses without interceptors" do
      expect_frozen(connect_client([]).call(rpc, "hi"))
    end

    it "sends metadata from every layer and the replaced request, then closes the call" do
      seen = []
      outer = layer(:client_unary) do |call, &continuation|
        call.request_metadata["x-outer"] = "one"
        continuation.call
      end
      inner = layer(:client_unary) do |call, &continuation|
        seen << call.request_metadata["x-outer"] << call.protocol << call.method
        call.request_metadata["x-inner"] = "two"
        call.request = "replaced"
        response = continuation.call
        seen << (begin
          call.request = "late"
        rescue ConnectRPC::InterceptorStateError => e
          e
        end)
        expect { call.request_metadata["x-late"] = "three" }.to raise_error(FrozenError)
        response
      end
      expect(connect_client([outer, inner]).call(rpc, "hi", metadata: { "x-caller" => "zero" }).message)
        .to eq("hello")
      expect(seen.first(3)).to eq([["one"], :connect, rpc])
      expect(seen.last).to be_a(ConnectRPC::InterceptorStateError)
      request = only(dispatches)
      expect(request.body).to eq("wire:replaced")
      expect(request.headers).to include("x-caller" => ["zero"], "x-outer" => ["one"], "x-inner" => ["two"])
      expect(only(outcomes).request_metadata).to include("x-caller" => ["zero"], "x-inner" => ["two"],
                                                         "traceparent" => [a_string_starting_with("00-")])
    end

    it "keeps the deadline and cancellation view that dispatch observes" do
      source = ConnectRPC::CancellationSource.new
      seen = nil
      probe = layer(:client_unary) do |call, &continuation|
        seen = [call.deadline, call.cancellation, call.cancellation.cancelled?]
        continuation.call
      end
      connect_client([probe]).call(rpc, "hi", timeout: 5, cancellation: source.token)
      deadline, cancellation, cancelled = seen
      expect(only(dispatches).deadline).to equal(deadline)
      expect(only(dispatches).cancellation).to equal(cancellation)
      expect(cancellation).not_to respond_to(:commit!)
      expect(cancelled).to be(false)
      expect(connect_client([probe]).call(rpc, "hi", timeout: nil).message).to eq("hello")
      expect(seen.first).to be_nil
    end

    it "short-circuits with a frozen Response, no dispatch and caller metadata only" do
      short = layer(:client_unary) do |call, &_continuation|
        call.request_metadata["x-added"] = "yes"
        cached
      end
      response = connect_client([short]).call(rpc, "hi", metadata: { "x-caller" => "zero" })
      expect(response.message).to eq("cached")
      expect_frozen(response)
      expect(dispatches).to be_empty
      expect(sent).to be_empty
      outcome = only(outcomes)
      expect(outcome.code).to eq("OK")
      expect(outcome.http_status).to be_nil
      expect(outcome.request_metadata).to eq("x-caller" => ["zero"])
      expect(only(completions).delivery).to eq(:unobserved)
    end

    it "hands outer layers an inner short-circuit or replacement already frozen" do
      observed = []
      outer = layer(:client_unary) do |_call, &continuation|
        response = continuation.call
        observed << [response.frozen?, response.metadata.frozen?, response.trailers.frozen?]
        response
      end
      [
        layer(:client_unary) { |_call, &_continuation| cached },
        layer(:client_unary) { |_call, &continuation| cached("replacement #{continuation.call.message}") }
      ].each { |inner| connect_client([outer, inner], instrumentation: nil).call(rpc, "hi") }
      expect(observed).to eq([[true, true, true], [true, true, true]])
      expect(dispatches.size).to eq(1)
    end

    it "rejects a layer result that is not a complete Response" do
      [
        "message",
        ConnectRPC::Response.new(message: "partial"),
        ConnectRPC::Response.new(message: "partial", metadata: {}, trailers: ConnectRPC::Metadata.new)
      ].each do |value|
        before = layer(:client_unary) { |_call, &_continuation| value }
        after = layer(:client_unary) do |_call, &continuation|
          continuation.call
          value
        end
        expect { connect_client([before]).call(rpc, "hi") }.to raise_error(ConnectRPC::InterceptorStateError)
        expect { connect_client([after]).call(rpc, "hi") }.to raise_error(ConnectRPC::InterceptorStateError)
      end
      expect(dispatches.size).to eq(3)
      expect(completions.map(&:delivery).uniq).to eq([:unobserved])
    end

    it "fails with an interceptor rejection raised before yield, without dispatch" do
      rejection = ConnectRPC::Error.new(:permission_denied, "Denied")
      reject = layer(:client_unary) { |_call, &_continuation| raise rejection }
      expect { connect_client([reject]).call(rpc, "hi") }.to(raise_error { |error| expect(error).to equal(rejection) })
      expect(dispatches).to be_empty
      expect(only(outcomes)).to have_attributes(code: "permission_denied", error: rejection, http_status: nil)
      expect(only(completions).delivery).to eq(:unobserved)
    end

    it "fails with an exception raised after a successful continuation" do
      failure = RuntimeError.new("after")
      raising = layer(:client_unary) do |_call, &continuation|
        continuation.call
        raise failure
      end
      expect { connect_client([raising]).call(rpc, "hi") }.to(raise_error { |error| expect(error).to equal(failure) })
      expect(dispatches.size).to eq(1)
      expect(only(outcomes)).to have_attributes(code: nil, error: failure, http_status: 200)
    end

    describe "rule 6" do
      def swallowing(seen)
        layer(:client_unary) do |_call, &continuation|
          continuation.call
        rescue StandardError => e
          seen << e
          cached("fallback")
        end
      end

      it "re-raises a decode failure's Error after the interceptor returns a Response" do
        seen = []
        transport = recording do |_request|
          ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                            body: "not-wire")
        end
        expect { connect_client([swallowing(seen)], transport: transport).call(rpc, "hi") }
          .to(raise_error { |error| expect(error).to equal(only(seen)) })
        expect(only(seen)).to have_attributes(code: "internal", message: "Cannot decode response")
        expect(only(outcomes).error).to be_a(ConnectRPC::CodecError)
        expect(only(outcomes).code).to eq("internal")
      end

      it "re-raises a NetHTTP connection failure after the interceptor returns a Response" do
        listener = TCPServer.new("127.0.0.1", 0)
        port = listener.addr[1]
        listener.close
        seen = []
        client = ConnectRPC::Client.new(base_url: "http://127.0.0.1:#{port}", binding: binding, instrumentation: bus,
                                        interceptors: [swallowing(seen)])
        expect { client.call(rpc, "hi", timeout: 5) }.to(raise_error { |error| expect(error).to equal(only(seen)) })
        expect(only(seen).code).to eq("unavailable")
        expect(only(outcomes).error).to be_a(SystemCallError)
        expect(only(outcomes).code).to eq("unavailable")
      end

      it "fails with an inner translation, not the original, when the outer layer returns" do
        original = RuntimeError.new("transport bug")
        translation = ConnectRPC::Error.new(:unavailable, "Translated")
        outer = layer(:client_unary) do |_call, &continuation|
          continuation.call
        rescue ConnectRPC::Error
          cached("fallback")
        end
        inner = layer(:client_unary) do |_call, &continuation|
          continuation.call
        rescue RuntimeError
          raise translation
        end
        transport = recording { |_request| raise original }
        expect { connect_client([outer, inner], transport: transport).call(rpc, "hi") }
          .to(raise_error { |error| expect(error).to equal(translation) })
        expect(only(outcomes)).to have_attributes(error: original, code: "unavailable")
      end
    end

    describe "continuation misuse" do
      it "rejects a second yield and re-raises one the interceptor swallowed" do
        twice = layer(:client_unary) do |_call, &continuation|
          continuation.call
          continuation.call
        end
        swallowed = nil
        swallowing = layer(:client_unary) do |_call, &continuation|
          response = continuation.call
          begin
            continuation.call
          rescue ConnectRPC::InterceptorStateError => e
            swallowed = e
          end
          response
        end
        expect { connect_client([twice]).call(rpc, "hi") }
          .to raise_error(ConnectRPC::InterceptorStateError, /more than once/)
        expect { connect_client([swallowing]).call(rpc, "hi") }
          .to(raise_error { |error| expect(error).to equal(swallowed) })
        expect(dispatches.size).to eq(2)
      end

      it "rejects a captured continuation after its interceptor returned without changing the result" do
        captured = nil
        capturing = layer(:client_unary) do |_call, &continuation|
          captured = continuation
          continuation.call
        end
        response = connect_client([capturing]).call(rpc, "hi")
        expect { captured.call }.to raise_error(ConnectRPC::InterceptorStateError, /after its interceptor returned/)
        expect(response.message).to eq("hello")
        expect(dispatches.size).to eq(1)
        expect(only(outcomes).code).to eq("OK")
      end

      it "rejects a continuation from another thread and fails the call even if the layer then yields" do
        foreign = nil
        threaded = layer(:client_unary) do |_call, &continuation|
          foreign = Thread.new do
            continuation.call
          rescue ConnectRPC::InterceptorStateError => e
            e
          end.value
          continuation.call
        end
        expect { connect_client([threaded]).call(rpc, "hi") }
          .to(raise_error { |error| expect(error).to equal(foreign) })
        expect(foreign.message).to match(/another fiber or thread/)
        expect(dispatches.size).to eq(1)
      end
    end

    describe "cancellation and deadlines" do
      it "cancels before dispatch when the source is cancelled inside an interceptor before yield" do
        source = ConnectRPC::CancellationSource.new
        cancelling = layer(:client_unary) do |_call, &continuation|
          source.cancel
          continuation.call
        end
        expect { connect_client([cancelling]).call(rpc, "hi", cancellation: source.token) }
          .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
        expect(dispatches).to be_empty
        expect(only(outcomes).code).to eq("canceled")
      end

      it "fails deadline_exceeded when the deadline expires during post-processing" do
        waiting = layer(:client_unary) do |call, &continuation|
          response = continuation.call
          wait_until { call.deadline.expired? }
          response
        end
        expect { connect_client([waiting]).call(rpc, "hi", timeout: 0.05) }
          .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
        expect(dispatches.size).to eq(1)
        expect(only(outcomes).code).to eq("deadline_exceeded")
      end

      it "raises the accepted reason for a short-circuit returned after cancellation was accepted" do
        source = ConnectRPC::CancellationSource.new
        short = layer(:client_unary) do |call, &_continuation|
          source.cancel
          call.cancellation.cancelled?
          cached
        end
        expect { connect_client([short]).call(rpc, "hi", cancellation: source.token) }
          .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
        expect(only(completions).delivery).to eq(:unobserved)
      end

      describe "failure provenance under an accepted cancellation" do
        let(:source) { ConnectRPC::CancellationSource.new }
        let(:failure) { ConnectRPC::Error.new(:unavailable, "Transport failed") }
        let(:transport) do
          recording do |request|
            source.cancel
            request.cancellation.cancelled?
            raise failure
          end
        end

        def run(interceptor)
          connect_client([interceptor], transport: transport).call(rpc, "hi", cancellation: source.token)
        end

        {
          "a pass-through layer" => ->(_call, &continuation) { continuation.call },
          "a layer that re-raises by identity" => lambda do |_call, &continuation|
            continuation.call
          rescue StandardError => e # rubocop:disable Lint/UselessRescue -- re-raises by identity on purpose
            raise e
          end
        }.each do |label, body|
          it "replaces a core-owned failure with canceled through #{label}" do
            expect { run(layer(:client_unary, &body)) }
              .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
            expect(only(outcomes)).to have_attributes(code: "canceled", error: failure)
          end
        end

        it "keeps a translation by identity, with its own code and backtrace" do
          translation = ConnectRPC::Error.new(:resource_exhausted, "Translated")
          translating = layer(:client_unary) do |_call, &continuation|
            continuation.call
          rescue ConnectRPC::Error
            raise translation
          end
          expect { run(translating) }.to(raise_error { |error| expect(error).to equal(translation) })
          expect(translation.backtrace.first).to include(__FILE__)
          expect(only(outcomes)).to have_attributes(code: "resource_exhausted", error: failure,
                                                    normalized_error: translation)
        end

        it "keeps an exception the interceptor raises after an accepted cancellation" do
          own = ConnectRPC::Error.new(:aborted, "Interceptor")
          raising = layer(:client_unary) do |call, &continuation|
            response = continuation.call
            source.cancel
            raise own if call.cancellation.cancelled?

            response
          end
          expect do
            connect_client([raising]).call(rpc, "hi", cancellation: source.token)
          end.to(raise_error { |error| expect(error).to equal(own) })
          expect(dispatches.size).to eq(1)
          expect(only(outcomes)).to have_attributes(code: "aborted", error: own, normalized_error: own)
        end
      end
    end

    it "runs interceptors inside the instrumentation activation and processing duration" do
      interval = nil
      active = nil
      timing = layer(:client_unary) do |_call, &continuation|
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        active = Thread.current[:interceptor_spec_rpc]
        response = continuation.call
        interval = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        response
      end
      connect_client([timing]).call(rpc, "hi")
      expect(active&.role).to eq(:client)
      expect(only(outcomes).duration).to be >= interval
    end
  end

  describe "client native gRPC unary" do
    let(:status_codec) do
      require "connectrpc/bindings/google_protobuf"
      ConnectRPC::Bindings::GoogleProtobuf.new
    end
    let(:opened) { [] }

    def envelope(bytes) = [0, bytes.bytesize].pack("CN") + bytes

    # Fields arrive in trailers after one response message, or trailers-only.
    def grpc_client(interceptors, status: "0", message: "hello", trailers_only: false, before_open: nil)
      source = StringIO.new(trailers_only ? "" : envelope("wire:#{message}"))
      fields = { "grpc-status" => [status], "x-end" => ["tail"] }
      fields["grpc-message"] = ["Remote"] unless status == "0"
      head = ConnectRPC::GRPCHead.new(status: 200, end_stream: trailers_only,
                                      headers: { "content-type" => ["application/grpc"], "x-reply" => ["leading"] }
                                                .merge(trailers_only ? fields : {}))
      exchange = Object.new
      exchange.define_singleton_method(:response_head) { |**| head }
      exchange.define_singleton_method(:response_trailers) { fields }
      exchange.define_singleton_method(:read) { |length, **| source.read([length, 64].min) }
      exchange.define_singleton_method(:write) { |_| nil }
      exchange.define_singleton_method(:close_send) { nil }
      exchange.define_singleton_method(:finish) { |**| nil }
      exchange.define_singleton_method(:abort) { nil }
      transport = Object.new
      opened = self.opened
      transport.define_singleton_method(:grpc_capabilities) { %i[unary] }
      transport.define_singleton_method(:open_grpc_stream) do |request, &work|
        opened << request
        before_open&.call(request)
        work.call(exchange)
      end
      ConnectRPC::Client.new(base_url: "https://example.invalid", protocol: :grpc, binding: binding,
                             status_codec: status_codec, transport: transport, instrumentation: bus,
                             interceptors: interceptors)
    end

    it "freezes responses without interceptors" do
      response = grpc_client([]).call(rpc, "hi")
      expect(response.message).to eq("hello")
      expect(response.trailers["x-end"]).to eq(["tail"])
      expect_frozen(response)
    end

    it "sends layer metadata and the replaced request, with the scope result from the chain" do
      body = nil
      binding.define_singleton_method(:encode) { |message, **| body = "wire:#{message}" }
      adding = layer(:client_unary) do |call, &continuation|
        call.request_metadata["x-layer"] = call.protocol.to_s
        call.request = "replaced"
        continuation.call
      end
      response = grpc_client([adding]).call(rpc, "hi", metadata: { "x-caller" => "zero" })
      expect(response.message).to eq("hello")
      expect_frozen(response)
      expect(body).to eq("wire:replaced")
      expect(only(opened).headers).to include("x-layer" => ["grpc"], "x-caller" => ["zero"])
      expect(only(outcomes).request_metadata).to include("x-layer" => ["grpc"], "traceparent" => [anything])
      expect(only(completions).delivery).to eq(:completed)
    end

    it "short-circuits with a frozen Response, no stream and :unobserved delivery" do
      response = grpc_client([layer(:client_unary) { |_call, &_continuation| cached }])
                 .call(rpc, "hi", metadata: { "x-caller" => "zero" })
      expect(response.message).to eq("cached")
      expect_frozen(response)
      expect(opened).to be_empty
      expect(only(outcomes)).to have_attributes(code: "OK", request_metadata: { "x-caller" => ["zero"] })
      expect(only(completions).delivery).to eq(:unobserved)
    end

    it "reports :unobserved for a rejection raised before yield" do
      rejection = ConnectRPC::Error.new(:unauthenticated, "No token")
      expect { grpc_client([layer(:client_unary) { |_call, &_continuation| raise rejection }]).call(rpc, "hi") }
        .to(raise_error { |error| expect(error).to equal(rejection) })
      expect(opened).to be_empty
      expect(only(outcomes).code).to eq("unauthenticated")
      expect(only(completions).delivery).to eq(:unobserved)
    end

    it "returns a replacement after a successful continuation" do
      replacing = layer(:client_unary) { |_call, &continuation| cached("re:#{continuation.call.message}") }
      response = grpc_client([replacing]).call(rpc, "hi")
      expect(response.message).to eq("re:hello")
      expect_frozen(response)
      expect(only(outcomes).code).to eq("OK")
      expect(only(completions).delivery).to eq(:completed)
    end

    {
      "an exception raised after yield" => [
        lambda { |failure|
          [->(_call, &continuation) { continuation.call && raise(failure) }]
        }, RuntimeError
      ],
      "a replacement followed by an outer raise" => [
        lambda { |failure|
          [->(_call, &continuation) { continuation.call && raise(failure) },
           lambda { |_call, &continuation|
             ConnectRPC::Response.new(message: "re:#{continuation.call.message}",
                                      metadata: ConnectRPC::Metadata.new, trailers: ConnectRPC::Metadata.new)
           }]
        }, RuntimeError
      ],
      "an invalid replacement" => [
        ->(_failure) { [->(_call, &continuation) { continuation.call && "invalid" }] },
        ConnectRPC::InterceptorStateError
      ]
    }.each do |label, (bodies, kind)|
      it "reports a completed exchange for #{label}" do
        failure = RuntimeError.new("interceptor")
        interceptors = bodies.call(failure).map { |body| layer(:client_unary, &body) }
        expect { grpc_client(interceptors).call(rpc, "hi") }.to raise_error(kind)
        expect(opened.size).to eq(1)
        expect(only(outcomes).code).to be_nil
        expect(only(completions).delivery).to eq(:completed)
      end
    end

    it "re-raises a remote error after the interceptor returns a Response" do
      seen = []
      swallowing = layer(:client_unary) do |_call, &continuation|
        continuation.call
      rescue ConnectRPC::Error => e
        seen << e
        cached("fallback")
      end
      expect { grpc_client([swallowing], status: "5", trailers_only: true).call(rpc, "hi") }
        .to(raise_error { |error| expect(error).to equal(only(seen)) })
      expect(only(seen).code).to eq("not_found")
      expect(only(outcomes)).to have_attributes(code: "not_found", error: only(seen))
      expect(only(completions).delivery).to eq(:completed)
    end

    it "reports a translated remote error as delivered with the translation's code" do
      translation = ConnectRPC::Error.new(:failed_precondition, "Translated")
      translating = layer(:client_unary) do |_call, &continuation|
        continuation.call
      rescue ConnectRPC::Error
        raise translation
      end
      expect { grpc_client([translating], status: "5", trailers_only: true).call(rpc, "hi") }
        .to(raise_error { |error| expect(error).to equal(translation) })
      expect(only(outcomes).code).to eq("failed_precondition")
      expect(only(outcomes).error.code).to eq("not_found")
      expect(only(completions).delivery).to eq(:completed)
    end

    it "cancels before opening a stream when cancelled inside an interceptor before yield" do
      source = ConnectRPC::CancellationSource.new
      cancelling = layer(:client_unary) do |_call, &continuation|
        source.cancel
        continuation.call
      end
      expect { grpc_client([cancelling]).call(rpc, "hi", cancellation: source.token) }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
      expect(opened).to be_empty
    end

    it "raises the accepted reason for a short-circuit returned after cancellation was accepted" do
      source = ConnectRPC::CancellationSource.new
      short = layer(:client_unary) do |call, &_continuation|
        source.cancel
        call.cancellation.cancelled?
        cached
      end
      expect { grpc_client([short]).call(rpc, "hi", cancellation: source.token) }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
      expect(only(outcomes).code).to eq("canceled")
      expect(only(completions).delivery).to eq(:unobserved)
    end

    describe "failure provenance under an accepted cancellation" do
      let(:source) { ConnectRPC::CancellationSource.new }
      let(:failure) { ConnectRPC::Error.new(:unavailable, "Open failed") }

      def run(interceptor)
        source = self.source
        failure = self.failure
        cancel_then_fail = lambda do |request|
          source.cancel
          request.cancellation.cancelled?
          raise failure
        end
        grpc_client([interceptor], before_open: cancel_then_fail).call(rpc, "hi", cancellation: source.token)
      end

      {
        "a pass-through layer" => ->(_call, &continuation) { continuation.call },
        "a layer that re-raises by identity" => lambda do |_call, &continuation|
          continuation.call
        rescue StandardError => e # rubocop:disable Lint/UselessRescue -- re-raises by identity on purpose
          raise e
        end
      }.each do |label, body|
        it "replaces a core-owned failure with canceled through #{label}" do
          expect { run(layer(:client_unary, &body)) }
            .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
          expect(only(outcomes)).to have_attributes(code: "canceled", error: failure)
        end
      end

      it "keeps a translation by identity, with its own code" do
        translation = ConnectRPC::Error.new(:resource_exhausted, "Translated")
        translating = layer(:client_unary) do |_call, &continuation|
          continuation.call
        rescue ConnectRPC::Error
          raise translation
        end
        expect { run(translating) }.to(raise_error { |error| expect(error).to equal(translation) })
        expect(only(outcomes)).to have_attributes(code: "resource_exhausted", error: failure,
                                                  normalized_error: translation)
        expect(only(completions).delivery).to eq(:aborted)
      end

      it "keeps an exception the interceptor raises after an accepted cancellation" do
        own = ConnectRPC::Error.new(:aborted, "Interceptor")
        raising = layer(:client_unary) do |call, &continuation|
          response = continuation.call
          source.cancel
          raise own if call.cancellation.cancelled?

          response
        end
        expect { grpc_client([raising]).call(rpc, "hi", cancellation: source.token) }
          .to(raise_error { |error| expect(error).to equal(own) })
        expect(only(outcomes)).to have_attributes(code: "aborted", error: own, normalized_error: own)
        expect(only(completions).delivery).to eq(:completed)
      end
    end
  end

  describe "server" do
    let(:status_codec) do
      require "connectrpc/bindings/google_protobuf"
      ConnectRPC::Bindings::GoogleProtobuf.new
    end
    let(:handled) { [] }
    let(:handler) do
      handled = self.handled
      lambda do |request, context|
        handled << [request, context.request_metadata["x-principal"]&.first]
        "re:#{request}"
      end
    end

    def frame(bytes, flag = 0) = [flag, bytes.bytesize].pack("CN") + bytes

    def post(target, metadata = {}, cancellation: nil, body: "wire:hi")
      target.call(path: rpc.path, http_method: "POST", input: StringIO.new(body), cancellation: cancellation,
                  headers: { "content-type" => ["application/proto"] }.merge(metadata))
    end

    def get(target, metadata = {})
      target.call(path: rpc.path, http_method: "GET", headers: metadata, input: StringIO.new(""),
                  query: "encoding=proto&base64=1&message=#{['wire:hi'].pack('m0').tr('+/', '-_').delete('=')}")
    end

    def native(target, metadata = {})
      fields = []
      written = +""
      output = Object.new
      output.define_singleton_method(:write) { |chunk, **| written << chunk and nil }
      output.define_singleton_method(:finish) { |headers:| fields << headers and nil }
      output.define_singleton_method(:abort) { nil }
      input = StringIO.new(frame("wire:hi"))
      input.define_singleton_method(:read) { |length, **| super(length) }
      target.call_grpc(path: rpc.path, http_method: "POST", input: input, output: output, publish: ->(_) {},
                       headers: { "content-type" => ["application/grpc"] }.merge(metadata), http2: true)
      [only(fields), written]
    end

    def web(target, metadata = {})
      target.call(path: rpc.path, http_method: "POST", input: StringIO.new(frame("wire:hi")),
                  headers: { "content-type" => ["application/grpc-web+proto"] }.merge(metadata))
    end

    def principal(protocols)
      layer(:server_unary) do |call, &continuation|
        protocols << call.protocol
        call.context.request_metadata["x-principal"] = "verified"
        call.context.response_metadata["x-before"] = "yes"
        response = continuation.call
        call.context.response_trailers["x-after"] = "yes"
        response
      end
    end

    it "shares request metadata with the handler on every protocol without changing the recorded carrier" do
      protocols = []
      target = server(interceptors: [principal(protocols)], handlers: { rpc.path => handler },
                      protocol: %i[connect grpc grpc_web], status_codec: status_codec)
      metadata = { "x-caller" => ["zero"] }
      connect = post(target, metadata)
      expect(connect.status).to eq(200)
      expect(connect.headers).to include("x-before" => ["yes"], "trailer-x-after" => ["yes"])
      expect(get(target, metadata).headers).to include("x-before" => ["yes"], "trailer-x-after" => ["yes"])
      fields, = native(target, metadata)
      expect(fields).to include("grpc-status" => ["0"], "x-after" => ["yes"])
      expect(web(target, metadata).body).to include("x-after: yes")
      expect(protocols).to eq(%i[connect connect grpc grpc_web])
      expect(handled).to eq([%w[hi verified]] * 4)
      expect(outcomes.map(&:request_metadata)).to all(eq(metadata))
      expect(outcomes.map(&:code)).to all(eq("OK"))
    end

    it "sends a rejection with its metadata and the context's, without running the handler" do
      reject = layer(:server_unary) do |call, &_continuation|
        call.context.response_metadata["x-context"] = "set"
        call.context.response_trailers["x-context-tail"] = "set"
        raise ConnectRPC::Error.new(:unauthenticated, "Missing tenant", metadata: { "x-error" => "meta" },
                                                                        trailers: { "x-error-tail" => "tail" })
      end
      response = post(server(interceptors: [reject], handlers: { rpc.path => handler }))
      expect(response.status).to eq(401)
      expect(JSON.parse(response.body)).to include("code" => "unauthenticated", "message" => "Missing tenant")
      expect(response.headers).to include("x-context" => ["set"], "x-error" => ["meta"],
                                          "trailer-x-context-tail" => ["set"], "trailer-x-error-tail" => ["tail"])
      expect(handled).to be_empty
    end

    it "fails internal when an interceptor returns without yielding" do
      skip_handler = layer(:server_unary) { |_call, &_continuation| "skipped" }
      response = post(server(interceptors: [skip_handler], handlers: { rpc.path => handler }))
      expect(response.status).to eq(500)
      expect(handled).to be_empty
      expect(only(outcomes).error).to be_a(ConnectRPC::InterceptorStateError)
      expect(only(outcomes).code).to eq("internal")
    end

    it "fails with the handler's error when an interceptor rescues it and returns a message" do
      failure = ConnectRPC::Error.new(:not_found, "Missing")
      swallowing = layer(:server_unary) do |_call, &continuation|
        continuation.call
      rescue ConnectRPC::Error
        "fallback"
      end
      response = post(server(interceptors: [swallowing], handlers: { rpc.path => ->(*) { raise failure } }))
      expect(response.status).to eq(404)
      expect(JSON.parse(response.body)).to include("code" => "not_found", "message" => "Missing")
      expect(only(outcomes).error).to equal(failure)
    end

    it "fails with the inner translation and records the handler original in a two-layer chain" do
      original = RuntimeError.new("handler bug")
      outer = layer(:server_unary) do |_call, &continuation|
        continuation.call
      rescue ConnectRPC::Error
        "fallback"
      end
      inner = layer(:server_unary) do |_call, &continuation|
        continuation.call
      rescue RuntimeError
        raise ConnectRPC::Error.new(:invalid_argument, "Translated")
      end
      response = post(server(interceptors: [outer, inner], handlers: { rpc.path => ->(*) { raise original } }))
      expect(response.status).to eq(400)
      expect(JSON.parse(response.body)).to include("code" => "invalid_argument", "message" => "Translated")
      expect(only(outcomes)).to have_attributes(error: original, code: "invalid_argument")
    end

    it "sends a replacement message returned after a successful continuation" do
      replacing = layer(:server_unary) { |_call, &continuation| "#{continuation.call}!" }
      expect(post(server(interceptors: [replacing], handlers: { rpc.path => handler })).body).to eq("wire:re:hi!")
      fields, written = native(server(interceptors: [replacing], handlers: { rpc.path => handler },
                                      protocol: :grpc, status_codec: status_codec))
      expect(fields["grpc-status"]).to eq(["0"])
      expect(written).to include("wire:re:hi!")
    end

    it "follows the nil response path when an interceptor returns nil" do
      nil_layer = layer(:server_unary) do |_call, &continuation|
        continuation.call
        nil
      end
      expect(post(server(interceptors: [nil_layer], handlers: { rpc.path => handler })).status).to eq(500)
    end

    it "sends the accepted cancellation and records the original when cancelled during an interceptor" do
      source = ConnectRPC::CancellationSource.new
      failure = RuntimeError.new("after cancel")
      cancelling = layer(:server_unary) do |call, &_continuation|
        source.cancel
        call.context.cancelled?
        raise failure
      end
      response = post(server(interceptors: [cancelling], handlers: { rpc.path => handler }),
                      cancellation: source.token)
      expect(JSON.parse(response.body)).to include("code" => "canceled")
      expect(only(outcomes)).to have_attributes(error: failure, code: "canceled")
      expect(handled).to be_empty
    end

    it "returns unimplemented through the chain for a declared method without a handler" do
      seen = []
      observing = layer(:server_unary) do |_call, &continuation|
        continuation.call
      rescue ConnectRPC::Error => e
        seen << e.code
        raise
      end
      response = post(server(interceptors: [observing], handlers: {}))
      expect(JSON.parse(response.body)).to include("code" => "unimplemented")
      expect(seen).to eq(["unimplemented"])
    end

    it "runs a custom invoker with no handler entry on native gRPC" do
      invoker = lambda do |_method, request, context|
        "invoked:#{request}:#{context.request_metadata['x-principal']&.first}"
      end
      target = ConnectRPC::Server.new(services: [service], binding: binding, invoker: invoker, protocol: :grpc,
                                      status_codec: status_codec, interceptors: [principal([])])
      fields, written = native(target)
      expect(fields["grpc-status"]).to eq(["0"])
      expect(written).to include("wire:invoked:hi:verified")
    end

    describe "continuation misuse" do
      {
        "a second yield" => lambda { |_call, &continuation|
          continuation.call
          continuation.call
        },
        "a swallowed second yield" => lambda { |_call, &continuation|
          response = continuation.call
          begin
            continuation.call
          rescue ConnectRPC::InterceptorStateError
            nil
          end
          response
        },
        "a continuation from another thread" => lambda { |_call, &continuation|
          foreign = Thread.new do
            continuation.call
          rescue ConnectRPC::InterceptorStateError
            nil
          end
          foreign.join
          continuation.call
        }
      }.each do |label, body|
        it "fails internal with InterceptorStateError recorded for #{label}" do
          response = post(server(interceptors: [layer(:server_unary, &body)], handlers: { rpc.path => handler }))
          expect(response.status).to eq(500)
          expect(only(outcomes).error).to be_a(ConnectRPC::InterceptorStateError)
          expect(handled.size).to eq(1)
        end
      end

      it "rejects a captured continuation after the interceptor returned" do
        captured = nil
        capturing = layer(:server_unary) do |_call, &continuation|
          captured = continuation
          continuation.call
        end
        expect(post(server(interceptors: [capturing], handlers: { rpc.path => handler })).status).to eq(200)
        expect { captured.call }.to raise_error(ConnectRPC::InterceptorStateError)
        expect(handled.size).to eq(1)
      end
    end

    it "runs interceptors inside the activation and processing duration" do
      interval = nil
      active = nil
      timing = layer(:server_unary) do |_call, &continuation|
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        active = Thread.current[:interceptor_spec_rpc]
        response = continuation.call
        interval = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        response
      end
      expect(post(server(interceptors: [timing], handlers: { rpc.path => handler })).status).to eq(200)
      expect(active&.role).to eq(:server)
      expect(only(outcomes).duration).to be >= interval
    end
  end
end
