# frozen_string_literal: true

require "connectrpc/otel"
require "open3"
require "connectrpc/transports/net_http"
require_relative "support/net_http_peer"

RSpec.describe "Trace propagation" do
  def upstream_trace = "771a43a4192642f0b136d5159a501700"
  def upstream_parent = "b28b6b8e2e4b4c8f"

  def incoming(trace: nil, baggage: nil, echo: ->(message, _) { message }, fail: nil)
    headers = {}
    headers["HTTP_SENTRY_TRACE"] = trace if trace
    headers["HTTP_BAGGAGE"] = baggage if baggage
    app = ConnectRPC::Rack.new(server: echo_server(instrumentation: bus, echo: echo, fail: fail))
    rack_post(app, "incoming", headers: headers)
  end

  # Records the request metadata the server received.
  def outgoing(instrumentation, base_url: "https://example.invalid", metadata: {})
    seen = {}
    echo = lambda do |message, context|
      %w[sentry-trace baggage].each { |key| seen[key] = context.request_metadata[key] }
      message
    end
    typed = in_process(echo_server(instrumentation: nil, echo: echo), instrumentation: instrumentation,
                                                                      base_url: base_url)
    typed.echo(MESSAGE.new(name: "outgoing"), metadata: metadata)
    seen
  end

  # Records the headers a client sends, for base URLs no in-process server routes.
  def sent_headers(instrumentation, base_url:)
    headers = nil
    transport = lambda do |request|
      headers = request.headers
      ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                        body: MESSAGE.new.to_proto)
    end
    core = ConnectRPC::Client.new(base_url: base_url, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                  transport: transport, instrumentation: instrumentation)
    ECHO::Client.new(client: core).echo(MESSAGE.new(name: "outgoing"))
    headers
  end

  describe "incoming" do
    it "inherits the propagated decision without a sampler" do
      sentry(sample_rate: 0.0)
      expect(incoming(trace: "#{upstream_trace}-#{upstream_parent}-1").status).to eq(200)
      expect(trace_of(rpc_transactions.first)).to include(trace_id: upstream_trace, parent_span_id: upstream_parent)
      sentry(sample_rate: 1.0)
      expect(incoming(trace: "#{upstream_trace}-#{upstream_parent}-0").status).to eq(200)
      expect(rpc_transactions).to be_empty
    end

    it "lets an application sampler override the parent decision and passes it RPC context without Rack env" do
      contexts = []
      sentry(sample_rate: nil, traces_sampler: lambda { |context|
        contexts << context
        0.0
      })
      expect(incoming(trace: "#{upstream_trace}-#{upstream_parent}-1").status).to eq(200)
      expect(rpc_transactions).to be_empty
      expect(contexts.size).to eq(1)
      expect(contexts.first).to include(parent_sampled: true, connectrpc: {
                                          role: :server, protocol: :connect, method: "example.v1.EchoService/Echo"
                                        })
      expect(contexts.first).not_to have_key(:env)
    end

    it "keeps serving when the application sampler raises" do
      sentry(sample_rate: 1.0, traces_sampler: ->(_context) { raise "sampler bug" })
      expect(incoming(trace: "#{upstream_trace}-#{upstream_parent}-1").status).to eq(200)
      if Gem::Version.new(Sentry::VERSION) >= Gem::Version.new("7.0.0")
        # sentry-ruby 7 falls back to traces_sample_rate.
        expect(trace_of(rpc_transactions.first)).to include(trace_id: upstream_trace)
      else
        # sentry-ruby 6 lets the sampler raise through start, which the lifecycle reports.
        expect(lifecycle_errors.map(&:message)).to eq(["sampler bug"])
        lifecycle_errors.clear
        expect(rpc_transactions).to be_empty
      end
      expect(scope_span).to be_nil
    end

    it "follows a deferred decision with the propagated sample_rand" do
      sentry(sample_rate: 0.5)
      [["0.2", 1], ["0.7", 0]].each do |sample_rand, count|
        baggage = "sentry-trace_id=#{upstream_trace},sentry-sample_rand=#{sample_rand}"
        expect { incoming(trace: "#{upstream_trace}-#{upstream_parent}", baggage: baggage) }
          .to change { rpc_transactions.size }.by(count)
      end
    end

    it "starts a new trace for malformed, repeated or other-organization headers" do
      sentry(org_id: "1")
      incoming(trace: "not-a-trace")
      traceparent = "#{upstream_trace}-#{upstream_parent}-1"
      incoming(trace: traceparent, baggage: "sentry-trace_id=#{upstream_trace},sentry-org_id=2")
      app = ConnectRPC::Rack.new(server: echo_server(instrumentation: bus))
      env = Rack::MockRequest.env_for(ECHO::ECHO.path, method: "POST", "CONTENT_TYPE" => "application/proto",
                                                       input: MESSAGE.new.to_proto)
      server = app.instance_variable_get(:@server)
      lifecycle = server.start_call(path: ECHO::ECHO.path, http_method: "POST",
                                    headers: { "content-type" => ["application/proto"],
                                               "sentry-trace" => [traceparent, traceparent] })
      server.call(path: ECHO::ECHO.path, http_method: "POST", headers: { "content-type" => ["application/proto"] },
                  input: env["rack.input"], lifecycle: lifecycle)
      lifecycle.finish(delivery: :unobserved)
      traces = rpc_transactions.map { |event| trace_of(event) }
      expect(traces.size).to eq(3)
      expect(traces.map { |trace| trace[:trace_id] }).not_to include(upstream_trace)
      expect(traces.map { |trace| trace[:parent_span_id] }).to eq([nil, nil, nil])
    end

    it "keeps incoming dynamic sampling context and builds head context for new traces" do
      baggage = "sentry-trace_id=#{upstream_trace},sentry-public_key=upstream,sentry-release=upstream@1," \
                "sentry-sample_rate=1.0,other=vendor"
      incoming(trace: "#{upstream_trace}-#{upstream_parent}-1", baggage: baggage)
      incoming
      continued, head = rpc_transactions
      expect(continued.dynamic_sampling_context).to eq("trace_id" => upstream_trace, "public_key" => "upstream",
                                                       "release" => "upstream@1", "sample_rate" => "1.0")
      expect(head.dynamic_sampling_context).to include("trace_id" => trace_of(head)[:trace_id],
                                                       "public_key" => "public", "sampled" => "true",
                                                       "transaction" => "example.v1.EchoService/Echo")
    end

    it "continues the incoming trace for errors-only applications" do
      sentry(sample_rate: nil)
      reported = nil
      handler = lambda do |*|
        reported = Sentry.get_current_scope.propagation_context.trace_id
        raise "errors-only bug"
      end
      expect(incoming(trace: "#{upstream_trace}-#{upstream_parent}-1", fail: handler, echo: handler).status).to eq(500)
      expect(transactions).to be_empty
      expect(reported).to eq(upstream_trace)
      expect(trace_of(error_events.first)).to include(trace_id: upstream_trace)
      expect(error_events.size).to eq(1)
      expect(Sentry.get_current_scope.propagation_context.trace_id).not_to eq(upstream_trace)
    end
  end

  describe "outgoing" do
    it "injects the client span with its sampled flag and dynamic sampling context" do
      caller = start_caller
      seen = outgoing(bus)
      caller.finish
      client = transactions.first.spans.find { |span| span[:op] == "rpc.client" }
      expect(seen.fetch("sentry-trace")).to eq(["#{caller.trace_id}-#{client[:span_id]}-1"])
      expect(seen.fetch("baggage").size).to eq(1)
      expect(seen.fetch("baggage").first).to include("sentry-trace_id=#{caller.trace_id}", "sentry-sampled=true")
      expect(client[:data]).to include("server.address" => "example.invalid", "server.port" => 443)
    end

    it "injects the scope propagation context without a span and records no client observation" do
      context = Sentry.get_current_scope.propagation_context
      seen = outgoing(bus)
      expect(seen.fetch("sentry-trace")).to eq(["#{context.trace_id}-#{context.span_id}"])
      expect(seen.fetch("baggage").first).to include("sentry-trace_id=#{context.trace_id}")
      expect(transactions).to be_empty
    end

    it "injects nothing when propagate_traces is off" do
      sentry(propagate_traces: false)
      start_caller
      expect(outgoing(bus)).to eq("sentry-trace" => nil, "baggage" => nil)
    end

    # Sentry's HTTP integrations match targets against scheme://host/path, without port or query.
    {
      ["https://example.invalid"] => true, [%r{\Ahttps://example\.invalid/}] => true,
      ["example.invalid"] => true, [%r{/example\.v1\.EchoService/Echo\z}] => true,
      ["other.invalid"] => false, [%r{\Ahttp://example\.invalid}] => false, [/\Aexample\.invalid/] => false,
      [/:443/] => false
    }.each do |targets, propagated|
      it "#{propagated ? 'propagates' : 'does not propagate'} to URL targets #{targets.inspect}" do
        sentry(trace_propagation_targets: targets)
        caller = start_caller
        seen = outgoing(bus)
        expect(seen.fetch("sentry-trace").nil?).to be(!propagated)
        caller.finish
        expect(transactions.first.spans.map { |span| span[:op] }).to eq(["rpc.client"])
      end
    end

    {
      "https://example.invalid:8443/api/" => "https://example.invalid/api/example.v1.EchoService/Echo",
      "http://[::1]:8080" => "http://[::1]/example.v1.EchoService/Echo"
    }.each do |base_url, url|
      it "matches #{base_url} as #{url}" do
        sentry(trace_propagation_targets: [/\A#{Regexp.escape(url)}\z/])
        expect(sent_headers(bus, base_url: base_url)["sentry-trace"]).not_to be_nil
      end
    end

    context "on the core Net::HTTP transport" do
      include NetHTTPPeer

      # Sentry.init applies the default :http patch in this process, and it stays prepended.
      it "leaves propagation to Sentry's applied :http patch, so one set of Sentry baggage is sent" do
        expect(Net::HTTP.ancestors).to include(Sentry::Net::HTTP)
        caller = start_caller
        headers = net_http_headers(bus, metadata: { "baggage" => ["caller=1"] })
        caller.finish
        spans = transactions.first.spans
        client = spans.find { |span| span[:op] == "rpc.client" }
        http = spans.find { |span| span[:op] == "http.client" }
        expect(http[:parent_span_id]).to eq(client[:span_id])
        expect(headers["sentry-trace"]).to eq(["#{caller.trace_id}-#{http[:span_id]}-1"])
        keys = baggage_keys(headers)
        expect(keys.grep(/\Asentry-/)).to eq(keys.grep(/\Asentry-/).uniq)
        expect(keys).to include("sentry-trace_id", "caller")
      end

      it "leaves the scope propagation context to the patch without a span" do
        context = Sentry.get_current_scope.propagation_context
        headers = net_http_headers(bus)
        expect(headers["sentry-trace"]).to eq(["#{context.trace_id}-#{context.span_id}"])
        expect(baggage_keys(headers).count("sentry-trace_id")).to eq(1)
      end

      # The patch rebuilds the URL from the request path and escapes it again, so an
      # encoded base path matches different targets there than in the subscriber.
      { %r{\Ahttp://127\.0\.0\.1/a%20b/} => "rpc.client", %r{/a%2520b/} => "http.client" }.each do |target, op|
        it "sends one set of Sentry headers from the #{op} span for an escaped base path and #{target.inspect}" do
          sentry(trace_propagation_targets: [target])
          caller = start_caller
          headers = net_http_headers(bus, path: "/a%20b/")
          caller.finish
          parent = transactions.first.spans.find { |span| span[:op] == op }
          expect(headers["sentry-trace"]).to eq(["#{caller.trace_id}-#{parent[:span_id]}-1"])
          expect(baggage_keys(headers).count("sentry-trace_id")).to eq(1)
        end
      end

      # The patch skips requests to the DSN's host, so the subscriber injects there.
      it "injects on a NetHTTP subclass only for the DSN host, which the patch skips" do
        capturing = Class.new(ConnectRPC::Transports::NetHTTP) do
          attr_reader :headers

          def call(request)
            @headers = request.headers
            ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                              body: Example::V1::EchoMessage.new.to_proto)
          end
        end
        traces = %w[https://sentry.invalid https://example.invalid].to_h do |base_url|
          transport = capturing.new
          core = ConnectRPC::Client.new(base_url: base_url, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                        transport: transport, instrumentation: bus)
          ECHO::Client.new(client: core).echo(MESSAGE.new(name: "outgoing"))
          [base_url, transport.headers["sentry-trace"]]
        end
        expect(traces.fetch("https://sentry.invalid")).not_to be_nil
        expect(traces.fetch("https://example.invalid")).to be_nil
      end

      # A patch, once prepended, cannot be removed, so the unpatched case runs in a fresh process.
      it "injects itself when the :http patch is disabled" do
        script = <<~RUBY
          require "connectrpc/sentry"
          require "connectrpc/bindings/google_protobuf"
          require "echo_connect"
          require "json"
          require #{File.expand_path('support/net_http_peer', __dir__).dump}
          Sentry.init do |config|
            config.dsn = #{SentryHelpers::DSN.dump}
            config.transport.transport_class = Sentry::DummyTransport
            config.background_worker_threads = 0
            config.auto_session_tracking = false
            config.send_client_reports = false
            config.sdk_logger = Sentry::Logger.new(File::NULL)
            config.traces_sample_rate = 1.0
            config.enabled_patches = []
          end
          abort "Net::HTTP is patched" if Net::HTTP.ancestors.include?(Sentry::Net::HTTP)
          caller = Sentry.start_transaction(name: "caller", op: "test")
          Sentry.get_current_scope.set_span(caller)
          instrumentation = ConnectRPC::Instrumentation.new(subscribers: [ConnectRPC::Sentry::Subscriber.new])
          extend NetHTTPPeer
          headers = net_http_headers(instrumentation)
          puts JSON.generate(trace_id: caller.trace_id, headers: headers, keys: baggage_keys(headers))
        RUBY
        fixtures = File.join(ROOT, "spec/fixtures/generation/echo")
        output, errors, status = Open3.capture3(RbConfig.ruby, "-I", fixtures, "-e", script)
        expect(status).to be_success, errors
        result = JSON.parse(output)
        expect(result.dig("headers", "sentry-trace")).to match([start_with("#{result.fetch('trace_id')}-")])
        expect(result.fetch("keys").count("sentry-trace_id")).to eq(1)
      end
    end

    context "with the real OpenTelemetry subscriber" do
      # The OTel subscriber uses the global propagator; install the one
      # OpenTelemetry::SDK.configure sets by default and restore the original.
      around do |example|
        original = OpenTelemetry.propagation
        OpenTelemetry.propagation =
          OpenTelemetry::Context::Propagation::CompositeTextMapPropagator.compose_propagators(
            [OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator,
             OpenTelemetry::Baggage::Propagation.text_map_propagator]
          )
        example.run
      ensure
        OpenTelemetry.propagation = original
      end

      def otel_baggage
        OpenTelemetry::Baggage.set_value("tenant", "acme")
      end

      {
        "OpenTelemetry first" => [ConnectRPC::OTel::Subscriber, ConnectRPC::Sentry::Subscriber],
        "Sentry first" => [ConnectRPC::Sentry::Subscriber, ConnectRPC::OTel::Subscriber]
      }.each do |order, classes|
        it "carries caller, Sentry and OpenTelemetry baggage with #{order}" do
          instrumentation = bus(*classes.map(&:new))
          seen = OpenTelemetry::Context.with_current(otel_baggage) do
            outgoing(instrumentation, metadata: { "baggage" => ["caller=1"] })
          end
          expect(seen.fetch("baggage").size).to eq(3)
          expect(seen.fetch("baggage").first).to eq("caller=1")
          expect(seen.fetch("baggage")[1]).to start_with("sentry-")
          expect(seen.fetch("baggage").last).to eq("tenant=acme")
          expect(seen.fetch("sentry-trace").size).to eq(1)
          expect(lifecycle_errors).to be_empty
        end
      end
    end
  end
end
