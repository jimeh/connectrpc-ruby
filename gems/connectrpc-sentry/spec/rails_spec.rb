# frozen_string_literal: true

require_relative "support/rails_application"

RSpec.describe "Rails with sentry-rails" do
  # One Rails application per process, booted by the first example that needs it.
  app = nil

  before do
    app ||= SentryRailsApplication.boot!(self)
    SentryRailsApplication::EchoController.rpc_instrumentation = bus
  end

  after { SentryRailsApplication::EchoController.rpc_instrumentation = nil }

  def post_rpc(app, name)
    rack_post(app, name, env: { "HTTP_HOST" => "www.example.com" })
  end

  def rpc_span(event)
    span = event.spans.find { |entry| entry[:op] == "rpc.server" }
    expect(span).not_to be_nil, "#{event.transaction} carries no rpc.server child"
    span
  end

  def ancestors(event, span)
    by_id = event.spans.to_h { |entry| [entry[:span_id], entry] }
    chain = []
    while (parent = by_id[span[:parent_span_id]])
      chain << parent[:op]
      span = parent
    end
    chain
  end

  it "nests one rpc.server span in the renamed http.server transaction, above controller and ActiveRecord spans" do
    expect(post_rpc(app, "rails").status).to eq(200)
    expect(transactions.size).to eq(1)
    host = transactions.first
    expect(trace_of(host)[:op]).to eq("http.server")
    expect(host.transaction).to eq("example.v1.EchoService/Echo")
    expect(host.transaction_info).to eq(source: :route)
    rpc = rpc_span(host)
    expect(rpc).to include(parent_span_id: trace_of(host)[:span_id], description: "example.v1.EchoService/Echo",
                           status: "ok", origin: "auto.rpc.connectrpc")
    expect(rpc[:timestamp]).not_to be_nil
    expect(rpc[:data]).to include("rpc.system.name" => "connectrpc", "rpc.method" => "example.v1.EchoService/Echo",
                                  "rpc.status_code" => "OK", "http.response.status_code" => 200,
                                  "connectrpc.processing.state" => "finished")
    expect(rpc[:data]).not_to have_key("connectrpc.delivery")
    controller = host.spans.find { |span| span[:op] == "view.process_action.action_controller" }
    insert = host.spans.find { |span| span[:op].start_with?("db") && span[:description].to_s.include?("INSERT") }
    expect([controller, insert]).to all(be_a(Hash))
    expect(ancestors(host, controller)).to include("rpc.server")
    expect(ancestors(host, insert)).to include("rpc.server")
    expect(host.spans.count { |span| span[:op] == "rpc.server" }).to eq(1)
    expect(rpc_transactions).to be_empty
    expect(error_events).to be_empty
    expect(scope_span).to be_nil
  end

  [false, true].each do |subscribed|
    it "reports a handler bug once on the rpc.server span with register_error_subscriber #{subscribed}" do
      subscriber = Sentry::Rails::ErrorSubscriber.new if subscribed
      Rails.error.subscribe(subscriber) if subscriber
      expect(post_rpc(app, "bug").status).to eq(500)
      expect(error_events.size).to eq(1)
      error = error_events.first
      rpc = rpc_span(transactions.first)
      expect(trace_of(error)).to include(span_id: rpc[:span_id], trace_id: rpc[:trace_id])
      expect(mechanism(error)).to include(type: subscribed ? "rails" : "connectrpc", handled: false)
      expect(error.to_h.dig(:exception, :values).last[:type]).to eq("RuntimeError")
      expect(rpc).to include(status: "internal_error")
      expect(rpc[:data]).to include("rpc.status_code" => "internal")
    ensure
      Rails.error.unsubscribe(subscriber) if subscriber
    end
  end

  it "does not report deliberate errors or Rails rescue_responses that map to other codes" do
    # Sentry drops HTTP 401-404 transactions by default; keep the not_found one observable here.
    sentry(trace_ignore_status_codes: [])
    expect(post_rpc(app, "deliberate").status).to eq(400)
    expect(post_rpc(app, "missing").status).to eq(404)
    expect(error_events).to be_empty
    codes = transactions.map { |event| rpc_span(event)[:data]["rpc.status_code"] }
    expect(codes).to eq(%w[invalid_argument not_found])
  end
end

RSpec.configure do |config|
  config.after(:suite) do
    FileUtils.remove_entry(SentryRailsApplication::ROOT) if File.directory?(SentryRailsApplication::ROOT)
  end
end
