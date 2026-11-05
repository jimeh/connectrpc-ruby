# frozen_string_literal: true

require "action_controller"
require "action_dispatch"

class FalconFallbackController < ActionController::Base
  self.allow_forgery_protection = true
  protect_from_forgery with: :exception
  before_action do
    head :unauthorized unless request.headers["x-auth"] == "allowed"
  end
  rescue_from ActionController::InvalidAuthenticityToken do
    head :unprocessable_content
  end

  def index
    render plain: "rails-controller"
  end
end

RSpec.describe "Falcon Rack/Rails fallback boundary" do
  it "keeps unary media on HTTP/1 streaming declarations outside binding, input reads and handler execution" do
    reactor do
      rpc = ConnectRPC::ServerStreamMethod.new(service: "fallback.Streams", name: "Watch",
                                               request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
      binding = ConnectRPC::Bindings::GoogleProtobuf.new
      target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, methods: [rpc])],
                                      binding: binding, handlers: { rpc.path => ->(*) { raise "handler invoked" } })
      expect(binding).not_to receive(:encode)
      expect(binding).not_to receive(:decode)
      allow(AsyncHelpers::INTERNAL::Input).to receive(:new).and_wrap_original do |original, *args, **options|
        original.call(*args, **options).tap { |input| expect(input).not_to receive(:read) }
      end
      falcon(server: target) do |_host, origin|
        { "application/proto" => 501 }.each do |media, status|
          expect(http1(origin, rpc.path, method: "POST", headers: { "content-type" => media },
                                         body: "must not read").first).to eq(status)
        end
      end
    end
  end

  it "executes ordinary Rails authentication and CSRF without imposing either on native RPC" do
    routes = ActionDispatch::Routing::RouteSet.new
    routes.draw { match "/rails", to: "falcon_fallback#index", via: %i[get post] }
    reactor do
      rack = ActionDispatch::Session::CookieStore.new(routes, key: "falcon-test-session")
      rack = ActionDispatch::Cookies.new(rack)
      app = lambda do |env|
        env["action_dispatch.key_generator"] = ActiveSupport::KeyGenerator.new("fixture-secret" * 8)
        env["action_dispatch.signed_cookie_salt"] = "signed cookie"
        env["action_dispatch.encrypted_cookie_salt"] = "encrypted cookie"
        env["action_dispatch.encrypted_signed_cookie_salt"] = "signed encrypted cookie"
        env["action_dispatch.cookies_serializer"] = :json
        rack.call(env)
      end
      falcon(rack_app: app) do |_host, origin|
        expect(http1(origin, "/rails").first).to eq(401)
        expect(http1(origin, "/rails", headers: { "x-auth" => "allowed" }).values_at(0, 2))
          .to eq([200, "rails-controller"])
        expect(http1(origin, "/rails", headers: { "x-auth" => "allowed" }, method: "POST").first).to eq(422)
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "native")).message.name).to eq("native")
      ensure
        transport&.close(grace: 0)
      end
    end
  end

  it "supports ordinary streamed and callable Rack bodies with completion callbacks" do
    reactor do
      completed = []
      closed = []
      rack = lambda do |env|
        env["rack.response_finished"] << ->(*) { completed << env["PATH_INFO"] }
        route = env["PATH_INFO"]
        body = case route
               when "/callable"
                 lambda do |stream|
                   stream.write("callable-body")
                 ensure
                   stream.close
                 end
               when "/array"
                 ["array-body"]
               when "/readable"
                 Protocol::HTTP::Body::Buffered.wrap("readable-body")
               else
                 Class.new do
                   def each
                     yield "streamed-"
                     yield "body"
                   end
                 end.new
               end
        body.define_singleton_method(:close) { |*| closed << route }
        [200, { "content-type" => "text/plain" }, body]
      end
      falcon(rack_app: rack) do |_host, origin|
        expect(http1(origin, "/callable").values_at(0, 2)).to eq([200, "callable-body"])
        expect(http1(origin, "/streamed").values_at(0, 2)).to eq([200, "streamed-body"])
        expect(http1(origin, "/array").values_at(0, 2)).to eq([200, "array-body"])
        expect(http1(origin, "/readable").values_at(0, 2)).to eq([200, "readable-body"])
        expect(completed).to eq(%w[/callable /streamed /array /readable])
        expect(closed).to eq(completed)
      end
    end
  end

  it "rejects full hijack before execution and partial hijack/protocol/101 responses while closing rejected bodies" do
    reactor do
      transferred = []
      closed = []
      completed = []
      rack = lambda do |env|
        route = env["PATH_INFO"]
        env["rack.response_finished"] << ->(*) { completed << route }
        case route
        when "/rescued"
          begin
            env["rack.hijack"].call
          rescue StandardError
            nil
          end
        when "/full"
          env["rack.hijack"].call
          transferred << :full
        when "/native-hijack"
          env["protocol.http.request"].hijack!
          transferred << :native
        end
        body = ["rejected"]
        body.define_singleton_method(:close) { closed << route }
        headers = { "content-type" => "text/plain" }
        headers["rack.hijack"] = ->(*) { transferred << :partial } if route == "/partial"
        headers["rack.protocol"] = "websocket" if route == "/protocol"
        [route == "/101" ? 101 : 200, headers, body]
      end
      falcon(rack_app: rack) do |_host, origin|
        %w[/full /native-hijack /rescued /partial /protocol /101].each do |route|
          expect(http1(origin, route).first).to eq(501)
        end
        expect(transferred).to be_empty
        expect(closed).to eq(%w[/rescued /partial /protocol /101])
        expect(completed).to eq(%w[/full /native-hijack /rescued /partial /protocol /101])
      end
    end
  end

  it "rejects actual CONNECT, WebSocket and h2c Upgrade requests before fallback success" do
    reactor do
      seen = []
      falcon(rack_app: lambda { |env|
        seen << env
        [200, {}, ["unsafe"]]
      }) do |_host, origin|
        uri = URI(origin)
        [["CONNECT", "localhost:443", ""], ["GET", "/", "Upgrade: websocket\r\nConnection: Upgrade\r\n"],
         ["GET", "/", "Upgrade: h2c\r\nConnection: Upgrade\r\n"]].each do |method, route, extra|
          raw = IO::Endpoint.tcp(uri.hostname, uri.port).connect
          raw.write("#{method} #{route} HTTP/1.1\r\nHost: localhost\r\n#{extra}\r\n")
          line = raw.gets
          expect(line).to start_with("HTTP/1.1 501")
        ensure
          AsyncHelpers::INTERNAL.raw_close(raw)
        end
        expect(seen).to be_empty
      end
    end
  end
end
