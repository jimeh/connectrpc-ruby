# frozen_string_literal: true

require "rails"
require "action_controller/railtie"
require "connectrpc/rails"
require "rackup/handler/webrick"
require "tmpdir"

# This application keeps Rails' native token/origin/forgery policy. The browser
# CSRF success uses this application's own origin; cross-origin remains denied.
module BrowserRails
  class << self
    attr_accessor :origin, :own_origin, :events
  end

  class Application < Rails::Application
    config.load_defaults Rails::VERSION::STRING.split(".").first(2).join(".")
    config.secret_key_base = "browser-fixture-only-" * 8
    config.eager_load = false
    config.logger = Logger.new(File::NULL)
    config.hosts = ["127.0.0.1"]
    config.action_controller.allow_forgery_protection = true
    config.action_dispatch.show_exceptions = :none
    config.session_store :cookie_store, key: "browser_rpc"
  end

  def self.start(allowed_origin) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    self.origin = allowed_origin
    self.events = []
    Application.config.root = Dir.mktmpdir("rails-", File.join(BrowserFixture::ROOT, "vendor/browser"))
    app = Application.initialize!
    app.routes.draw do
      get "/token", to: "browser_peer#token"
      match "/connectrpc.grpcinterop.v1.Peer(/*rpc)", to: "browser_peer#connect", via: %i[post options], format: false
    end
    ready = Queue.new
    on_start = -> { ready << true }
    server = Rackup::Handler::WEBrick::Server.new(
      app, BindAddress: "127.0.0.1", Port: 0, AccessLog: [],
           Logger: WEBrick::Log.new(File::NULL), StartCallback: on_start
    )
    self.own_origin = "http://127.0.0.1:#{server.listeners.first.addr[1]}"
    thread = Thread.new { server.start }
    raise "Rails browser fixture readiness failed" unless ready.pop(timeout: 5)

    [server, thread, "http://127.0.0.1:#{server.listeners.first.addr[1]}"]
  end
end

class BrowserPeerController < ActionController::Base # rubocop:disable Style/OneClassPerFile -- standalone fixture app
  include ConnectRPC::Rails::Controller

  before_action :authenticate_rpc, only: :connect
  after_action :secure_rpc, only: :connect

  def token
    cookies[:browser_fixture] = "present"
    render json: { token: form_authenticity_token }
  end

  private

  def authenticate_rpc
    BrowserRails.events << :authentication
    unless request.headers["authorization"] == "Bearer browser"
      raise ConnectRPC::Error.new(:unauthenticated, "Authentication required")
    end

    connect_context.response_metadata["x-authenticated"] = ["yes"]
  end

  def secure_rpc
    response.set_header("content-security-policy", "default-src 'none'")
    BrowserRails.events << :after
  end

  def connect_protocol = :grpc_web
  def connect_binding = ConnectRPC::Bindings::GoogleProtobuf.new
  def connect_status_codec = connect_binding
  def connect_service = BrowserFixture::PEER::SERVICE

  def connect_cors
    ConnectRPC::GRPCWebCORS.new(origins: [BrowserRails.origin, BrowserRails.own_origin], credentials: true,
                                request_headers: %w[authorization x-csrf-token], expose_headers: ["x-authenticated"])
  end

  def connect_handlers
    unsupported = ->(*) { raise "streaming invoked" }
    BrowserFixture::PEER.handlers(unary: method(:rpc_unary), server_stream: unsupported,
                                  client_stream: unsupported, bidi: unsupported)
  end

  def rpc_unary(message, _context)
    BrowserRails.events << :handler
    raise "Missing native session cookie" unless cookies[:browser_fixture] == "present"

    message
  end
end
