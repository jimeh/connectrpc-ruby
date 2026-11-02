# frozen_string_literal: true

class EchoController < ActionController::API
  include ConnectRPC::Rails::Controller

  before_action :rpc_before, only: :connect
  around_action :rpc_around, only: :connect
  after_action :rpc_after, only: :connect
  rescue_from ArgumentError do |_error|
    raise ConnectRPC::Error.new(:invalid_argument, "mapped safely")
  end

  def ordinary
    render json: { name: params[:name], format: request.format.to_s }
  end

  private

  def connect_service = Example::V1::EchoService::SERVICE
  def connect_binding = ConnectRPC::Bindings::GoogleProtobuf.new
  def connect_handlers = Example::V1::EchoService.handlers(echo: method(:rpc_echo), fail: method(:rpc_fail))

  def connect_limits
    limits = case request.headers["x-case"]
             when "request-limit" then { request_limit: 8 }
             when "gzip-inflated-limit" then { request_limit: 8, request_wire_limit: 1024 }
             when "gzip-wire-limit" then { response_wire_limit: 80 }
             when "response-limit" then { response_limit: 100 }
             else {}
             end
    limits[:encoding] = :gzip if request.headers["x-gzip"] == "yes"
    limits
  end

  def rpc_before
    trace = request.get_header("connectrpc.test.trace")
    trace&.push([:before, connect_request, connect_context, connect_method])
    case request.headers["x-case"]
    when "auth" then raise ConnectRPC::Error.new(:unauthenticated, "Sign in")
    when "raw" then render plain: "private Rails body", status: request.headers.fetch("x-status").to_i
    when "redirect" then redirect_to "/ordinary"
    when "cookie"
      response.set_header("set-cookie", ["first=1; path=/", "second=2; path=/"])
      response.set_header("www-authenticate", 'Bearer realm="rpc"')
      response.set_header("x-request-id", "host-id")
      response.set_header("content-security-policy", "default-src 'none'")
      %w[content-length content-encoding etag last-modified location].each { |key| response.set_header(key, "stale") }
      response.set_header("content-type", "text/plain; charset=utf-8")
    end
  end

  def rpc_around
    request.get_header("connectrpc.test.trace")&.push([:around_before])
    yield
  ensure
    request.get_header("connectrpc.test.trace")&.push([:around_ensure])
  end

  def rpc_after
    request.get_header("connectrpc.test.trace")&.push([:after, connect_request, connect_context, connect_method])
    self.status = :forbidden if request.headers["x-case"] == "after-halt"
  end

  def rpc_echo(message, context)
    request.get_header("connectrpc.test.trace")&.push([:handler, message, context, connect_method])
    case request.headers["x-case"]
    when "argument" then raise ArgumentError, "secret exception"
    when "bug" then raise "secret bug"
    when "params"
      context.response_metadata.add("x-params",
                                    [params[:query], params["query"], params[:rpc], params["rpc"]].join("|"))
    when "concurrent"
      sync = request.get_header("connectrpc.test.sync")
      sync.fetch(:entered).push([message.name, context])
      sync.fetch(:release).pop
    when "deadline"
      # A controllable monotonic clock expires only once the handler has begun.
      request.get_header("connectrpc.test.expire").call
    end
    context.response_metadata.add("x-leading", "one")
    context.response_metadata.add("x-leading", "two")
    context.response_trailers.add("x-trailing", "three")
    message
  end

  def rpc_fail(message, _context)
    detail = ConnectRPC::ErrorDetail.new(type: "example.v1.EchoMessage", value: message.to_proto)
    raise ConnectRPC::Error.new(
      :invalid_argument, "safe failure", details: [detail],
                                         metadata: { "x-error" => %w[one two] }, trailers: { "x-end" => %w[three four] }
    )
  end
end
