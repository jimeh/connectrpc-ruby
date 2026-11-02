# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/rotation_examples"
require_relative "../../connectrpc-async/spec/support/wire_drain_examples"
require_relative "../../connectrpc-async/spec/support/frame_gate"
require_relative "../../connectrpc-async/spec/support/gzip_examples"
require_relative "../../connectrpc-async/spec/support/deadline_examples"
require_relative "../../connectrpc-async/spec/support/reset_examples"

# Test-only caller facade. All protocol/lifecycle behavior comes from the real
# Falcon Server and shared session; observations do not substitute verification.
class FalconTLSFixture < ConnectRPC::Falcon::Server
  attr_reader :setup_errors

  def initialize(tls: nil, **)
    scheme = tls ? "https" : "http"
    @bound = ConnectRPC::Falcon::Endpoint.parse("#{scheme}://127.0.0.1:0").bound
    @setup_errors = []
    super(endpoint: @bound, tls: tls, **)
  end

  def start
    run
    self
  end

  def port = @bound.sockets.first.local_address.ip_port

  def close(**options)
    super
  ensure
    @bound.close
  end
end

RSpec.describe "Falcon shared reviewed HTTP/2 TLS and wire lifecycle" do
  def rotation_host(**)
    host = FalconTLSFixture.new(**)
    session = rotation_session(host)
    allow(session).to receive(:setup_connection).and_wrap_original do |original, *arguments|
      original.call(*arguments)
    rescue OpenSSL::SSL::SSLError => e
      host.setup_errors << e.message
      raise
    end
    host.start
  end

  def rotation_session(host) = host.instance_variable_get(:@session)
  def wire_host(**) = FalconTLSFixture.new(**).start
  def wire_session(host) = host.instance_variable_get(:@session)

  def gzip_host(**) = FalconTLSFixture.new(**).start
  def deadline_host(**) = FalconTLSFixture.new(**).start
  def cleanup_host(**) = FalconTLSFixture.new(**).start
  def cleanup_session(host) = host.instance_variable_get(:@session)

  include_examples "HTTP/2 TLS retirement and session isolation"
  include_examples "HTTP/2 wire drain ownership"
  include_examples "HTTP/2 gzip core integration"
  include_examples "HTTP/2 deadlines and stream isolation"
  include_examples "HTTP/2 peer reset during headers"
end
