# frozen_string_literal: true

module AsyncHostingHelpers
  def host_session(host) = host.instance_variable_get(:@session)

  def rotation_host(**) = TLSObservedHost.new(**).start
  def rotation_session(host) = host_session(host)
  def wire_host(**) = ConnectRPC::Async::Host.new(**).start
  def wire_session(host) = host_session(host)

  def gzip_host(**) = ConnectRPC::Async::Host.new(**).start
  def deadline_host(**) = ConnectRPC::Async::Host.new(**).start
  def cleanup_host(**) = ConnectRPC::Async::Host.new(**).start
  def cleanup_session(host) = host_session(host)
end
