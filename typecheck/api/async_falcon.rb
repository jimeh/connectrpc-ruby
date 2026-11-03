# frozen_string_literal: true

server = ConnectRPC::Server.new(services: [], binding: APIBinding.new)
provider = APIProvider.new
host = ConnectRPC::Async::Host.new(server: server, tls_provider: provider, on_tls_error: APIReporter.new,
                                   idle_timeout: nil, refresh_jitter: 0.1)
host.start.port&.succ
host.addresses.each(&:ip_port)
host.refresh_tls&.succ
host.reload_tls(certificate_chain: ["leaf"], private_key: "key", client_ca: nil)
host.tls_status[:source_check_age]&.round
host.tls_status[:publication_age]&.round
host.peer_certificate&.to_der
host.drain_tls(grace: 0)
host.close(grace: 5)
configuration = ConnectRPC::Async::TLSConfiguration.new(tls_provider: lambda {
  { version: "v1", certificate_chain: ["leaf"], private_key: "key", client_ca: ["ca"] }
}, on_error: ->(event) { event[:category].to_s })
configuration.start.generation&.succ
configuration.refresh&.succ
configuration.reload(certificate_chain: ["leaf"], private_key: "key", client_ca: nil)
configuration.status[:state].to_s
configuration.close
application = ConnectRPC::Async::Application.new(server: server)
request = APIRequest.new
native_response = application.call(request)
native_response&.body&.read
session = ConnectRPC::Async::ServerSession.new(
  application: application, lifecycle_factory: ->(incoming) { application.start_call(incoming) },
  stop_accepting: -> {}
)
session.start.schedule { application.call(request) }.wait
session.tls_status[:generation]&.succ
session.refresh_tls
session.drain_tls
session.close
session.abort
transport = ConnectRPC::Async::Transport.new(
  origin: "https://example.invalid", ca_file: "application-ca.pem", certificate_chain: ["client"],
  private_key: "key", connection_limit: 2, max_requests: 10
)
ConnectRPC::Client.new(base_url: "https://example.invalid", binding: APIBinding.new, transport: transport)
transport.close(grace: 0)
endpoint = ConnectRPC::Falcon::Endpoint.parse("https://127.0.0.1:9292")
endpoint.scheme.size
# @type var rack_app: ConnectRPC::Rack::_App
rack_app = APIApp.new
falcon = ConnectRPC::Falcon::Server.new(
  server: server, endpoint: endpoint, tls_provider: provider, rack_app: rack_app,
  on_tls_error: ->(event) { event[:status][:category]&.to_s }
)
falcon.run.wait
falcon.call(request)&.status&.succ
falcon.tls_status[:publication_age]&.round
falcon.tls_generation&.succ
falcon.reload_tls(certificate_chain: ["leaf"], private_key: "key", client_ca: nil)
falcon.refresh_tls
falcon.drain_tls(grace: 0)
falcon.peer_certificate&.to_der
falcon.close(grace: 5)
falcon.abort.tls_status
APIEnvironment.new.make_server(endpoint)
