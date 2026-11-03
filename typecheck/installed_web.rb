# frozen_string_literal: true

# Runs in both fresh-GEM_HOME load orders with ordinary discovered packages.
codec = ConnectRPC::Bindings::GoogleProtobuf.new
declaration = ConnectRPC::Method.new(service: "installed.Web", name: "Call", request_type: String,
                                     response_type: String)
target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: declaration.service, methods: [declaration])],
                                binding: InstalledBinding.new, status_codec: codec, protocol: %i[connect grpc_web],
                                handlers: { declaration.path => ->(message, _) { message } })
wire = "#{[0, 9].pack('CN')}installed"
%w[application/grpc-web application/grpc-web-text].each do |media|
  text = media.end_with?("-text")
  request = ConnectRPC::TransportRequest.new(url: URI("http://in-process.invalid#{declaration.path}"),
                                             http_method: "POST", headers: { "content-type" => [media] },
                                             body: text ? [wire].pack("m0") : wire, response_limit: 1024, deadline: nil)
  response = ConnectRPC::Testing::Transport.new(server: target).call(request)
  body = text ? response.body.scan(/.{4}/).map { |group| group.unpack1("m0") }.join : response.body
  raise "Installed manual Web Testing seam" unless body == "#{wire}#{[128, 16].pack('CN')}grpc-status: 0\r\n"
end
Async do
  host = ConnectRPC::Async::Host.new(server: target).start
  origin = "http://127.0.0.1:#{host.port}"
  transport = ConnectRPC::Async::Transport.new(origin: origin)
  request = ConnectRPC::TransportRequest.new(url: URI("#{origin}#{declaration.path}"), http_method: "POST",
                                             headers: { "content-type" => ["application/grpc-web"] },
                                             body: wire, response_limit: 1024, deadline: nil)
  response = transport.call(request)
  raise "Installed Web network response" unless response.status == 200 && response.body.start_with?(wire) &&
                                                response.body.end_with?("grpc-status: 0\r\n")

  connect = ConnectRPC::Client.new(base_url: origin, transport: transport, binding: InstalledBinding.new)
  raise "Installed mixed Web/Connect network response" unless connect.call(declaration, "mixed").message == "mixed"
ensure
  transport&.close(grace: 0)
  host&.close(grace: 0)
end.wait
raise "Installed Web runtime loaded SDK" if defined?(OpenTelemetry::SDK)

puts "Installed Web: custom tokens/separate status codec, manual encoded Testing seam, mixed real H2 calls, no SDK"
