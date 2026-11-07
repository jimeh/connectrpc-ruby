# frozen_string_literal: true

$LOAD_PATH.unshift(File.join(__dir__, "generated"))
require "connectrpc/bindings/google_protobuf"
require "streaming_connect"

service = RbsFixture::Streaming::Mixed
request_type = RbsFixture::Messages::Envelope::Request
reply_type = RbsFixture::Messages::Envelope::Reply
binding = ConnectRPC::Bindings::GoogleProtobuf.new
handlers = service.handlers(
  lookup: ->(request, _) { reply_type.new(count: request.text.size) },
  watch: lambda { |request, writer, context|
    context.send_headers(metadata: { "x-installed" => "early" })
    2.times { writer.send_message(reply_type.new(count: request.text.size)) }
  },
  collect: lambda { |reader, _|
    size = 0
    reader.each { |request| size += request.text.size }
    reply_type.new(count: size)
  },
  chat: ->(duplex, _) { duplex.each { |request| duplex.send_message(reply_type.new(count: request.text.size)) } }
)
class InstalledStreamingNetwork
  def initialize(service, binding, handlers, request_type)
    @service = service
    @binding = binding
    @handlers = handlers
    @request_type = request_type
  end

  def run(backend, protocol = :connect) # rubocop:disable Metrics/MethodLength
    server = ConnectRPC::Server.new(services: [@service::SERVICE], binding: @binding, handlers: @handlers,
                                    encoding: :gzip, protocol: protocol)
    host, origin, bound = make_host(backend, server)
    transport_class = backend == :falcon_http1 ? ConnectRPC::Async::HTTP1Transport : ConnectRPC::Async::Transport
    transport = transport_class.new(origin: origin)
    codecs = protocol == :grpc ? [:proto] : %i[proto json]
    codecs.product(%i[identity gzip]).each do |codec, coding|
      probe(transport, origin, backend, codec, coding, protocol)
    end
  ensure
    transport&.close(grace: 0)
    host&.close(grace: 0)
    bound&.close
  end

  private

  def make_host(backend, server)
    return make_falcon(server) if %i[falcon falcon_http1].include?(backend)

    host = ConnectRPC::Async::Host.new(server: server).start
    [host, "http://127.0.0.1:#{host.port}", nil]
  end

  def make_falcon(server)
    bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
    host = ConnectRPC::Falcon::Server.new(server: server, endpoint: bound)
    host.run
    [host, "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}", bound]
  end

  def probe(transport, origin, backend, codec, coding, protocol) # rubocop:disable Metrics/ParameterLists, Metrics/MethodLength
    core = ConnectRPC::Client.new(base_url: origin, binding: @binding, transport: transport,
                                  use_get: protocol == :connect, protocol: protocol,
                                  codec: codec, encoding: coding)
    typed = @service::Client.new(client: core)
    request = @request_type.new(text: "installed")
    check(typed.lookup(request, timeout: nil).message.count == 9, "mixed GET")
    watch(typed, request)
    collect(typed, request)
    chat(typed, request) unless backend == :falcon_http1
    puts "PASS installed generated streaming #{backend}/#{protocol}/#{codec}/#{coding}: " \
         "mixed GET/POST SS/CS#{'/bidi' unless backend == :falcon_http1} nil deadlines"
  end

  def watch(typed, request)
    replies = []
    typed.watch(request, timeout: nil) do |reader|
      check(reader.headers["x-installed"] == ["early"], "early metadata")
      reader.each { |reply| replies << reply.count }
    end
    check(replies == [9, 9], "typed SS")
  end

  def collect(typed, request)
    response = typed.collect(timeout: nil) { |writer| writer.send_message(request) }
    check(response.message.count == 9, "typed CS")
  end

  def chat(typed, request)
    typed.chat(timeout: nil) do |duplex|
      duplex.send_message(request)
      check(duplex.receive.count == 9, "bidi incrementality")
      duplex.close_send
      check(duplex.receive.nil?, "bidi completion")
    end
  end

  def check(condition, message)
    raise "Installed streaming #{message} failed" unless condition
  end
end
network = InstalledStreamingNetwork.new(service, binding, handlers, request_type)
Async do
  %i[async falcon].product(%i[connect grpc]).each { |backend, protocol| network.run(backend, protocol) }
  network.run(:falcon_http1)
end.wait
custom = ConnectRPC::Client.new(base_url: "https://installed.invalid", binding: InstalledBinding.new,
                                transport: APIStreamingTransport.new)
method = ConnectRPC::ServerStreamMethod.new(service: "installed.Custom", name: "Watch",
                                            request_type: String, response_type: String)
custom.server_stream(method, "q") do |reader|
  raise "Installed custom typed SPI payload" unless reader.receive == "typed" && reader.receive.nil?
end
raise "SDK runtime dependency leaked" if defined?(OpenTelemetry::SDK)

Gem.loaded_specs.each_value do |spec|
  next if spec.default_gem?

  unless spec.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
    raise "Streaming library escaped installation: #{spec.name}"
  end
end
puts "Installed seven-package composition: generated streaming network and custom SPI passed with no SDK"

# Installed custom-host evidence exercises the explicit capability SPI without
# relying on a backend, its private implementation, or checkout load paths.
input_type = Class.new(StringIO) do
  def read(length, timeout: nil) # rubocop:disable Lint/UnusedMethodArgument -- structural host SPI
    super(length)
  end
end
output_type = Class.new do
  attr_reader :bytes

  def initialize = @bytes = +"".b

  def write(bytes, timeout: nil) # rubocop:disable Lint/UnusedMethodArgument -- structural host SPI
    @bytes << bytes
  end

  def abort = nil
end

ss = ConnectRPC::ServerStreamMethod.new(service: "installed.Host", name: "Watch", request_type: String,
                                        response_type: String)
cs = ConnectRPC::ClientStreamMethod.new(service: "installed.Host", name: "Collect", request_type: String,
                                        response_type: String)
bidi = ConnectRPC::BidiStreamMethod.new(service: "installed.Host", name: "Chat", request_type: String,
                                        response_type: String)
custom = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: ss.service, methods: [ss, cs, bidi])],
                                binding: InstalledBinding.new,
                                handlers: { ss.path => ->(message, writer, _) { writer.send_message(message) },
                                            cs.path => ->(reader, _) { reader.receive } })
[ss, cs].each do |rpc|
  output = output_type.new
  published = []
  payload = "installed host"
  custom.call_stream(path: rpc.path, http_method: "POST", headers: { "content-type" => ["application/connect+proto"] },
                     input: input_type.new([0, payload.bytesize].pack("CN") + payload), output: output,
                     publish: ->(head) { published << head }, capabilities: %i[server_stream client_stream])
  unless published.one? && published.first.status == 200 && output.bytes.include?(payload)
    raise "Installed explicit #{rpc.cardinality} host failed: #{output.bytes.inspect}"
  end
end
response = custom.call_stream(path: bidi.path, http_method: "GET", headers: {}, input: StringIO.new,
                              output: output_type.new, publish: ->(_) { raise "Unexpected publication" },
                              capabilities: [:server_stream])
raise "Host-neutral bidi rejection changed" unless response.status == 505 && !response.headers.key?("connection")

[[], [:unary], %i[server_stream server_stream], false].each do |invalid|
  begin
    custom.call_stream(path: ss.path, http_method: "POST", headers: {}, input: StringIO.new,
                       output: output_type.new, publish: ->(_) {}, capabilities: invalid)
  rescue ArgumentError => e
    raise "Unexpected installed capability error" unless e.message == "Invalid streaming capabilities"

    next
  end
  raise "Invalid installed capability accepted"
end
puts "PASS installed explicit SS/CS host capabilities, host-neutral bidi 505 and invalid capabilities"
