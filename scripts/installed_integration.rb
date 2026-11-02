# frozen_string_literal: true

# Copied beside localhost.rb into an external cwd by check_package.rb.
require "connectrpc/bindings/google_protobuf"
require "google/protobuf/wrappers_pb"
require "connectrpc/transports/net_http"
require_relative "localhost"
require "rack/lint"

type = Google::Protobuf::StringValue
method = ConnectRPC::Method.new(service: "installed.Echo", name: "Call", request_type: type, response_type: type)
service = ConnectRPC::Service.new(name: method.service, methods: [method])
binding = ConnectRPC::Bindings::GoogleProtobuf.new
handler = lambda do |request, context|
  if request.value == "fail"
    detail = ConnectRPC::ErrorDetail.new(type: "google.protobuf.StringValue", value: request.to_proto)
    raise ConnectRPC::Error.new(:invalid_argument, "safe", details: [detail], trailers: { "x-error" => ["installed"] })
  end
  context.response_metadata["x-leading"] = %w[one two]
  context.response_trailers.add("x-installed", "yes")
  type.new(value: "installed #{request.value}")
end
server = ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => handler },
                                encoding: :gzip)
host = EchoExample::Localhost.new(Rack::Lint.new(ConnectRPC::Rack.new(server: server)))
begin
  observed = []
  http = ConnectRPC::Transports::NetHTTP.new
  transport = lambda do |request|
    response = http.call(request)
    observed << [request, response]
    response
  end
  %i[proto json].product(%i[identity gzip]).each do |codec, encoding|
    client = ConnectRPC::Client.new(base_url: host.url, binding: binding, codec: codec, encoding: encoding,
                                    transport: transport)
    result = client.call(method, type.new(value: "typed"), timeout: 2)
    abort "Installed typed integration failed" unless result.message.value == "installed typed"
    abort "Installed metadata failed" unless result.metadata["x-leading"] == ["one, two"]
    abort "Installed trailers failed" unless result.trailers["x-installed"] == ["yes"]
    request, response = observed.last
    if encoding == :gzip
      unless request.headers["content-encoding"] == ["gzip"] && response.headers["content-encoding"] == ["gzip"] &&
             !request.body.empty? && !response.body.empty?
        abort "Installed gzip wire missing"
      end
      decoded = binding.decode(Zlib.gunzip(response.body), type: type, codec: codec)
      abort "Installed gzip wire payload changed" unless decoded.value == "installed typed"
    end

    begin
      client.call(method, type.new(value: "fail"), timeout: 2)
      abort "Installed deliberate error missing"
    rescue ConnectRPC::Error => e
      abort "Installed error code/message failed" unless e.code == "invalid_argument" && e.message == "safe"
      abort "Installed error trailer failed" unless e.trailers["x-error"] == ["installed"]
      decoded = e.details.first.decode(binding: binding, type: type)
      abort "Installed typed error detail failed" unless decoded.value == "fail"
    end
    puts "Installed typed #{codec}/#{encoding} Rack/NetHTTP success/error/detail passed (Ruby #{RUBY_VERSION})"
  end
  ["{", '{"code":null}', '{"code":"internal","details":[null]}',
   "{\"code\":\"internal\",\"message\":\"\xff\"}".b].each do |body|
    error = ConnectRPC::Protocol.parse_error(503, { "content-type" => ["application/json"] }, body)
    abort "Default JSON malformed error fallback failed" unless error.code == "unavailable"
  end
  core = Gem.loaded_specs.fetch("connectrpc")
  middleware = Gem.loaded_specs.fetch("connectrpc-rack")
  abort "Core ships middleware" if File.exist?(File.join(core.full_gem_path, "lib/connectrpc/rack.rb"))
  source = File.join(middleware.full_gem_path, "lib/connectrpc/rack.rb")
  abort "Wrong middleware source owner" unless ConnectRPC::Rack.instance_method(:call).source_location.first == source
  abort "Duplicate middleware require owner" unless $LOADED_FEATURES.grep(%r{/connectrpc/rack\.rb$}) == [source]
  core_source = ConnectRPC::Server.instance_method(:call).source_location.first
  abort "Wrong core source owner" unless core_source == File.join(core.full_gem_path, "lib/connectrpc/server.rb")
  puts "Installed core/Rack source ownership passed"
  puts "Default libraries: JSON #{Gem.loaded_specs.fetch('json').version}, Net::HTTP #{Net::HTTP::VERSION}, Zlib #{Zlib::VERSION}"
  %w[connectrpc connectrpc-rack google-protobuf rack rackup webrick bigdecimal rake].each do |name|
    installed = Gem.loaded_specs.fetch(name)
    unless installed.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
      abort "#{name} loaded outside isolated GEM_HOME"
    end
  end
ensure
  host.close
end
