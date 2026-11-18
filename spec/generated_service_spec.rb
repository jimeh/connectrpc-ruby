# frozen_string_literal: true

require "open3"
require "rbconfig"
require "connectrpc/bindings/google_protobuf"
require_relative "../examples/localhost"

$LOAD_PATH.unshift(File.expand_path("fixtures/generation/ruby", __dir__))
require "generation/service_connect"
require "generation/dotted_connect"
require "generation/google_connect"
require "generation/google_ruby_connect"
require "generation/builtin_root_connect"
require "generation/builtin_direct_connect"
require "generation_empty_connect"
require "generation/streaming_connect"

RSpec.describe "Generated service integration" do
  let(:service) { GenerationFixture::V1::EchoService }
  let(:type) { GenerationFixture::V1::Message }
  let(:binding) { ConnectRPC::Bindings::GoogleProtobuf.new }

  it "retains schema safety without inferring GET safety from names" do
    service = GenerationFixture::V1::EchoService
    expect(service::ECHO.idempotency).to eq(:no_side_effects)
    expect(service::FAIL.idempotency).to eq(:idempotent)
    expect(service::GET_HTTP.idempotency).to eq(:unknown)
  end

  it "uses concrete official message classes and frozen reusable declarations" do
    expect(service::ECHO.request_type).to equal(type)
    expect(service::ECHO.response_type).to equal(type)
    expect(service::GET_HTTP.request_type).to equal(GenerationFixture::Types::Outer_name::PB__value)
    expect(service::SERVICE.rpcs).to eq([service::ECHO, service::FAIL, service::GET_HTTP])
    expect(service::SERVICE).to be_frozen
    expect(service::ECHO).to be_frozen
    expect(service::ECHO.path).to eq("/generation_fixture.v1.EchoService/Echo")
    expect(GenerationFixture::V1::ConnectRPC.new.value).to eq("")
  end

  it "loads official Google package and ruby_package messages with identical WKT types" do
    google_service = Google::GenerationFixture::V1::EchoService
    google_type = Google::GenerationFixture::V1::Message
    expect(google_service::ECHO.request_type).to equal(google_type)
    expect(google_service::ECHO.response_type).to equal(google_type)
    expect(google_service::TIME.request_type).to equal(Google::Protobuf::Timestamp)
    expect(google_service::TIME.response_type).to equal(Google::Protobuf::Timestamp)
    mapped_service = Google::GenerationFixture::MappedV1::EchoService
    mapped_type = Google::GenerationFixture::MappedV1::Message
    expect(mapped_service::ECHO.request_type).to equal(mapped_type)
    expect(mapped_service::ECHO.response_type).to equal(mapped_type)
    %i[proto json].each do |codec|
      message = google_type.new(time: Google::Protobuf::Timestamp.new(seconds: 123, nanos: 456))
      decoded = binding.decode(binding.encode(message, type: google_type, codec: codec), type: google_type,
                                                                                         codec: codec)
      expect(decoded.class).to equal(google_type)
      expect(decoded).to eq(message)
    end
  end

  it "reopens the builtin Math module for official nested and direct-child messages" do
    expect(Math::PI).to be_a(Float)
    expect(Math::DomainError).to be < StandardError
    { Math::GenerationFixture::V1::EchoService => Math::GenerationFixture::V1::Message,
      Math::GenerationFixturePlanService => Math::GenerationFixturePlan }.each do |builtin_service, builtin_type|
      expect(builtin_service::ECHO.request_type).to equal(builtin_type)
      expect(builtin_service::ECHO.response_type).to equal(builtin_type)
      expect(builtin_service::SERVICE.rpcs).to eq([builtin_service::ECHO])
      %i[proto json].each do |codec|
        message = builtin_type.new(count: 7)
        decoded = binding.decode(binding.encode(message, type: builtin_type, codec: codec), type: builtin_type,
                                                                                            codec: codec)
        expect(decoded.class).to equal(builtin_type)
        expect(decoded).to eq(message)
      end
    end
    expect(Math::GenerationFixture::V1::EchoService::ECHO.path).to eq("/math.generation_fixture.v1.EchoService/Echo")
    expect(Math::GenerationFixturePlanService::ECHO.path).to eq("/math.GenerationFixturePlanService/Echo")
  end

  it "forwards omitted, nil, finite, metadata and arbitrary options with return identity" do
    calls = []
    result = Object.new
    recorder = Object.new
    recorder.define_singleton_method(:call) do |method, request, **options|
      calls << [method, request, options]
      result
    end
    client = service::Client.new(client: recorder)
    request = type.new(count: 0)
    [{}, { timeout: nil }, { timeout: 2, metadata: { "x-test" => ["yes"] } }, { custom: true }].each do |options|
      expect(client.echo(request, **options)).to equal(result)
      expect(calls.last).to eq([service::ECHO, request, options])
    end
  end

  it "requires every handler and rejects noncallable values with the keyword name" do
    handler = ->(request, _context) { request }
    expect(service.handlers(echo: handler, fail: handler, get_http: handler)).to eq(
      service::ECHO.path => handler, service::FAIL.path => handler, service::GET_HTTP.path => handler
    )
    expect { service.handlers(echo: handler) }.to raise_error(ArgumentError, /missing keywords/)
    expect do
      service.handlers(echo: handler, fail: false,
                       get_http: handler)
    end.to raise_error(ArgumentError, /fail must respond to call/)
    expect(GenerationFixture::V1::EmptyService::SERVICE.rpcs).to be_empty
    expect(GenerationFixture::V1::EmptyService.handlers).to eq({})
  end

  it "retains Client option validation, timeout defaults and custom transport/limits" do
    requests = []
    transport = Object.new
    transport.define_singleton_method(:call) do |request|
      requests << request
      ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                        body: request.body)
    end
    core = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport,
                                  request_limit: 10, response_limit: 10)
    client = service::Client.new(client: core)
    client.echo(type.new(count: 0))
    expect(requests.last.deadline).to be_a(ConnectRPC::Deadline)
    expect(requests.last.headers.fetch("connect-timeout-ms").first.to_i).to be_between(29_000, 30_000)
    client.echo(type.new, timeout: nil)
    expect(requests.last.deadline).to be_nil
    expect(requests.last.headers).not_to have_key("connect-timeout-ms")
    expect { client.echo(type.new, unknown: true) }.to raise_error(ArgumentError, /unknown keyword/)
    expect { client.echo(GenerationFixture::V1::ConnectRPC.new) }.to raise_error(ConnectRPC::Error, /Cannot encode/)
    sent = requests.size
    expect { client.echo(type.new(count: 0), timeout: 0) }.to raise_error(ConnectRPC::Error) { |error|
      expect(error.code).to eq("deadline_exceeded")
    }
    expect(requests.size).to eq(sent)
  end

  it "defaults Client, Server and ErrorDetail to the Google binding the generated file loads" do
    echo = ->(request, _context) { request }
    failure = lambda do |request, _context|
      detail = ConnectRPC::ErrorDetail.new(type: type.descriptor.name, value: request.to_proto)
      raise ConnectRPC::Error.new(:invalid_argument, "safe", details: [detail])
    end
    handlers = service.handlers(echo: echo, fail: failure, get_http: echo)
    server = ConnectRPC::Server.new(services: [service::SERVICE], handlers: handlers)
    transport = lambda do |request|
      server.call(path: request.url.path, http_method: request.http_method, headers: request.headers,
                  input: StringIO.new(request.body))
    end
    client = service::Client.new(client: ConnectRPC::Client.new(base_url: "http://example.invalid",
                                                                transport: transport))
    expect(client.echo(type.new(count: 3)).message).to eq(type.new(count: 3))
    expect { client.fail(type.new(count: 4)) }.to raise_error(ConnectRPC::Error) { |error|
      expect(error.details.first.decode(type: type)).to eq(type.new(count: 4))
    }
    grpc = ConnectRPC::Server.new(services: [service::SERVICE], handlers: handlers, protocol: :grpc)
    finished = {}
    output = Object.new
    output.define_singleton_method(:write) { |*, **| nil }
    output.define_singleton_method(:finish) { |headers:| finished.merge!(headers) }
    output.define_singleton_method(:abort) { nil }
    body = type.new(count: 5).to_proto
    input = StringIO.new([0, body.bytesize].pack("CN") + body)
    input.define_singleton_method(:read) { |length, **| super(length) }
    grpc.call_grpc(path: service::FAIL.path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                   input: input, output: output, publish: ->(_) {}, http2: true)
    expect(finished["grpc-status"]).to eq(["3"])
    expect(finished["grpc-status-details-bin"]).not_to be_nil
    expect do
      ConnectRPC::Client.new(base_url: "http://example.invalid", protocol: :grpc, transport: transport)
    end.not_to raise_error
  end

  %i[proto json].each do |codec|
    it "runs typed #{codec} Rack/NetHTTP success, presence and deliberate error details" do
      handler = lambda do |request, context|
        context.response_metadata.add("x-request", context.request_metadata["x-request"].first)
        context.response_trailers.add("x-tail", "done")
        request
      end
      failure = lambda do |request, _context|
        detail = ConnectRPC::ErrorDetail.new(type: type.descriptor.name, value: request.to_proto)
        raise ConnectRPC::Error.new(:invalid_argument, "safe", details: [detail], metadata: { "x-error" => ["yes"] },
                                                               trailers: { "x-end" => ["yes"] })
      end
      server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding,
                                      handlers: service.handlers(echo: handler, fail: failure, get_http: handler))
      host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: server))
      begin
        client = service::Client.new(client: ConnectRPC::Client.new(base_url: host.url, binding: binding, codec: codec))
        [type.new, type.new(count: 0)].each do |message|
          response = client.echo(message, metadata: { "x-request" => ["generated"] }, timeout: 2)
          expect(response.message).to eq(message)
          expect(response.message.has_count?).to eq(message.has_count?)
          expect(response.metadata["x-request"]).to eq(["generated"])
          expect(response.trailers["x-tail"]).to eq(["done"])
        end
        expect { client.fail(type.new(count: 42), timeout: 2) }.to raise_error(ConnectRPC::Error) { |error|
          expect([error.code, error.message]).to eq(%w[invalid_argument safe])
          expect(error.details.first.decode(binding: binding, type: type)).to eq(type.new(count: 42))
          expect(error.metadata["x-error"]).to eq(["yes"])
          expect(error.trailers["x-end"]).to eq(["yes"])
        }
      ensure
        host.close
      end
    end
  end

  it "preserves official naming for empty, dotted and explicit Ruby packages" do
    expect(PB__generation_service::CALL.request_type).to equal(Generation_message)
    expect(GenerationFixture::DottedName::Service_name::GET_FOO.request_type).to equal(GenerationFixture::DottedName::Message_name)
  end

  it "allows repeated require but fails clearly on deliberate redefinition" do
    expect(require("generation/service_connect")).to be(false)
    expect { load File.expand_path("fixtures/generation/ruby/generation/service_connect.rb", __dir__) }
      .to raise_error(ArgumentError, /Generated service constant already defined/)
  end

  it "guards out-of-request Client constants before reopening their classes" do
    code = <<~CODE
      require "generation/service_pb"
      module GenerationFixture::V1::EchoService
        class Client; end
      end
      require "generation/service_connect"
    CODE
    _output, error, status = Open3.capture3(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__),
                                            "-I", File.expand_path("fixtures/generation/ruby", __dir__), "-e", code)
    expect(status.success?).to be(false)
    expect(error).to include("Generated service constant already defined", "Client")
  end
end

RSpec.describe "Generated service-object handlers" do
  let(:mixed) { GenerationFixture::Streaming::Mixed }
  let(:item) { GenerationFixture::Streaming::Item }
  let(:implementation) do
    item = self.item
    Class.new do
      define_method(:lookup) { |request, _context| item.new(name: "lookup #{request.name}") }
      define_method(:watch) do |request, writer, _context|
        2.times { |index| writer.send_message(item.new(name: "#{request.name} #{index}")) }
      end
      define_method(:collect) do |reader, _context|
        names = []
        while (message = reader.receive)
          names << message.name
        end
        item.new(name: names.join(","))
      end
      define_method(:chat) do |duplex, _context|
        duplex.each { |message| duplex.send_message(item.new(name: "echo #{message.name}")) }
      end
    end
  end

  def frame(bytes, flag = 0) = [flag, bytes.bytesize].pack("CN") + bytes

  def frames(bytes)
    offset = 0
    parsed = []
    while offset < bytes.bytesize
      flag, length = bytes.byteslice(offset, 5).unpack("CN")
      parsed << [flag, bytes.byteslice(offset + 5, length)]
      offset += 5 + length
    end
    parsed
  end

  def native_input(bytes)
    StringIO.new(bytes).tap { |source| source.define_singleton_method(:read) { |length, **| super(length) } }
  end

  def native_output(written, finished = {})
    Object.new.tap do |output|
      output.define_singleton_method(:write) { |chunk, **| written << chunk }
      output.define_singleton_method(:finish) { |headers:| finished.merge!(headers) }
      output.define_singleton_method(:abort) { nil }
    end
  end

  # Omitting binding: also exercises the generated file's default binding.
  def mixed_server(handlers)
    ConnectRPC::Server.new(services: [mixed::SERVICE], handlers: handlers, protocol: %i[connect grpc grpc_web])
  end

  def connect_stream(server, rpc, *messages)
    written = +""
    server.call_stream(path: rpc.path, http_method: "POST",
                       headers: { "content-type" => ["application/connect+proto"] },
                       input: native_input(messages.map { |message| frame(message.to_proto) }.join),
                       output: native_output(written), publish: ->(_) {}, http2: true)
    parsed = frames(written)
    [parsed.reject { |flag, _| flag.anybits?(2) }.map { |_, bytes| item.decode(bytes).name },
     JSON.parse(parsed.find { |flag, _| flag.anybits?(2) }.last)]
  end

  it "maps every shape to the object's public methods and serves them through a real Server" do
    impl = implementation.new
    handlers = mixed.handlers_for(impl, strict: true)
    expect(handlers.keys).to eq(mixed::SERVICE.rpcs.map(&:path))
    expect(handlers.values).to all(be_a(Method))
    expect(handlers.values.map(&:receiver)).to all(equal(impl))
    server = mixed_server(handlers)
    response = server.call(path: mixed::LOOKUP.path, http_method: "POST",
                           headers: { "content-type" => ["application/proto"] },
                           input: StringIO.new(item.new(name: "a").to_proto))
    expect([response.status, item.decode(response.body).name]).to eq([200, "lookup a"])
    expect(connect_stream(server, mixed::WATCH, item.new(name: "w")).first).to eq(["w 0", "w 1"])
    expect(connect_stream(server, mixed::COLLECT, item.new(name: "x"), item.new(name: "y")).first).to eq(["x,y"])
    expect(connect_stream(server, mixed::CHAT, item.new(name: "p"), item.new(name: "q")).first)
      .to eq(["echo p", "echo q"])
  end

  it "omits missing methods so the default invoker answers unimplemented on every route" do
    impl = Object.new
    impl.define_singleton_method(:watch) { |request, writer, _context| writer.send_message(request) }
    handlers = mixed.handlers_for(impl)
    expect(handlers.keys).to eq([mixed::WATCH.path])
    server = mixed_server(handlers)
    connect = server.call(path: mixed::LOOKUP.path, http_method: "POST",
                          headers: { "content-type" => ["application/proto"] },
                          input: StringIO.new(item.new(name: "a").to_proto))
    expect([connect.status, JSON.parse(connect.body)["code"]]).to eq([501, "unimplemented"])
    expect(connect_stream(server, mixed::COLLECT, item.new(name: "x")).last.dig("error", "code"))
      .to eq("unimplemented")
    [mixed::LOOKUP, mixed::COLLECT].each do |rpc|
      finished = {}
      server.call_grpc(path: rpc.path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                       input: native_input(frame(item.new(name: "g").to_proto)),
                       output: native_output(+"", finished), publish: ->(_) {}, http2: true)
      expect(finished["grpc-status"]).to eq(["12"])
    end
    web = server.call(path: mixed::LOOKUP.path, http_method: "POST",
                      headers: { "content-type" => ["application/grpc-web"] },
                      input: StringIO.new(frame(item.new(name: "web").to_proto)))
    expect(frames(web.body).last.last).to include("grpc-status: 12\r\n")
  end

  it "rejects a strict partial implementation naming every missing method, and non-boolean strict" do
    impl = Object.new
    impl.define_singleton_method(:watch) { |*| nil }
    impl.define_singleton_method(:collect) { |*| nil }
    expect { mixed.handlers_for(impl, strict: true) }.to raise_error(ArgumentError) { |error|
      expect(error.message).to include("lookup", "chat")
      expect(error.message).not_to include("watch", "collect")
    }
    [nil, 1, "true", :yes].each do |strict|
      expect { mixed.handlers_for(implementation.new, strict: strict) }.to raise_error(ArgumentError, /strict/)
    end
  end

  it "treats private methods as missing and honours respond_to_missing?" do
    hidden = implementation.new
    hidden.singleton_class.send(:private, :lookup)
    expect(mixed.handlers_for(hidden).keys).not_to include(mixed::LOOKUP.path)
    expect { mixed.handlers_for(hidden, strict: true) }.to raise_error(ArgumentError, /lookup/)
    item = self.item
    dynamic = Class.new do
      define_method(:respond_to_missing?) do |name, include_private = false|
        name == :lookup || super(name, include_private)
      end
      define_method(:method_missing) do |name, *args|
        name == :lookup ? item.new(name: "dynamic #{args.first.name}") : super(name, *args)
      end
    end.new
    handlers = mixed.handlers_for(dynamic)
    expect(handlers.keys).to eq([mixed::LOOKUP.path])
    response = mixed_server(handlers).call(path: mixed::LOOKUP.path, http_method: "POST",
                                           headers: { "content-type" => ["application/proto"] },
                                           input: StringIO.new(item.new(name: "d").to_proto))
    expect(item.decode(response.body).name).to eq("dynamic d")
  end

  describe "inherited core methods" do
    # Stand-ins for public methods that frameworks such as ActiveSupport add
    # to Object or Module; each is removed again after the example.
    def stub_core(owner, name)
      raise "#{owner}##{name} already exists" if owner.method_defined?(name) || owner.private_method_defined?(name)

      owner.define_method(name) { |*| raise "inherited #{name} must not be bound" }
      stubbed << [owner, name]
    end

    let(:stubbed) { [] }

    after { stubbed.each { |owner, name| owner.send(:remove_method, name) } }

    it "treats a method an object only inherits from Object as missing" do
      stub_core(Object, :lookup)
      impl = Object.new
      impl.define_singleton_method(:watch) { |request, writer, _context| writer.send_message(request) }
      expect(impl.respond_to?(:lookup)).to be(true)
      handlers = mixed.handlers_for(impl)
      expect(handlers.keys).to eq([mixed::WATCH.path])
      response = mixed_server(handlers).call(path: mixed::LOOKUP.path, http_method: "POST",
                                             headers: { "content-type" => ["application/proto"] },
                                             input: StringIO.new(item.new(name: "a").to_proto))
      expect([response.status, JSON.parse(response.body)["code"]]).to eq([501, "unimplemented"])
      expect { mixed.handlers_for(impl, strict: true) }.to raise_error(ArgumentError) { |error|
        expect(error.message).to include("lookup")
        expect(error.message).not_to include("watch")
      }
    end

    it "maps a Module's own singleton methods but not inherited Module or Class methods" do
      stub_core(Module, :watch)
      stub_core(Class, :collect)
      item = self.item
      service = Module.new do
        define_singleton_method(:lookup) { |request, _context| item.new(name: "module #{request.name}") }
      end
      handlers = mixed.handlers_for(service)
      expect(handlers.keys).to eq([mixed::LOOKUP.path])
      response = mixed_server(handlers).call(path: mixed::LOOKUP.path, http_method: "POST",
                                             headers: { "content-type" => ["application/proto"] },
                                             input: StringIO.new(item.new(name: "m").to_proto))
      expect(item.decode(response.body).name).to eq("module m")
      expect(mixed.handlers_for(Class.new).keys).to be_empty
      expect { mixed.handlers_for(Class.new, strict: true) }.to raise_error(ArgumentError, /watch, collect/)
    end

    it "maps a Class's own and inherited singleton methods but not Class or Object methods" do
      stub_core(Class, :watch)
      stub_core(Object, :collect)
      item = self.item
      base = Class.new do
        define_singleton_method(:lookup) { |request, _context| item.new(name: "class #{request.name}") }
      end
      service = Class.new(base)
      handlers = mixed.handlers_for(service)
      expect(handlers.keys).to eq([mixed::LOOKUP.path])
      expect(handlers.values.first.receiver).to equal(service)
      response = mixed_server(handlers).call(path: mixed::LOOKUP.path, http_method: "POST",
                                             headers: { "content-type" => ["application/proto"] },
                                             input: StringIO.new(item.new(name: "c").to_proto))
      expect(item.decode(response.body).name).to eq("class c")
      expect { mixed.handlers_for(service, strict: true) }.to raise_error(ArgumentError, /watch, collect, chat/)
    end

    it "does not bind Class or Module methods for a class that inherits directly from BasicObject" do
      stub_core(Class, :watch)
      stub_core(Module, :collect)
      item = self.item
      service = Class.new(BasicObject)
      service.define_singleton_method(:lookup) { |request, _context| item.new(name: "basic #{request.name}") }
      handlers = mixed.handlers_for(service)
      expect(handlers.keys).to eq([mixed::LOOKUP.path])
      expect(handlers.fetch(mixed::LOOKUP.path).call(item.new(name: "b"), nil).name).to eq("basic b")
      expect { mixed.handlers_for(service, strict: true) }
        .to raise_error(ArgumentError, "Missing service methods: watch, collect, chat")
    end

    it "does not bind methods from modules prepended to Object or Module" do
      # Object and Module can never un-prepend a module, so these anonymous
      # modules only hold their methods during the example; the after hook
      # removes them, leaving empty modules that cannot affect other examples.
      on_object = Module.new
      on_object.define_method(:lookup) { |*| raise "prepended lookup must not be bound" }
      on_module = Module.new
      on_module.define_method(:watch) { |*| raise "prepended watch must not be bound" }
      stubbed << [on_object, :lookup] << [on_module, :watch]
      Object.prepend(on_object)
      Module.prepend(on_module)

      impl = Object.new
      impl.define_singleton_method(:collect) { |_reader, _context| nil }
      expect(impl.respond_to?(:lookup)).to be(true)
      expect(mixed.handlers_for(impl).keys).to eq([mixed::COLLECT.path])
      expect { mixed.handlers_for(impl, strict: true) }
        .to raise_error(ArgumentError, "Missing service methods: lookup, watch, chat")

      service = Module.new
      service.define_singleton_method(:chat) { |_duplex, _context| nil }
      expect(service.respond_to?(:watch)).to be(true)
      expect(mixed.handlers_for(service).keys).to eq([mixed::CHAT.path])
      expect { mixed.handlers_for(service, strict: true) }
        .to raise_error(ArgumentError, "Missing service methods: lookup, watch, collect")
    end

    it "treats a handler mixin that is also mixed into Object as core" do
      # Accepted limitation: lookup cannot tell the implementation's own mixin
      # from the same module in Object's ancestors. Object can never un-include
      # a module, so the after hook removes the method instead, leaving an
      # empty module that cannot affect other examples.
      mixin = Module.new
      mixin.define_method(:lookup) { |*| raise "core mixin must not be bound" }
      stubbed << [mixin, :lookup]
      impl = Class.new { include mixin }.new
      Object.include(mixin)
      expect(mixed.handlers_for(impl)).to eq({})
      expect { mixed.handlers_for(impl, strict: true) }.to raise_error(ArgumentError, /lookup/)
    end
  end

  it "binds an object's class, mixin and singleton methods and a Module's own singleton methods" do
    mixin = Module.new do
      def watch(_request, _writer, _context) = nil
    end
    impl = Class.new do
      include mixin

      def lookup(_request, _context) = nil
    end.new
    impl.define_singleton_method(:collect) { |_reader, _context| nil }
    handlers = mixed.handlers_for(impl)
    expect(handlers.keys).to eq([mixed::LOOKUP, mixed::WATCH, mixed::COLLECT].map(&:path))
    expect(handlers.values.map(&:owner)).to eq([impl.class, mixin, impl.singleton_class])

    service = Module.new do
      def self.chat(_duplex, _context) = nil
    end
    expect(mixed.handlers_for(service).transform_values(&:receiver)).to eq(mixed::CHAT.path => service)
  end

  it "returns no handlers for an immediate or frozen implementation and strict names every RPC" do
    [1, :service, "frozen literal"].each do |impl|
      expect(impl).to be_frozen
      expect(mixed.handlers_for(impl)).to eq({})
      expect { mixed.handlers_for(impl, strict: true) }
        .to raise_error(ArgumentError, "Missing service methods: lookup, watch, collect, chat")
    end
  end

  it "returns an empty map for an empty service" do
    expect(GenerationFixture::V1::EmptyService.handlers_for(Object.new)).to eq({})
    expect(GenerationFixture::V1::EmptyService.handlers_for(Object.new, strict: true)).to eq({})
  end
end
