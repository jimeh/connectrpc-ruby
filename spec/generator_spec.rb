# frozen_string_literal: true

require "spec_helper"
require "open3"
require "rbconfig"
require "google/protobuf/plugin_pb"

RSpec.describe "Service generation" do
  def request_for(rpcs: ["GetHTTP"], service: "EchoService", package: "generation.v1")
    methods = rpcs.map do |name|
      Google::Protobuf::MethodDescriptorProto.new(name: name, input_type: ".#{package}.Message",
                                                  output_type: ".#{package}.Message")
    end
    file = Google::Protobuf::FileDescriptorProto.new(
      name: "sample.proto", package: package, syntax: "proto3",
      message_type: [Google::Protobuf::DescriptorProto.new(name: "Message")],
      service: [Google::Protobuf::ServiceDescriptorProto.new(name: service, method: methods)]
    )
    Google::Protobuf::Compiler::CodeGeneratorRequest.new(file_to_generate: [file.name], proto_file: [file])
  end

  def generate(request)
    require "connectrpc/generator"
    ConnectRPC::Generator.generate(request)
  end

  it "emits reusable unary metadata, acronym methods and thin forwarding" do
    response = generate(request_for)
    expect(response.error).to eq("")
    expect(response.file.map(&:name)).to eq(["sample_connect.rb"])
    content = response.file.first.content
    expect(content).to include('require "sample_pb"', "module EchoService", "GET_HTTP = ::ConnectRPC::Method.new",
                               "def get_http(request, **options)", "@client.call(GET_HTTP, request, **options)",
                               "def self.handlers(get_http:)", "SERVICE = ::ConnectRPC::Service.new",
                               'name: "generation.v1.EchoService", rpcs: [GET_HTTP]',
                               "def self.handlers_for(impl, strict: false)",
                               "rpcs = { get_http: GET_HTTP }",
                               "core = ::Class.ancestors",
                               "impl.respond_to?(name) && !core.include?(impl.public_method(name).owner)")
    requires = %(require "connectrpc"\nrequire "connectrpc/bindings/google_protobuf"\nrequire "sample_pb")
    expect(content).to include(requires)
    expect(content).not_to include("methods: [")
    expect(response.supported_features).to eq(1)
  end

  it "maps standard method options and rejects unknown idempotency enum values" do
    { IDEMPOTENCY_UNKNOWN: nil, NO_SIDE_EFFECTS: :no_side_effects, IDEMPOTENT: :idempotent }.each do |level, ruby|
      request = request_for
      method = request.proto_file.first.service.first["method"].first
      method.options = Google::Protobuf::MethodOptions.new(idempotency_level: level)
      result = generate(request)
      expect(result.error).to eq("")
      if ruby
        expect(result.file.first.content).to include("idempotency: :#{ruby}")
      else
        expect(result.file.first.content).not_to include("idempotency:")
      end
    end
    request = request_for
    request.proto_file.first.service.first["method"].first.options =
      Google::Protobuf::MethodOptions.new(idempotency_level: 99)
    result = generate(request)
    expect(result.error).to include("idempotency")
    expect(result.file).to be_empty
  end

  it "emits all streaming declarations and forwards required blocks/options" do
    [[true, false, "ClientStreamMethod", "client_stream", ""],
     [false, true, "ServerStreamMethod", "server_stream", "request, "],
     [true, true, "BidiStreamMethod", "bidi_stream", ""]].each do |cs, ss, type, operation, argument|
      request = request_for
      method = request.proto_file.first.service.first["method"].first
      method.client_streaming = cs
      method.server_streaming = ss
      response = generate(request)
      expect(response.error).to eq("")
      expect(response.file.first.content).to include("::ConnectRPC::#{type}.new",
                                                     "def get_http(#{argument}**options, &)",
                                                     "@client.#{operation}(GET_HTTP, #{argument}**options, &)")
    end
  end

  it "rejects names that normalize to the same Ruby method" do
    response = generate(request_for(rpcs: %w[GetHTTP GetHttp]))
    expect(response.error).to include("GetHTTP", "GetHttp", "collision")
    expect(response.file).to be_empty
  end

  it "rejects unknown plugin parameters" do
    request = request_for
    request.parameter = "unknown=true"
    expect(generate(request).error).to include("parameter")
  end

  it "uses fixed reserved sets covering inherited methods on the running Ruby" do
    generate(request_for)
    inherited = Object.public_instance_methods.map(&:to_s).grep(/\A[a-z_][a-z_0-9]*\z/)
    reserved = ConnectRPC::Generator::Names::CLIENT_RESERVED + ConnectRPC::Generator::Names::KEYWORDS
    expect(inherited - reserved).to be_empty
  end

  %w[_Ping End Begin Method Send Initialize Handlers Service Client class].each do |name|
    it "rejects unsupported/reserved RPC #{name}" do
      response = generate(request_for(rpcs: [name]))
      expect(response.error).to include(name)
      expect(response.file).to be_empty
    end
  end

  %w[
    InitializeCopy InitializeDup InitializeClone MethodMissing RespondToMissing
    SingletonMethodAdded SingletonMethodRemoved SingletonMethodUndefined
    ToStr ToAry ToHash ToProc ToIo ToPath ToInt ToSym ToRegexp
  ].each do |name|
    it "rejects object hook or implicit conversion RPC #{name}" do
      response = generate(request_for(rpcs: [name]))
      expect(response.error).to include("Reserved", name)
      expect(response.file).to be_empty
    end
  end

  it "ignores imported streaming services and only emits targets" do
    request = request_for
    imported = Google::Protobuf.deep_copy(request.proto_file.first)
    imported.name = "imported.proto"
    imported.package = "unrelated"
    imported.service.first["method"].first.client_streaming = true
    request.proto_file << imported
    response = generate(request)
    expect(response.error).to eq("")
    expect(response.file.map(&:name)).to eq(["sample_connect.rb"])
  end

  it "generates empty service declarations and handlers without messages" do
    request = request_for(rpcs: [])
    response = generate(request)
    expect(response.error).to eq("")
    expect(response.file.first.content).to include("rpcs: []", "def self.handlers\n", "{}",
                                                   "def self.handlers_for(_impl, strict: false)")
    request.proto_file.first.service.clear
    expect(generate(request).file).to be_empty
  end

  it "ignores compiler version and source comments for deterministic output" do
    request = request_for
    first = generate(request).file.first.content
    request.compiler_version = Google::Protobuf::Compiler::Version.new(major: 99)
    request.proto_file.first.source_code_info = Google::Protobuf::SourceCodeInfo.new
    expect(generate(request).file.first.content).to eq(first)
  end

  it "rejects unknown targets, duplicate files, targets and proto symbols" do
    request = request_for
    request.file_to_generate << "absent.proto"
    expect(generate(request).error).to include("Missing generation target", "absent.proto")
    request.file_to_generate.pop
    request.file_to_generate << "sample.proto"
    expect(generate(request).error).to include("Duplicate generation target")
    request.file_to_generate.pop
    request.proto_file << request.proto_file.first
    expect(generate(request).error).to include("Duplicate proto file")
    request.proto_file.pop
    request.proto_file.first.message_type << request.proto_file.first.message_type.first
    expect(generate(request).error).to include("Duplicate proto symbol", "Message")
  end

  it "rejects unresolved, enum and synthetic map-entry RPC types" do
    request = request_for
    method = request.proto_file.first.service.first["method"].first
    method.input_type = ".missing.Type"
    expect(generate(request).error).to include("non-message type", ".missing.Type", "GetHTTP")
    method.input_type = ".generation.v1.Message"
    request.proto_file.first.message_type.first.options = Google::Protobuf::MessageOptions.new(map_entry: true)
    expect(generate(request).error).to include("non-message type")
  end

  ["../evil.proto", "/evil.proto", "a//b.proto", "a/../b.proto", "a\#{evil}.proto", "a\n.proto"].each do |name|
    it "rejects unsafe output path #{name.inspect}" do
      request = request_for
      request.proto_file.first.name = name
      request.file_to_generate[0] = name
      expect(generate(request).error).to include("Unsafe relative proto path")
    end
  end

  it "rejects output stem collisions between proto and protodevel" do
    request = request_for
    other = Google::Protobuf::FileDescriptorProto.new(name: "sample.protodevel", package: "other")
    request.proto_file << other
    expect(generate(request).error).to include("Output collision", "sample.proto", "sample.protodevel")
  end

  it "rejects unsupported syntax and editions without partial output" do
    %w[editions bad].each do |syntax|
      request = request_for
      request.proto_file.first.syntax = syntax
      response = generate(request)
      expect(response.error).to include("Unsupported syntax")
      expect(response.file).to be_empty
    end
  end

  it "rejects namespace injection and invalid constant tokens" do
    ["Safe::Bad;puts 1", "Safe::", "lower::Valid", "BEGIN", "a..b"].each do |namespace|
      request = request_for
      request.proto_file.first.options = Google::Protobuf::FileOptions.new(ruby_package: namespace)
      expect(generate(request).error).to include("Invalid Ruby constant")
    end
  end

  it "rejects service/message and alias namespace collisions with both proto names" do
    request = request_for(service: "message")
    expect(generate(request).error).to include("namespace collision", "generation.v1.message", "generation.v1.Message")
    request = request_for
    other = Google::Protobuf::FileDescriptorProto.new(
      name: "other.proto", package: "other",
      options: Google::Protobuf::FileOptions.new(ruby_package: "Generation::V1"),
      message_type: [Google::Protobuf::DescriptorProto.new(name: "EchoService")]
    )
    request.proto_file << other
    expect(generate(request).error).to include("namespace collision", "other.EchoService", "generation.v1.EchoService")
  end

  it "rejects package/class and service/package merges" do
    request = request_for
    other = Google::Protobuf::FileDescriptorProto.new(
      name: "other.proto", package: "generation.v1.echo_service",
      message_type: [Google::Protobuf::DescriptorProto.new(name: "Client")]
    )
    request.proto_file << other
    expect(generate(request).error).to include("namespace collision", "EchoService")
    other.package = "generation.v1.message"
    expect(generate(request).error).to include("namespace collision", "Message")
  end

  %w[File Process Object ConnectRPC Google].each do |name|
    it "rejects reserved top-level service #{name}" do
      request = request_for(rpcs: [], service: name, package: "")
      expect(generate(request).error).to include("Reserved top-level Ruby constant", name)
    end
  end

  %w[Google ConnectRPC].each do |name|
    %i[message enum].each do |kind|
      it "rejects loaded top-level #{kind} replacement of #{name}" do
        request = request_for(rpcs: [], package: "")
        file = request.proto_file.first
        if kind == :message
          file.message_type << Google::Protobuf::DescriptorProto.new(name: name)
        else
          file.enum_type << Google::Protobuf::EnumDescriptorProto.new(name: name)
        end
        response = generate(request)
        expect(response.error).to include("Reserved", name)
        expect(response.file).to be_empty
      end
    end
  end

  %w[Zlib URI Net OpenSSL Socket Timeout].each do |name|
    it "rejects runtime stdlib root #{name} as a top-level service" do
      response = generate(request_for(rpcs: [], service: name, package: ""))
      expect(response.error).to include("Reserved top-level Ruby constant #{name}")
      expect(response.file).to be_empty
    end

    it "rejects runtime stdlib root #{name} as a top-level message" do
      request = request_for(rpcs: [], package: "")
      request.proto_file.first.message_type << Google::Protobuf::DescriptorProto.new(name: name)
      response = generate(request)
      expect(response.error).to include("Reserved top-level Ruby constant #{name}")
      expect(response.file).to be_empty
    end
  end

  it "reserves every top-level constant the runtime library defines" do
    require "connectrpc/generator"
    root = File.expand_path("..", __dir__)
    # A clean process without Bundler setup; the bundle path supplies google-protobuf.
    gem_path = [Gem.loaded_specs.fetch("google-protobuf").base_dir, *Gem.default_path].join(File::PATH_SEPARATOR)
    probe = <<~RUBY
      before = Object.constants
      Dir[File.join(ARGV.fetch(0), "lib/**/*.rb")].sort.each { |file| require file }
      puts(Object.constants - before)
    RUBY
    output, error, status = Bundler.with_unbundled_env do
      Open3.capture3({ "GEM_PATH" => gem_path, "GEM_HOME" => nil, "RUBYOPT" => nil, "RUBYLIB" => nil },
                     RbConfig.ruby, "-I#{File.join(root, 'lib')}", "-e", probe, root)
    end
    expect(status.success?).to be(true), error
    missing = output.split.sort - ConnectRPC::Generator::Names::TOP_LEVEL_RESERVED
    expect(missing).to be_empty, "Unreserved runtime constants: #{missing.join(' ')}"
  end

  def with_ruby_package(request, namespace)
    request.proto_file.first.options = Google::Protobuf::FileOptions.new(ruby_package: namespace)
    request
  end

  def with_extra(request, message: nil, enum: nil)
    file = request.proto_file.first
    file.message_type << Google::Protobuf::DescriptorProto.new(name: message) if message
    if enum
      file.enum_type << Google::Protobuf::EnumDescriptorProto.new(
        name: enum, value: [Google::Protobuf::EnumValueDescriptorProto.new(name: "#{enum}_UNSPECIFIED")]
      )
    end
    request
  end

  {
    "math.v1" => "Math", "process.v1" => "Process", "signal.v1" => "Signal", "file_test.v1" => "FileTest",
    "object_space.v1" => "ObjectSpace", "marshal.v1" => "Marshal", "math" => "Math", "process.sys.v1" => "Process"
  }.each do |package, root|
    it "generates services nested under builtin module package #{package}" do
      response = generate(request_for(package: package))
      expect(response.error).to eq("")
      expect(response.file.first.content).to include("module #{root}\n", "module EchoService")
    end
  end

  %w[Math::V1 GC::V1 GC::Profiler::V1 Process::UID::V1 Signal].each do |namespace|
    it "generates services under builtin module ruby_package #{namespace}" do
      response = generate(with_ruby_package(request_for, namespace))
      expect(response.error).to eq("")
      expect(response.file.first.content).to include("module #{namespace.split('::').first}\n", "module EchoService")
    end
  end

  {
    "net.v1" => "Net", "file.v1" => "File", "kernel.v1" => "Kernel", "errno.v1" => "Errno",
    "comparable.v1" => "Comparable", "warning.v1" => "Warning"
  }.each do |package, root|
    it "rejects package #{package} under reserved root #{root} and names ruby_package" do
      response = generate(request_for(package: package))
      expect(response.error).to include("Reserved top-level Ruby constant #{root} ", "ruby_package")
      expect(response.file).to be_empty
    end
  end

  %w[URI::V1 OpenSSL::V1 Zlib Errno::V1].each do |namespace|
    it "rejects reserved root ruby_package #{namespace} and names ruby_package" do
      response = generate(with_ruby_package(request_for, namespace))
      expect(response.error).to include("Reserved top-level Ruby constant #{namespace.split('::').first} ",
                                        "ruby_package")
      expect(response.file).to be_empty
    end
  end

  it "rejects top-level Math service and message replacements and names ruby_package" do
    response = generate(request_for(rpcs: [], service: "Math", package: ""))
    expect(response.error).to include("Reserved top-level Ruby constant Math ", "ruby_package")
    request = request_for(rpcs: [], package: "")
    request.proto_file.first.message_type << Google::Protobuf::DescriptorProto.new(name: "Math")
    response = generate(request)
    expect(response.error).to include("Reserved top-level Ruby constant Math ", "ruby_package")
    expect(response.file).to be_empty
  end

  it "allows fresh direct children of a builtin module" do
    response = generate(with_extra(request_for(package: "math"), message: "PlanMessage", enum: "PlanKind"))
    expect(response.error).to eq("")
    expect(response.file.first.content).to include("module Math\n", "module EchoService")
  end

  {
    "package math.e.v1" => [-> { request_for(package: "math.e.v1") }, "Math::E"],
    "package process.status.v1" => [-> { request_for(package: "process.status.v1") }, "Process::Status"],
    "ruby_package Math::DomainError" => [-> { with_ruby_package(request_for, "Math::DomainError") },
                                         "Math::DomainError"],
    "ruby_package ObjectSpace::WeakMap::V1" => [-> { with_ruby_package(request_for, "ObjectSpace::WeakMap::V1") },
                                                "ObjectSpace::WeakMap"],
    "message E in package math" => [-> { with_extra(request_for(package: "math"), message: "E") }, "Math::E"],
    "enum PI in package math" => [-> { with_extra(request_for(package: "math"), enum: "PI") }, "Math::PI"],
    "service Status in package process" => [-> { request_for(service: "Status", package: "process") },
                                            "Process::Status"],
    "platform message CLOCK_MONOTONIC in package process" => [
      -> { with_extra(request_for(package: "process"), message: "CLOCK_MONOTONIC") }, "Process::CLOCK_MONOTONIC"
    ],
    "message Sys in package process" => [-> { with_extra(request_for(package: "process"), message: "Sys") },
                                         "Process::Sys"],
    "ruby_package GC with message Profiler" => [
      -> { with_extra(with_ruby_package(request_for, "GC"), message: "Profiler") }, "GC::Profiler"
    ]
  }.each do |description, (build, constant)|
    it "rejects builtin child #{constant} through #{description} and names ruby_package" do
      response = generate(instance_exec(&build))
      expect(response.error).to include("Reserved builtin Ruby constant #{constant} ", "ruby_package")
      expect(response.file).to be_empty
    end
  end

  it "keeps builtin reopen policy data consistent with the running Ruby" do
    require "connectrpc/generator"
    names = ConnectRPC::Generator::Names
    # The interpreter's own modules, without RubyGems or Bundler side effects.
    probe = <<~RUBY
      modules = ObjectSpace.each_object(Module).to_a
      facts = ARGV.to_h do |name|
        root = Object.const_get(name)
        mixed = modules.any? do |other|
          !other.equal?(root) && (other.include?(root) || other.singleton_class.include?(root))
        end
        children = root.constants(false).to_h do |constant|
          value = root.const_get(constant, false)
          kind = if value.instance_of?(Module)
                   value.constants(false).empty? ? :module : :populated_module
                 else
                   :value
                 end
          [constant.to_s, kind]
        end
        [name, [root.class, mixed, children]]
      end
      $stdout.binmode.write(Marshal.dump(facts))
    RUBY
    output, error, status = Bundler.with_unbundled_env do
      Open3.capture3({ "RUBYOPT" => nil, "RUBYLIB" => nil }, RbConfig.ruby, "--disable-gems", "-e", probe,
                     *names::REOPENABLE_ROOTS, binmode: true)
    end
    expect(status.success?).to be(true), error
    facts = Marshal.load(output) # rubocop:disable Security/MarshalLoad
    expect(facts.keys).to eq(names::REOPENABLE_ROOTS)
    expect(names::REOPENABLE_ROOTS - names::TOP_LEVEL_RESERVED).to be_empty
    facts.each do |root, (type, mixed, children)|
      expect([type, mixed]).to eq([Module, false]), "#{root} must be a plain, unmixed module"
      expect(children.select { |_name, kind| kind == :populated_module }.keys).to be_empty
      modules = children.select { |_name, kind| kind == :module }.keys.sort
      expect(modules).to eq(names::BUILTIN_MODULE_CHILDREN.fetch(root, []).sort), "#{root} module children"
      fixed_values = names::BUILTIN_VALUE_CHILDREN.fetch(root, [])
      values = children.select { |_name, kind| kind == :value }.keys
      uncovered = values.reject { |name| fixed_values.any? { |entry| entry === name } } # rubocop:disable Style/CaseEquality
      expect(uncovered).to be_empty, "Unprotected #{root} constants: #{uncovered.join(' ')}"
      expect(fixed_values.grep(String) - values).to be_empty, "#{root} value children"
    end
  end

  it "generates a target that imports a builtin module package and uses its messages" do
    request = request_for
    imported = Google::Protobuf::FileDescriptorProto.new(
      name: "imported.proto", package: "math.v1", syntax: "proto3",
      message_type: [Google::Protobuf::DescriptorProto.new(name: "Imported")]
    )
    request.proto_file << imported
    request.proto_file.first.dependency << imported.name
    request.proto_file.first.service.first["method"].first.input_type = ".math.v1.Imported"
    response = generate(request)
    expect(response.error).to eq("")
    expect(response.file.first.content).to include('require "imported_pb"', '"math.v1.Imported"')
  end

  {
    "net.v1" => "Reserved top-level Ruby constant Net ", "math.e" => "Reserved builtin Ruby constant Math::E "
  }.each do |package, message|
    it "rejects a loaded import in package #{package} and names ruby_package" do
      request = request_for
      imported = Google::Protobuf::FileDescriptorProto.new(
        name: "imported.proto", package: package,
        message_type: [Google::Protobuf::DescriptorProto.new(name: "Imported")]
      )
      request.proto_file << imported
      request.proto_file.first.dependency << imported.name
      response = generate(request)
      expect(response.error).to include(message, "imported.proto", "ruby_package")
      expect(response.file).to be_empty
    end
  end

  it "validates large package-less schemas in roughly linear time" do
    build = lambda do |count|
      messages = Array.new(count) do |index|
        Google::Protobuf::DescriptorProto.new(name: "M#{index}",
                                              nested_type: [Google::Protobuf::DescriptorProto.new(name: "Inner")])
      end
      rpc = Google::Protobuf::MethodDescriptorProto.new(name: "Call", input_type: ".M0", output_type: ".M1.Inner")
      file = Google::Protobuf::FileDescriptorProto.new(
        name: "bulk.proto", syntax: "proto3", message_type: messages,
        service: [Google::Protobuf::ServiceDescriptorProto.new(name: "BulkService", method: [rpc])]
      )
      Google::Protobuf::Compiler::CodeGeneratorRequest.new(file_to_generate: [file.name], proto_file: [file])
    end
    small = build.call(1_500)
    large = build.call(6_000)
    response = generate(large)
    expect(response.error).to eq("")
    expect(response.file.first.content).to include("module BulkService", '"M0"', '"M1.Inner"', "def call(request")
    # Same-run thread CPU ratio: 4x input stays well under quadratic 16x on any
    # runner speed, and other threads in the suite cannot inflate either sample.
    cpu = lambda do |request|
      Array.new(3) do
        GC.start
        started = Process.clock_gettime(Process::CLOCK_THREAD_CPUTIME_ID)
        generate(request)
        Process.clock_gettime(Process::CLOCK_THREAD_CPUTIME_ID) - started
      end.min
    end
    small_time = cpu.call(small)
    large_time = cpu.call(large)
    expect(large_time / small_time).to be < 8, format("1,500: %<small>.3fs, 6,000: %<large>.3fs CPU",
                                                      small: small_time, large: large_time)
  end

  %w[file ConnectRPC].each do |package|
    it "rejects incompatible loaded import package #{package}" do
      request = request_for
      imported = Google::Protobuf::FileDescriptorProto.new(
        name: "imported.proto", package: package,
        message_type: [Google::Protobuf::DescriptorProto.new(name: "Imported")]
      )
      request.proto_file << imported
      request.proto_file.first.dependency << imported.name
      response = generate(request)
      expect(response.error).to include("Reserved", package == "file" ? "File" : "ConnectRPC")
      expect(response.file).to be_empty
    end
  end

  %w[
    Google::Protobuf Google::Protobuf::DescriptorPool Google::Protobuf::Error
    Google::Protobuf::Internal::Arena Google::Protobuf::MessageExts
    Google::Protobuf::Compiler::CodeGeneratorRequest Google::Protobuf::Timestamp
    ConnectRPC::Method ConnectRPC::Bindings::GoogleProtobuf
  ].each do |constant|
    %i[message enum package service].each do |kind|
      description = if kind == :package && constant == "Google::Protobuf"
                      "allows package nesting under #{constant}"
                    else
                      "rejects #{kind} collision with runtime #{constant}"
                    end
      it description do
        request = request_for(rpcs: [])
        file = request.proto_file.first
        parts = constant.split("::")
        # A package is emitted with module blocks; the other forms replace or
        # reopen the final constant through the official Ruby package option.
        namespace = kind == :package ? parts : parts[0...-1]
        file.options = Google::Protobuf::FileOptions.new(ruby_package: namespace.join("::"))
        case kind
        when :message
          file.message_type << Google::Protobuf::DescriptorProto.new(name: parts.last)
        when :enum
          file.enum_type << Google::Protobuf::EnumDescriptorProto.new(name: parts.last)
        when :service
          file.service.first.name = parts.last
        end
        response = generate(request)
        if kind == :package && constant == "Google::Protobuf"
          expect(response.error).to eq("")
        else
          if constant.start_with?("Google")
            protected_name = constant.sub("Internal::Arena", "Internal")
            expect(response.error).to include("Reserved runtime Ruby constant #{protected_name}")
          else
            expect(response.error).to include("Reserved top-level Ruby constant ConnectRPC")
          end
          expect(response.file).to be_empty
        end
      end
    end
  end

  it "allows Google package nesting and equivalent explicit Ruby package mappings" do
    request = request_for(package: "google.correction_fixture.v1")
    expect(generate(request).error).to eq("")
    request.proto_file.first.options = Google::Protobuf::FileOptions.new(ruby_package: "Google::CorrectionFixture::V1")
    expect(generate(request).error).to eq("")
  end

  it "does not reject unrelated unloaded runtime package declarations" do
    request = request_for
    request.proto_file << Google::Protobuf::FileDescriptorProto.new(name: "unloaded.proto", package: "file")
    expect(generate(request).error).to eq("")
  end

  it "rejects Ruby collisions in imported message namespaces required by a target" do
    request = request_for
    %w[first second].each do |name|
      imported = Google::Protobuf::FileDescriptorProto.new(
        name: "#{name}.proto", package: name,
        options: Google::Protobuf::FileOptions.new(ruby_package: "ImportedCollision"),
        message_type: [Google::Protobuf::DescriptorProto.new(name: "Message")]
      )
      request.proto_file << imported
      request.proto_file.first.dependency << imported.name
    end
    request.proto_file.first.service.first["method"].first.input_type = ".first.Message"
    response = generate(request)
    expect(response.error).to include("namespace collision", "first.Message", "second.Message")
    expect(response.file).to be_empty
  end

  it "allows RPC names distinct from inherited predicate methods" do
    response = generate(request_for(rpcs: %w[Equal Frozen RespondTo]))
    expect(response.error).to eq("")
    expect(response.file.first.content).to include("def equal(request", "def frozen(request", "def respond_to(request")
  end
end
