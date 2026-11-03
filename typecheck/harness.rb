# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "pathname"
require "rbconfig"
require_relative "negative_check"

module RBSHarness
  NEGATIVES = {
    "generated_request" => "Ruby::ArgumentTypeMismatch",
    "direct_request" => "Ruby::UnsatisfiableConstraint",
    "response_assignment" => "Ruby::IncompatibleAssignment",
    "response_use" => "Ruby::NoMethod",
    "response_writer" => "Ruby::ArgumentTypeMismatch",
    "handler_return" => "Ruby::ArgumentTypeMismatch",
    "handler_object" => "Ruby::ArgumentTypeMismatch",
    "handler_envelope" => "Ruby::ArgumentTypeMismatch",
    "handler_request" => "Ruby::NoMethod",
    "handler_context" => "Ruby::NoMethod",
    "field" => "Ruby::ArgumentTypeMismatch",
    "constructor" => "Ruby::ArgumentTypeMismatch",
    "repeated" => "Ruby::ArgumentTypeMismatch",
    "map" => "Ruby::ArgumentTypeMismatch",
    "lifecycle_delivery" => "Ruby::ArgumentTypeMismatch",
    "lifecycle_failure" => "Ruby::ArgumentTypeMismatch",
    "struct" => "Ruby::UnresolvedOverloading"
  }.freeze

  def self.capture!(command, working:, environment: {})
    output, status = Open3.capture2e(environment, *command, chdir: working)
    raise "Command failed: #{command.inspect}\n#{output}" unless status.success?

    output
  end

  def self.check(working:, bridge:, generated:, core: nil, tools: [RbConfig.ruby, "-S"])
    FileUtils.rm_rf(File.join(working, "core-sig"))
    FileUtils.cp_r(core, File.join(working, "core-sig")) if core
    bridge_relative = Pathname(bridge).relative_path_from(Pathname(working)).to_s
    generated_relative = Pathname(generated).relative_path_from(Pathname(working)).to_s
    libraries = core ? "signature 'core-sig'\n  library 'uri'" : "library 'connectrpc'"
    configuration = <<~STEEP
      target :consumer do
        #{libraries}
        library 'stringio'
        signature #{bridge_relative.dump}, #{generated_relative.dump}, 'consumers'
        check 'consumers'
        configure_code_diagnostics(Steep::Diagnostic::Ruby.strict)
      end
    STEEP
    File.write(File.join(working, "Steepfile"), configuration)
    command = [*tools, "steep", "check", "--jobs=1"]
    positive = capture!([*command, "consumers/positive.rb", "consumers/support.rb"], working: working)
    File.write(File.join(working, "positive.log"), positive)
    results = RBSNegativeCheck.check(command: command, working: working, directory: "consumers/negative",
                                     cases: NEGATIVES, log: "negative.log")
    service = File.join(working, "service-signatures")
    FileUtils.mkdir_p(service)
    FileUtils.cp(Dir.glob(File.join(generated, "**/*_connect.rbs")), service)
    File.write(File.join(working, "Steepfile.nominal"), <<~STEEP)
      target :nominal do
        #{libraries}
        signature 'service-signatures', 'nominal'
        check 'nominal'
        configure_code_diagnostics(Steep::Diagnostic::Ruby.strict)
      end
    STEEP
    capture!([*tools, "steep", "check", "--jobs=1", "--steepfile=Steepfile.nominal"], working: working)
    options = core ? ["-I", core, "-r", "uri"] : ["-r", "connectrpc"]
    capture!([*tools, "rbs", *options, "-I", bridge, "-I", generated, "-I",
              File.join(working, "consumers"), "validate"], working: working)
    isolation = <<~RUBY_CODE
      require 'rbs'
      loader = RBS::EnvironmentLoader.new
      #{core ? "loader.add(path: Pathname(#{core.dump})); loader.add(library: 'uri')" : "loader.add(library: 'connectrpc')"}
      env = RBS::Environment.from_loader(loader).resolve_type_names
      forbidden = %w[::Google ::Rack ::Rails ::Async ::Falcon ::OpenTelemetry ::ConnectRPC::Rack ::ConnectRPC::Rails ::ConnectRPC::Async ::ConnectRPC::Falcon ::ConnectRPC::OTel ::ConnectRPC::Testing]
      abort 'Core signatures introduce optional declarations' if env.class_decls.keys.any? { |key| forbidden.any? { |name| key.to_s == name || key.to_s.start_with?(name + '::') } }
      puts 'Core signature loading introduces no optional namespaces'
    RUBY_CODE
    puts capture!([RbConfig.ruby, "-e", isolation], working: working)
    File.write(File.join(working, "results.json"),
               JSON.pretty_generate(ruby: RUBY_VERSION, batch_exit: 1, cases: results))
    puts "Strict positive, nominal field extension, RBS validation and #{results.size} negative consumers " \
         "passed (Ruby #{RUBY_VERSION})"
  end
end
