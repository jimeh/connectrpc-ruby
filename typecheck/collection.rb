# frozen_string_literal: true

require "fileutils"
require "yaml"
require_relative "api_check"
require_relative "harness"

# Installs each optional package into a consumer that uses `rbs collection`,
# listing the optional gem first as every package README does. RBS locks each
# manifest dependency as a stdlib library before it walks Gemfile.lock, so a
# manifest that names a connectrpc gem breaks the whole Steep environment.
module RBSCollection
  NAMESPACES = { "rack" => "Rack", "testing" => "Testing", "on-rails" => "OnRails", "async" => "Async",
                 "falcon" => "Falcon", "otel" => "OTel", "prometheus" => "Prometheus", "yabeda" => "Yabeda",
                 "health" => "Health", "sentry" => "Sentry" }.freeze
  # Only the always-present gem and stdlib sources, so installation is offline.
  CONFIGURATION = "sources: []\npath: .gem_rbs_collection\n"
  STEEPFILE = <<~STEEP
    target :app do
      %<libraries>s
      check "app.rb"
      configure_code_diagnostics(Steep::Diagnostic::Ruby.strict)
    end
  STEEP
  APP = <<~RUBY
    # frozen_string_literal: true

    [ConnectRPC::Error, ConnectRPC::%<namespace>s].each { |constant| constant.name }
  RUBY
  COMMANDS = [%w[bundle lock --local], %w[rbs collection install], %w[steep check --jobs=1]].freeze

  def self.check(working:)
    APICheck::PACKAGES.each { |name| consumer(name, directory: File.join(working, "collection", name)) }
    puts "rbs collection consumers locked every connectrpc library as a gem and passed strict Steep " \
         "(#{APICheck::PACKAGES.size} optional packages)"
  end

  def self.consumer(name, directory:)
    FileUtils.mkdir_p(directory)
    files(name).each { |file, content| File.write(File.join(directory, file), content) }
    environment = { "BUNDLE_GEMFILE" => File.join(directory, "Gemfile"),
                    "BUNDLE_USER_HOME" => File.join(directory, ".bundle-home") }
    COMMANDS.each do |command|
      output = RBSHarness.capture!([RbConfig.ruby, "-S", *command], working: directory, environment: environment)
      File.write(File.join(directory, "#{command.first}.log"), output)
    end
    verify_lock(name, File.join(directory, "rbs_collection.lock.yaml"))
  end

  def self.files(name)
    libraries = APICheck.libraries(name).map { |library| "library #{library.dump}" }.join("\n  ")
    { "Gemfile" => %(source "https://rubygems.org"\n\ngem "connectrpc-#{name}"\n),
      "rbs_collection.yaml" => CONFIGURATION, "Steepfile" => format(STEEPFILE, libraries: libraries),
      "app.rb" => format(APP, namespace: NAMESPACES.fetch(name)) }
  end

  def self.verify_lock(name, path)
    locked = YAML.safe_load_file(path).fetch("gems").to_h { |gem| [gem.fetch("name"), gem.dig("source", "type")] }
    expected = APICheck.libraries(name).to_h { |library| [library, "rubygems"] }
    return if locked.slice(*expected.keys) == expected

    raise "rbs collection locked connectrpc-#{name} libraries wrongly: #{locked.inspect}"
  end
end
