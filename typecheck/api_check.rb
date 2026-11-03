# frozen_string_literal: true

require_relative "harness"

module APICheck
  PACKAGES = %w[rack testing rails async falcon otel].freeze
  BRIDGES = { "rails" => "rbs/action_controller_8", "falcon" => "rbs/async_service_0_25" }.freeze

  def self.bridge_paths(root:, installed:)
    BRIDGES.map do |name, relative|
      package = File.join(root, "gems/connectrpc-#{name}")
      package = Gem::Specification.find_by_name("connectrpc-#{name}").full_gem_path if installed
      File.join(package, relative)
    end
  end

  def self.check(root:, working:, installed: false, tools: [RbConfig.ruby, "-S"])
    FileUtils.mkdir_p(working)
    FileUtils.cp_r(File.join(root, "typecheck/api"), working)
    bridges = bridge_paths(root: root, installed: installed)
    configuration = if installed
                      (["connectrpc"] + PACKAGES.map do |name|
                        "connectrpc-#{name}"
                      end).map { |name| "library #{name.dump}" }.join("\n  ")
                    else
                      signatures = [File.join(root, "sig"), *PACKAGES.map do |name|
                        File.join(root, "gems/connectrpc-#{name}/sig")
                      end]
                      paths = signatures.map do |directory|
                        "signature #{Pathname(directory).relative_path_from(Pathname(working)).to_s.dump}"
                      end.join("\n  ")
                      "library 'uri'\n  library 'openssl'\n  library 'socket'\n  #{paths}"
                    end
    bridge_configuration = bridges.map do |directory|
      "signature #{Pathname(directory).relative_path_from(Pathname(working)).to_s.dump}"
    end.join("\n  ")
    File.write(File.join(working, "Steepfile.api"), <<~STEEP)
      target :api do
        #{configuration}
        #{bridge_configuration}
        signature 'api'
        check 'api'
        configure_code_diagnostics(Steep::Diagnostic::Ruby.strict)
      end
    STEEP
    command = [*tools, "steep", "check", "--jobs=1", "--no-daemon", "--steepfile=Steepfile.api"]
    rbs = [*tools, "rbs"]
    if installed
      (["connectrpc"] + PACKAGES.map { |name| "connectrpc-#{name}" }).each { |name| rbs.push("-r", name) }
    else
      [File.join(root, "sig"), *PACKAGES.map do |name|
        File.join(root, "gems/connectrpc-#{name}/sig")
      end].each do |directory|
        rbs.push("-I", directory)
      end
      %w[uri openssl socket].each { |name| rbs.push("-r", name) }
    end
    bridge_options = bridges.flat_map { |directory| ["-I", directory] }
    RBSHarness.capture!([*rbs, *bridge_options, "-I", File.join(working, "api"), "validate"], working: working)
    positive = RBSHarness.capture!([*command, *Dir.glob("api/*.rb", base: working)], working: working)
    checked = positive.lines.filter_map { |line| line.strip.size if line.match?(/^\.+$/) }.sum
    raise "API consumer file count mismatch" unless checked == Dir.glob("api/*.rb", base: working).size

    File.write(File.join(working, "api-positive.log"), positive)
    cases = JSON.parse(File.read(File.join(working, "api/negatives.json")))
    cases.each do |name, diagnostic|
      output, status = Open3.capture2e(*command, "api/negative/#{name}.rb", chdir: working)
      File.write(File.join(working, "api-#{name}.log"), output)
      actual = output.scan(/Diagnostic ID: (\S+)/).flatten
      unless status.exitstatus == 1 && actual == [diagnostic]
        raise "Expected exactly #{diagnostic} for #{name}; exit #{status.exitstatus}, #{actual.inspect}\n#{output}"
      end
    end
    count = Dir.glob("api/*.rb", base: working).size
    puts "Public API consumers: #{count} positive files, #{cases.size} exact negative diagnostics " \
         "(Ruby #{RUBY_VERSION})"
    falcon_only(root: root, working: working, installed: installed, tools: tools)
  end

  def self.falcon_only(root:, working:, installed:, tools:)
    FileUtils.cp_r(File.join(root, "typecheck/falcon"), working)
    configuration = if installed
                      "library 'connectrpc-falcon'"
                    else
                      directories = [File.join(root, "sig"), *%w[async falcon].map do |name|
                        File.join(root, "gems/connectrpc-#{name}/sig")
                      end]
                      signatures = directories.map do |directory|
                        "signature #{Pathname(directory).relative_path_from(Pathname(working)).to_s.dump}"
                      end.join("\n  ")
                      "library 'uri'\n  library 'openssl'\n  library 'socket'\n  #{signatures}"
                    end
    File.write(File.join(working, "Steepfile.falcon"), <<~STEEP)
      target :falcon do
        #{configuration}
        check 'falcon'
        configure_code_diagnostics(Steep::Diagnostic::Ruby.strict)
      end
    STEEP
    command = [*tools, "steep", "check", "--jobs=1", "--no-daemon", "--steepfile=Steepfile.falcon"]
    positive = RBSHarness.capture!([*command, "falcon/positive.rb"], working: working)
    File.write(File.join(working, "falcon-only-positive.log"), positive)
    output, status = Open3.capture2e(*command, "falcon/negative.rb", chdir: working)
    File.write(File.join(working, "falcon-only-negative.log"), output)
    actual = output.scan(/Diagnostic ID: (\S+)/).flatten
    unless status.exitstatus == 1 && actual == ["Ruby::ArgumentTypeMismatch"]
      raise "Wrong Falcon-only malformed body diagnostic: #{status.exitstatus}, #{actual.inspect}\n#{output}"
    end

    puts "Falcon-only enumerable/callable fallback consumers passed; malformed body rejected exactly once"
  end
end

if $PROGRAM_NAME == __FILE__
  root = File.expand_path("..", __dir__)
  APICheck.check(root: root, working: File.join(root, "vendor/rbs/api", RUBY_VERSION))
end
