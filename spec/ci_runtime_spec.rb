# frozen_string_literal: true

require "json"
require "open3"
require "rbconfig"
require "tmpdir"
require "yaml"

RSpec.describe "CI runtime boundary" do
  let(:root) { File.expand_path("..", __dir__) }
  let(:ruby) { File.realpath(RbConfig.ruby) }
  let(:environment) do
    {
      "RUNNER_TOOL_CACHE" => File.dirname(ruby, 2),
      "CI_EXPECTED_RUBY_VERSION" => RUBY_VERSION,
      "PATH" => "#{File.dirname(ruby)}:#{ENV.fetch('PATH')}",
      "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_FROZEN" => "true",
      "BUNDLE_DISABLE_SHARED_GEMS" => "true", "BUNDLE_VERSION" => "4.0.23",
      "BUNDLE_PATH" => File.join(root, "vendor/bundle")
    }
  end

  def guard(overrides = {}, *)
    Bundler.with_unbundled_env do
      Open3.capture3(environment.merge(overrides), ruby, File.join(root, "scripts/ci_runtime.rb"), *)
    end
  end

  it "accepts the exact runtime and reports the bundle actually used" do
    output, error, status = guard
    expect(status.success?).to be(true), error
    expect(output).to include("Ruby #{RUBY_VERSION}: #{ruby}", "Bundler 4.0.23",
                              "cache root: #{root}/vendor/bundle", "CPUs:", "image:")
  end

  it "rejects a different matrix Ruby" do
    _output, error, status = guard("CI_EXPECTED_RUBY_VERSION" => "0.0.0")
    expect(status.success?).to be(false)
    expect(error).to include("unexpected Ruby")
  end

  it "rejects a runtime outside the runner tool cache" do
    Dir.mktmpdir("connectrpc-ci-cache-") do |cache|
      _output, error, status = guard("RUNNER_TOOL_CACHE" => cache)
      expect(status.success?).to be(false)
      expect(error).to include("Ruby is outside RUNNER_TOOL_CACHE")
    end
  end

  it "rejects a PATH Ruby that differs from the running Ruby" do
    Dir.mktmpdir("connectrpc-ci-path-") do |directory|
      File.symlink(File.join(File.dirname(ruby), "bundle"), File.join(directory, "ruby"))
      _output, error, status = guard("PATH" => "#{directory}:#{ENV.fetch('PATH')}")
      expect(status.success?).to be(false)
      expect(error).to include("PATH Ruby differs from running Ruby")
    end
  end

  it "rejects bundle settings that bypass the cached frozen path" do
    { "BUNDLE_PATH" => "/tmp/wrong-bundle", "BUNDLE_FROZEN" => "false",
      "BUNDLE_IGNORE_CONFIG" => "false", "BUNDLE_VERSION" => "0.0.0" }.each do |key, value|
      _output, error, status = guard(key => value)
      expect(status.success?).to be(false)
      expect(error).to include("unexpected #{key}")
    end
  end

  def go_guard(mode, context_overrides = {}, directory: mode == "interop" ? "interop" : "browser/reference",
               outside_cache: false)
    module_dir = mode == "interop" ? "interop" : "browser/reference"
    cache_dir = mode == "interop" ? "interop" : "browser/go"
    context = {
      "GOVERSION" => "go1.27.3", "GOROOT" => File.dirname(ruby),
      "GOMODCACHE" => File.join(root, "vendor", cache_dir, "mod"),
      "GOCACHE" => File.join(root, "vendor", cache_dir, "build"),
      "GOTOOLCHAIN" => "local", "GOWORK" => "off", "GOFLAGS" => "-mod=readonly",
      "GOMOD" => File.join(root, module_dir, "go.mod")
    }.merge(context_overrides)
    Dir.mktmpdir("connectrpc-go-boundary-") do |bin|
      go = File.join(bin, "go")
      # Stub the external go env boundary. The symlink gives the simulated action
      # runtime a real executable identity within the test's tool cache.
      File.symlink(outside_cache ? "/bin/true" : ruby, go)
      prelude = <<~RUBY
        require "open3"
        original = Open3.method(:capture2)
        Open3.define_singleton_method(:capture2) do |*args|
          if args.first == #{go.inspect}
            [#{JSON.generate(context).inspect}, Struct.new(:success?).new(true)]
          else
            original.call(*args)
          end
        end
        load ARGV.shift
      RUBY
      Bundler.with_unbundled_env do
        Open3.capture3(environment.merge("PATH" => "#{File.dirname(ruby)}:#{bin}:#{ENV.fetch('PATH')}"),
                       ruby, "-e", prelude, File.join(root, "scripts/ci_runtime.rb"), mode,
                       chdir: File.join(root, directory))
      end
    end
  end

  %w[interop browser-reference].each do |mode|
    it "validates the finite #{mode} Go module and cache context without changing runtime ownership" do
      output, error, status = go_guard(mode)
      expect(status.success?).to be(true), error
      expect(output).to include("Go:", "go1.27.3")
      { "GOVERSION" => "go0.0.0", "GOROOT" => "/tmp", "GOMODCACHE" => "/tmp/wrong",
        "GOCACHE" => "/tmp/wrong", "GOTOOLCHAIN" => "auto", "GOWORK" => "auto", "GOFLAGS" => "",
        "GOMOD" => File.join(root, "wrong/go.mod") }.each do |key, value|
        _output, error, status = go_guard(mode, { key => value })
        expect(status.success?).to be(false), key
        expect(error).to include(key == "GOVERSION" ? "unexpected Go version" : key)
      end
      _output, error, status = go_guard(mode, directory: ".")
      expect(status.success?).to be(false)
      expect(error).to include("wrong working directory")
      _output, error, status = go_guard(mode, outside_cache: true)
      expect(status.success?).to be(false)
      expect(error).to include("Go is outside RUNNER_TOOL_CACHE")
    end
  end

  it "rejects unknown guard modes" do
    _output, error, status = guard({}, "unknown")
    expect(status.success?).to be(false)
    expect(error).to include("Usage:")
  end
end

RSpec.describe "CI check mapping" do
  let(:workflow) { YAML.load_file(File.expand_path("../.github/workflows/ci.yml", __dir__)) }
  let(:jobs) { workflow.fetch("jobs") }

  it "preserves all 28 existing checks and adds two independent browser leaves" do
    core = %w[static specs package interop conformance]
    backend = %w[async_specs async_interop]
    falcon = %w[falcon_specs falcon_interop]
    expect(jobs.keys).to match_array([*core, "rails", *backend, "rack", "testing", "rbs", *falcon, "otel",
                                      "browser_grpc_web"])
    count = lambda do |names|
      jobs.slice(*names).values.sum do |job|
        job.dig("strategy", "matrix", "ruby")&.size || job.dig("strategy", "matrix", "include")&.size || 1
      end
    end
    expect(count.call(core)).to eq(9)
    expect(count.call(["rails"])).to eq(3)
    expect(count.call(backend)).to eq(4)
    expect(count.call(falcon)).to eq(4)
    expect(count.call(["rack"])).to eq(2)
    expect(count.call(["testing"])).to eq(2)
    expect(count.call(["rbs"])).to eq(2)
    expect(count.call(jobs.keys - ["browser_grpc_web"])).to eq(28)
    expect(count.call(["otel"])).to eq(2)
    expect(count.call(["browser_grpc_web"])).to eq(2)
    expect(count.call(jobs.keys)).to eq(30)
    jobs.each_value { |job| expect(job).not_to have_key("needs") }
    jobs.except("static", "rails").each_value do |job|
      expect(job.dig("strategy", "fail-fast")).to be(false)
      expect(job.dig("strategy", "matrix", "ruby")).to eq(%w[3.4.12 4.0.9])
    end
    commands = jobs.transform_values { |job| job.fetch("steps").filter_map { |step| step["run"] } }
    expect(commands.fetch("static")).to include("mise run check:static")
    expect(commands.fetch("specs").first(2)).to eq(["mise run syntax", "mise run test"])
    expect(commands.fetch("package").first(2)).to eq(["mise run generation:check", "mise run check:package"])
    expect(commands.fetch("interop").first(2)).to eq(["mise run fixture:check", "mise run test:interop"])
    expect(commands.fetch("conformance")).to include("mise run test:conformance")
    expect(commands.fetch("otel").first).to eq("mise run otel:verify")
    expect(commands.fetch("browser_grpc_web").first).to eq("mise run browser:verify")
    expect(commands.fetch("browser_grpc_web")[1]).to include("mise run async:conformance:web",
                                                             "mise run falcon:conformance:web",
                                                             "mise run rack:conformance:web")
    expect(commands.fetch("rbs").first(3)).to eq(["mise run rbs:cache-platform", "mise run rbs:check",
                                                  "mise run rbs:package"])
  end

  it "pins the three Rails Appraisal lanes and caches the selected frozen lock" do
    rails = jobs.fetch("rails")
    expected = [
      { "rails" => "8.0", "ruby" => "3.4.12" }, { "rails" => "8.1", "ruby" => "3.4.12" },
      { "rails" => "8.1", "ruby" => "4.0.9" }
    ]
    expect(rails.dig("strategy", "matrix", "include")).to eq(expected)
    expect(rails.fetch("env")).to include("BUNDLE_COOLDOWN" => "7", "RAILS_LANE" => "${{ matrix.rails }}")
    setup = rails.fetch("steps").find { |step| step.fetch("uses", "").start_with?("ruby/setup-ruby@") }
    gemfile = "${{ github.workspace }}/gems/connectrpc-rails/gemfiles/rails_${{ matrix.rails }}.gemfile"
    expect(setup.dig("env", "BUNDLE_GEMFILE")).to eq(gemfile)
    commands = rails.fetch("steps").filter_map { |step| step["run"] }
    expect(commands.first(3)).to eq(["mise run test:rails", "mise run generation:check:rails",
                                     "mise run check:package:rails"])
    expect(commands.join).not_to include("appraisal install", "mise run verify")
  end

  it "keeps shared complete bundle keys and prevents Mise from shadowing action runtimes" do
    jobs.each do |name, job|
      steps = job.fetch("steps")
      ruby = steps.find { |step| step.fetch("uses", "").start_with?("ruby/setup-ruby@") }
      expect(ruby.fetch("with")).to include("bundler" => "4.0.23", "bundler-cache" => name != "rbs",
                                            "cache-version" => "bundler-4.0.23-v1")
      mise = steps.find { |step| step.fetch("uses", "").start_with?("jdx/mise-action@") }
      expect(mise.fetch("with")).to include("add_shims_to_path" => false, "env" => false, "export_path" => false)
      expect(steps.last).to include("if" => "always()")
      expect(steps.last.fetch("run")).to include("git status --porcelain --untracked-files=all")
    end
  end

  it "separates complete Mise tool sets and gives setup-go the caches used by canonical tasks" do
    sets = jobs.transform_values do |job|
      job.fetch("steps").find { |step| step.fetch("uses", "").start_with?("jdx/mise-action@") }.fetch("with")
    end
    expect(sets.fetch("static")).to include("install" => true, "cache" => true, "install_args" => "actionlint zizmor")
    expect(sets.fetch("conformance")).to include("install" => true, "cache" => true,
                                                 "install_args" => "github:connectrpc/conformance@1.0.6 " \
                                                                   "aqua:protocolbuffers/protobuf/protoc@36.2")
    %w[specs interop].each do |job|
      expect(sets.fetch(job)).to include("install" => false, "cache" => false)
    end
    expect(sets.fetch("package")).to include("install" => true, "cache" => true,
                                             "install_args" => "aqua:protocolbuffers/protobuf/protoc@36.2 " \
                                                               "aqua:bufbuild/buf@1.75.0")
    expect(sets.fetch("rbs")).to eq(sets.fetch("package"))
    interop = jobs.fetch("interop")
    expect(interop.fetch("env")).to include("GOMODCACHE" => "${{ github.workspace }}/vendor/interop/mod",
                                            "GOCACHE" => "${{ github.workspace }}/vendor/interop/build")
    go = interop.fetch("steps").find { |step| step.fetch("uses", "").start_with?("actions/setup-go@") }
    expect(go.fetch("with")).to include("go-version" => "1.27.3", "check-latest" => false, "cache" => true,
                                        "cache-dependency-path" => "interop/go.sum")
  end

  it "provides action-owned pinned Go to both browser leaves without unrelated default caches" do
    browser = jobs.fetch("browser_grpc_web")
    expect(browser.fetch("env")).to include("GOMODCACHE" => "${{ github.workspace }}/vendor/browser/go/mod",
                                            "GOCACHE" => "${{ github.workspace }}/vendor/browser/go/build")
    go = browser.fetch("steps").find { |step| step.fetch("uses", "").start_with?("actions/setup-go@") }
    expect(go).not_to be_nil
    expect(go.fetch("uses")).to eq("actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e")
    expect(go.fetch("with")).to include("go-version" => "1.27.3", "check-latest" => false, "cache" => true,
                                        "cache-dependency-path" => "browser/reference/go.sum")
  end

  it "caches RBS dependency inputs while always running fresh source and installed checks" do
    steps = jobs.fetch("rbs").fetch("steps")
    cache = steps.find { |step| step.fetch("uses", "").start_with?("actions/cache@") }
    expect(cache.fetch("uses")).to eq("actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9")
    settings = cache.fetch("with")
    expect(settings.fetch("path").lines.map(&:strip)).to eq(
      %w[vendor/bundle vendor/rbs/bundle vendor/rack/bundle vendor/testing/bundle
         vendor/async/bundle vendor/falcon/bundle vendor/otel/bundle]
    )
    expect(settings).not_to have_key("restore-keys")
    expect(settings.fetch("key")).to include("rbs-bundles-v1", "runner.os", "runner.arch",
                                             "steps.rbs-platform.outputs.platform", "bundler-4.0.23", "hashFiles(",
                                             "'Gemfile*'", "'connectrpc.gemspec'", "'typecheck/Gemfile*'",
                                             "'gems/**/Gemfile*'", "'gems/**/*.gemfile*'", "'gems/**/*.gemspec'")
    checks = steps.select { |step| step.fetch("name", "").start_with?("Run rbs:") }
    expect(checks.map { |step| step.fetch("run") }).to eq(["mise run rbs:check", "mise run rbs:package"])
    checks.each { |step| expect(step).not_to have_key("if") }
    expect(cache).not_to have_key("env")
    ruby = steps.find { |step| step.fetch("uses", "").start_with?("ruby/setup-ruby@") }
    expect(ruby).not_to have_key("env")
  end
end

RSpec.describe "RBS native cache platform" do
  it "binds the actual Ruby ABI/platform and requires the actual runner image version" do
    script = File.expand_path("../typecheck/cache_platform.rb", __dir__)
    Dir.mktmpdir("connectrpc-cache-platform-") do |working|
      destination = File.join(working, "outputs")
      environment = { "CI_EXPECTED_RUBY_VERSION" => RUBY_VERSION, "ImageOS" => "ubuntu24",
                      "ImageVersion" => "20261026.354.1", "GITHUB_OUTPUT" => destination }
      _output, error, status = Open3.capture3(environment, RbConfig.ruby, script)
      expect(status.success?).to be(true), error
      expected = [RUBY_VERSION, RbConfig::CONFIG.fetch("ruby_version"), Gem::Platform.local.to_s,
                  "ubuntu24", "20261026.354.1"].join("-")
      expect(File.read(destination)).to eq("platform=#{expected}\n")
      [{ "ImageVersion" => nil }, { "ImageVersion" => "" }, { "ImageOS" => "ubuntu\n24" },
       { "CI_EXPECTED_RUBY_VERSION" => "0.0.0" }].each do |override|
        File.unlink(destination)
        _output, _error, status = Open3.capture3(environment.merge(override), RbConfig.ruby, script)
        expect(status.success?).to be(false)
        expect(File.exist?(destination)).to be(false)
        File.write(destination, "")
      end
    end
  end
end

RSpec.describe "CI Mise task replacement" do
  it "preserves the complete interop context while replacing runtime installation" do
    tasks = [nil, "ci"].map do |mode|
      output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info", "setup:interop", "--json")
      expect(status.success?).to be(true), error
      JSON.parse(output)
    end
    local, ci = tasks
    %w[tools env dir depends depends_post].each { |field| expect(ci.fetch(field)).to eq(local.fetch(field)) }
    expect(ci.fetch("description")).not_to be_empty
    expect(local.fetch("run")).to eq(["mise install --locked go@1.27.3", "go mod download"])
    expect(ci.fetch("run")).to eq(["ruby {{config_root}}/scripts/ci_runtime.rb interop", "go mod download"])
  end

  it "preserves browser reference tools, module, caches and build commands across the CI override" do
    local, ci = [nil, "ci"].map do |mode|
      output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info",
                                             "browser:reference:setup", "--json")
      expect(status.success?).to be(true), error
      JSON.parse(output)
    end
    %w[tools env dir depends depends_post].each { |field| expect(ci.fetch(field)).to eq(local.fetch(field)) }
    expect(local.fetch("tools")).to eq("go" => "1.27.3")
    expect(local.fetch("dir")).to eq(File.expand_path("../browser/reference", __dir__))
    expected_env = ["GOTOOLCHAIN=local", "GOWORK=off", "GOFLAGS=-mod=readonly",
                    "GOMODCACHE={{config_root}}/vendor/browser/go/mod",
                    "GOCACHE={{config_root}}/vendor/browser/go/build"]
    expect(local.fetch("env")).to match_array(expected_env)
    build = ["go mod download all", "go mod verify", "ruby ../../scripts/check_web_reference.rb",
             "go build -o ../../vendor/browser/reference ."]
    expect(local.fetch("run")).to eq(["mise install --locked go@1.27.3", *build])
    expect(ci.fetch("run")).to eq(["ruby {{config_root}}/scripts/ci_runtime.rb browser-reference", *build])
  end
end

RSpec.describe "Optional Rails task context" do
  it "keeps frozen Rails setup independent of root setup and preserves the task context in CI" do
    ["setup:rails", "test:rails", "generation:check:rails", "build:rails", "check:package:rails"].each do |name|
      tasks = [nil, "ci"].map do |mode|
        output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info", name, "--json")
        expect(status.success?).to be(true), error
        JSON.parse(output)
      end
      local, ci = tasks
      %w[env tools dir depends run].each { |field| expect(ci.fetch(field)).to eq(local.fetch(field)) }
      expect(local.fetch("env")).to include("BUNDLE_COOLDOWN=7")
      expect(local.fetch("depends")).not_to include("setup")
      expect(local.fetch("depends")).to eq(["setup:bundler"]) if name == "setup:rails"
    end
  end
end

RSpec.describe "Task-local tool activation" do
  it "runs tool-owning commands directly so nested Mise cannot remove their tools" do
    output, error, status = Open3.capture3("mise", "tasks", "--json")
    expect(status.success?).to be(true), error
    tasks = JSON.parse(output)
    tasks.reject { |task| task.fetch("tools", {}).empty? }.each do |task|
      expect(task.fetch("run")).not_to include(a_string_matching(/\bmise exec\b/)), task.fetch("name")
    end
  end
end

RSpec.shared_examples "Optional backend CI boundary" do |backend_name, label|
  let(:root) { File.expand_path("..", __dir__) }

  it "uses independent frozen bundles and separate backend commands in both Ruby lanes" do
    jobs = YAML.load_file(File.join(root, ".github/workflows/ci.yml")).fetch("jobs")
    ["#{backend_name}_specs", "#{backend_name}_interop"].each do |name|
      job = jobs.fetch(name)
      expect(job.fetch("env")).to include(
        "BUNDLE_GEMFILE" => "${{ github.workspace }}/gems/connectrpc-#{backend_name}/Gemfile",
        "BUNDLE_PATH" => "${{ github.workspace }}/vendor/#{backend_name}/bundle",
        "BUNDLE_USER_HOME" => "${{ github.workspace }}/vendor/#{backend_name}/bundler", "BUNDLE_COOLDOWN" => "7"
      )
    end
    commands = jobs.transform_values { |job| job.fetch("steps").filter_map { |step| step["run"] } }
    expected = ["mise run #{backend_name}:test", "mise run #{backend_name}:package"]
    expect(commands.fetch("#{backend_name}_specs").first(2)).to eq(expected)
    expect(commands.fetch("#{backend_name}_specs")).to include("mise run #{backend_name}:conformance")
    expect(commands.fetch("#{backend_name}_interop").first(2)).to eq(
      ["mise run #{backend_name}:interop", "mise run #{backend_name}:interop:streaming"]
    )
    expect(jobs.fetch("#{backend_name}_interop").fetch("env")).to include(
      "GOMODCACHE" => "${{ github.workspace }}/vendor/interop/mod",
      "GOCACHE" => "${{ github.workspace }}/vendor/interop/build"
    )
  end

  it "preserves complete backend Mise contexts while replacing only runtime setup in CI" do
    %w[tools setup test check build package interop interop:streaming conformance].each do |task|
      name = "#{backend_name}:#{task}"
      local, ci = [nil, "ci"].map do |mode|
        output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info", name, "--json")
        expect(status.success?).to be(true), error
        JSON.parse(output)
      end
      %w[tools env dir depends depends_post].each { |key| expect(ci.fetch(key)).to eq(local.fetch(key)) }
      if name == "#{backend_name}:tools"
        expect(ci.fetch("run")).to eq(["ruby scripts/ci_runtime.rb"])
        expect(local.fetch("run")).to eq(["mise install --locked ruby",
                                          "mise exec -- ruby ../../scripts/setup_bundler.rb"])
      else
        expect(ci.fetch("run")).to eq(local.fetch("run"))
      end
      expect(local.fetch("depends")).to eq(["#{backend_name}:tools"]) if name == "#{backend_name}:setup"
    end
  end

  it "accepts exact backend runtime paths and rejects bundle/Gemfile/directory drift" do
    ruby = File.realpath(RbConfig.ruby)
    backend = File.join(root, "gems/connectrpc-#{backend_name}")
    environment = {
      "RUNNER_TOOL_CACHE" => File.dirname(ruby, 2), "CI_EXPECTED_RUBY_VERSION" => RUBY_VERSION,
      "PATH" => "#{File.dirname(ruby)}:#{ENV.fetch('PATH')}",
      "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_FROZEN" => "true", "BUNDLE_DISABLE_SHARED_GEMS" => "true",
      "BUNDLE_VERSION" => "4.0.23", "BUNDLE_COOLDOWN" => "7",
      "BUNDLE_PATH" => File.join(root, "vendor/#{backend_name}/bundle"),
      "BUNDLE_GEMFILE" => File.join(backend, "Gemfile"),
      "BUNDLE_USER_HOME" => File.join(root, "vendor/#{backend_name}/bundler")
    }
    script = File.join(backend, "scripts/ci_runtime.rb")
    Bundler.with_unbundled_env do
      output, error, status = Open3.capture3(environment, ruby, script, chdir: backend)
      expect(status.success?).to be(true), error
      expect(output).to include("#{label} bundle path:", "/vendor/#{backend_name}/bundle")
      %w[BUNDLE_GEMFILE BUNDLE_PATH BUNDLE_USER_HOME BUNDLE_COOLDOWN].each do |key|
        _output, error, status = Open3.capture3(environment.merge(key => "/tmp/wrong"), ruby, script, chdir: backend)
        expect(status.success?).to be(false)
        expect(error).to include("unexpected #{key}")
      end
      _output, error, status = Open3.capture3(environment, ruby, script, chdir: root)
      expect(status.success?).to be(false)
      expect(error).to include("unexpected working directory")
    end
  end
end

RSpec.describe "Optional Async CI boundary" do
  include_examples "Optional backend CI boundary", "async", "Async"
end

RSpec.describe "Optional Falcon CI boundary" do
  include_examples "Optional backend CI boundary", "falcon", "Falcon"
end

RSpec.describe "Optional Rack CI boundary" do
  let(:root) { File.expand_path("..", __dir__) }

  it "uses independent frozen bundles and separate middleware commands in both Ruby lanes" do
    jobs = YAML.load_file(File.join(root, ".github/workflows/ci.yml")).fetch("jobs")
    %w[rack].each do |name|
      job = jobs.fetch(name)
      expect(job.fetch("env")).to include(
        "BUNDLE_GEMFILE" => "${{ github.workspace }}/gems/connectrpc-rack/Gemfile",
        "BUNDLE_PATH" => "${{ github.workspace }}/vendor/rack/bundle",
        "BUNDLE_USER_HOME" => "${{ github.workspace }}/vendor/rack/bundler", "BUNDLE_COOLDOWN" => "7"
      )
    end
    commands = jobs.transform_values { |job| job.fetch("steps").filter_map { |step| step["run"] } }
    expect(commands.fetch("rack").first(3)).to eq(["mise run rack:test", "mise run rack:check",
                                                   "mise run rack:package"])
  end

  it "preserves complete middleware Mise contexts while replacing only runtime setup in CI" do
    %w[rack:tools rack:setup rack:test rack:check rack:build rack:package].each do |name|
      local, ci = [nil, "ci"].map do |mode|
        output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info", name, "--json")
        expect(status.success?).to be(true), error
        JSON.parse(output)
      end
      %w[tools env dir depends depends_post].each { |key| expect(ci.fetch(key)).to eq(local.fetch(key)) }
      if name == "rack:tools"
        expect(ci.fetch("run")).to eq(["ruby scripts/ci_runtime.rb"])
        expect(local.fetch("run")).to eq(["mise install --locked ruby",
                                          "mise exec -- ruby ../../scripts/setup_bundler.rb"])
      else
        expect(ci.fetch("run")).to eq(local.fetch("run"))
      end
      expect(local.fetch("depends")).to eq(["rack:tools"]) if name == "rack:setup"
    end
  end

  it "accepts exact middleware runtime paths and rejects bundle/Gemfile/directory drift" do
    ruby = File.realpath(RbConfig.ruby)
    middleware = File.join(root, "gems/connectrpc-rack")
    environment = {
      "RUNNER_TOOL_CACHE" => File.dirname(ruby, 2), "CI_EXPECTED_RUBY_VERSION" => RUBY_VERSION,
      "PATH" => "#{File.dirname(ruby)}:#{ENV.fetch('PATH')}",
      "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_FROZEN" => "true", "BUNDLE_DISABLE_SHARED_GEMS" => "true",
      "BUNDLE_VERSION" => "4.0.23", "BUNDLE_COOLDOWN" => "7",
      "BUNDLE_PATH" => File.join(root, "vendor/rack/bundle"), "BUNDLE_GEMFILE" => File.join(middleware, "Gemfile"),
      "BUNDLE_USER_HOME" => File.join(root, "vendor/rack/bundler")
    }
    script = File.join(middleware, "scripts/ci_runtime.rb")
    Bundler.with_unbundled_env do
      output, error, status = Open3.capture3(environment, ruby, script, chdir: middleware)
      expect(status.success?).to be(true), error
      expect(output).to include("Rack bundle path:", "/vendor/rack/bundle")
      %w[BUNDLE_GEMFILE BUNDLE_PATH BUNDLE_USER_HOME BUNDLE_COOLDOWN].each do |key|
        _output, error, status = Open3.capture3(environment.merge(key => "/tmp/wrong"), ruby, script, chdir: middleware)
        expect(status.success?).to be(false)
        expect(error).to include("unexpected #{key}")
      end
      _output, error, status = Open3.capture3(environment, ruby, script, chdir: root)
      expect(status.success?).to be(false)
      expect(error).to include("unexpected working directory")
    end
  end
end

RSpec.describe "Optional OpenTelemetry CI boundary" do
  it "keeps the independent context and CI override aligned" do
    root = File.expand_path("..", __dir__)
    local, ci = [nil, "ci"].map do |mode|
      output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info", "otel:tools", "--json")
      expect(status.success?).to be(true), error
      JSON.parse(output)
    end
    %w[tools env dir depends depends_post].each { |key| expect(ci.fetch(key)).to eq(local.fetch(key)) }
    expect(ci.fetch("run")).to eq(["ruby scripts/ci_runtime.rb"])
    expect(local.fetch("run")).to eq(["mise exec -- ruby scripts/tools.rb"])
    expect(File.read(File.join(root, "gems/connectrpc-otel/scripts/ci_runtime.rb"))).to include("scripts/ci_runtime.rb")
  end
end
