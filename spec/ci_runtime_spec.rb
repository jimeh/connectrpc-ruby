# frozen_string_literal: true

require "json"
require "open3"
require "rbconfig"
require "tmpdir"
require "yaml"
require_relative "../scripts/dependency_floors"
require_relative "../scripts/release_train"

RSpec.describe "CI runtime boundary" do
  let(:root) { File.expand_path("..", __dir__) }
  let(:ruby) { File.realpath(RbConfig.ruby) }
  let(:environment) do
    {
      "RUNNER_TOOL_CACHE" => File.dirname(ruby, 2),
      "CI_EXPECTED_RUBY_VERSION" => RUBY_VERSION,
      "PATH" => "#{File.dirname(ruby)}:#{ENV.fetch('PATH')}",
      "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_DISABLE_SHARED_GEMS" => "true", "BUNDLE_VERSION" => "4.0.23",
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

  it "rejects bundle settings that bypass the cached path or require a committed lock" do
    { "BUNDLE_PATH" => "/tmp/wrong-bundle", "BUNDLE_FROZEN" => "true", "BUNDLE_DEPLOYMENT" => "true",
      "BUNDLE_LOCKFILE" => "/tmp/other.lock", "BUNDLE_IGNORE_CONFIG" => "false",
      "BUNDLE_VERSION" => "0.0.0" }.each do |key, value|
      _output, error, status = guard(key => value)
      expect(status.success?).to be(false)
      expect(error).to include("unexpected #{key}")
    end
  end

  def go_modes
    { "interop" => %w[interop interop], "browser-reference" => %w[browser/reference browser/go],
      "hyper" => %w[gems/connectrpc-hyper/interop hyper/go] }
  end

  def go_guard(mode, context_overrides = {}, directory: go_modes.fetch(mode).first, outside_cache: false)
    module_dir, cache_dir = go_modes.fetch(mode)
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

  %w[interop browser-reference hyper].each do |mode|
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
  let(:all_jobs) { workflow.fetch("jobs") }
  # The hyper transport's jobs, including the reusable archive workflow and
  # the dependent Windows job, have their own mapping below.
  let(:hyper_jobs) { %w[hyper hyper_msrv hyper_gems hyper_windows] }
  let(:jobs) { all_jobs.except(*hyper_jobs) }

  it "preserves all 28 existing checks and adds browser, metrics, health, Sentry and lowest-allowed leaves" do
    expect(all_jobs.keys).to include(*hyper_jobs)
    core = %w[static specs package interop conformance]
    backend = %w[async_specs async_interop]
    falcon = %w[falcon_specs falcon_interop]
    metrics = %w[prometheus yabeda]
    expect(jobs.keys).to match_array([*core, "rails", "floors", *backend, "rack", "testing", "rbs", *falcon, "otel",
                                      *metrics, "health", "sentry", "browser_grpc_web", "examples"])
    count = lambda do |names|
      jobs.slice(*names).values.sum do |job|
        matrix = job.dig("strategy", "matrix") || {}
        matrix["ruby"]&.size || matrix["include"]&.size || matrix["leaf"]&.size || 1
      end
    end
    expect(count.call(core)).to eq(9)
    expect(count.call(["rails"])).to eq(3)
    expect(count.call(backend)).to eq(4)
    expect(count.call(falcon)).to eq(4)
    expect(count.call(["rack"])).to eq(2)
    expect(count.call(["testing"])).to eq(2)
    expect(count.call(["rbs"])).to eq(2)
    expect(count.call(jobs.keys - [*metrics, "health", "sentry", "browser_grpc_web", "examples", "floors"])).to eq(28)
    expect(count.call(["otel"])).to eq(2)
    expect(count.call(["browser_grpc_web"])).to eq(2)
    expect(count.call(metrics)).to eq(4)
    expect(count.call(["health"])).to eq(2)
    expect(count.call(["sentry"])).to eq(2)
    expect(count.call(["floors"])).to eq(10)
    expect(count.call(jobs.keys - ["examples"])).to eq(48)
    jobs.each_value { |job| expect(job).not_to have_key("needs") }
    jobs.except("static", "rails", "examples", "floors").each_value do |job|
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
    metrics.each { |name| expect(commands.fetch(name).first).to eq("mise run #{name}:verify") }
    expect(commands.fetch("health").first).to eq("mise run health:verify")
    expect(commands.fetch("sentry").first).to eq("mise run sentry:verify")
    expect(commands.fetch("browser_grpc_web").first).to eq("mise run browser:verify")
    expect(commands.fetch("browser_grpc_web")[1]).to include("mise run async:conformance:web",
                                                             "mise run falcon:conformance:web",
                                                             "mise run rack:conformance:web")
    expect(commands.fetch("rbs").first).to include("libclang-dev")
    expect(commands.fetch("rbs").drop(1).first(3)).to eq(["mise run rbs:cache-platform", "mise run rbs:check",
                                                          "mise run rbs:package"])
  end

  it "runs daily and resolves fresh bundles without frozen mode" do
    triggers = workflow.fetch("on", workflow[true])
    expect(triggers.fetch("schedule")).to eq([{ "cron" => "37 4 * * *" }])
    expect(triggers).to include("pull_request", "push", "workflow_dispatch")
    expect(workflow.dig("concurrency", "group")).to include("github.event_name", "github.ref")
    expect(workflow.fetch("env")).not_to include("BUNDLE_FROZEN", "BUNDLE_DEPLOYMENT", "BUNDLE_LOCKFILE")
    cooldowns = jobs.values.filter_map { |job| job.dig("env", "BUNDLE_COOLDOWN") }.uniq
    expect(cooldowns).to eq(["3"])
  end

  it "runs each lowest-allowed leaf on Ruby 3.4.12 through its Mise floors task" do
    floors = jobs.fetch("floors")
    expect(floors.dig("strategy", "fail-fast")).to be(false)
    expect(floors.dig("strategy", "matrix", "leaf"))
      .to eq(%w[async falcon rack rails otel prometheus yabeda health sentry hyper])
    expect(floors.fetch("env")).to include("CI_EXPECTED_RUBY_VERSION" => "3.4.12", "BUNDLE_COOLDOWN" => "3")
    ruby = floors.fetch("steps").find { |step| step.fetch("uses", "").start_with?("ruby/setup-ruby@") }
    expect(ruby.fetch("with")).to include("ruby-version" => "3.4.12", "bundler-cache" => false)
    step = floors.fetch("steps").find { |entry| entry["run"]&.include?("floors:") }
    expect(step).to include("run" => 'mise run "floors:$FLOORS_LEAF"',
                            "env" => { "FLOORS_LEAF" => "${{ matrix.leaf }}" })
    output, error, status = Open3.capture3("mise", "tasks", "--json")
    expect(status.success?).to be(true), error
    tasks = JSON.parse(output).to_h { |task| [task.fetch("name"), task] }
    floors.dig("strategy", "matrix", "leaf").each do |leaf|
      task = tasks.fetch("floors:#{leaf}")
      expect(task.fetch("env")).to include("CONNECTRPC_DEPENDENCY_FLOORS=1")
      expect(task.fetch("run")).to include(a_string_ending_with(" setup"), a_string_ending_with(" floors"),
                                           a_string_ending_with(" test"), a_string_ending_with(" package"))
    end
    expect(tasks.fetch("floors:rails").fetch("env")).to include("RAILS_LANE=8.0")
  end

  it "runs a floors leaf for every versioned package with third-party runtime dependencies" do
    expected = ReleaseTrain::NAMES.reject { |name| DependencyFloors.floors([name]).empty? }.map do |name|
      name == "connectrpc-on-rails" ? "rails" : name.delete_prefix("connectrpc-")
    end
    expect(jobs.dig("floors", "strategy", "matrix", "leaf")).to match_array(expected)
    output, error, status = Open3.capture3("mise", "tasks", "info", "floors", "--json")
    expect(status.success?).to be(true), error
    expect(JSON.parse(output).fetch("depends")).to match_array(expected.map { |leaf| "floors:#{leaf}" })
  end

  it "runs every runnable example pairing once, on one Ruby lane" do
    examples = jobs.fetch("examples")
    expect(examples).not_to have_key("strategy")
    expect(examples.fetch("env")).to include("CI_EXPECTED_RUBY_VERSION" => "4.0.9")
    steps = examples.fetch("steps")
    ruby = steps.find { |step| step.fetch("uses", "").start_with?("ruby/setup-ruby@") }
    expect(ruby.fetch("with")).to include("ruby-version" => "4.0.9")
    cache = steps.find { |step| step.fetch("uses", "").start_with?("actions/cache@") }
    expect(cache.dig("with", "path").lines.map(&:strip)).to eq(%w[vendor/rack/bundle vendor/async/bundle
                                                                  vendor/falcon/bundle])
    expect(cache.dig("with", "key")).to include("steps.bundle-platform.outputs.platform", "hashFiles(")
    expect(steps.filter_map { |step| step["run"] }.first(2)).to eq(["mise run rbs:cache-platform",
                                                                    "mise run example:verify"])
  end

  it "pins the three Rails Appraisal lanes and caches the selected lane lock" do
    rails = jobs.fetch("rails")
    expected = [
      { "rails" => "8.0", "ruby" => "3.4.12" }, { "rails" => "8.1", "ruby" => "3.4.12" },
      { "rails" => "8.1", "ruby" => "4.0.9" }
    ]
    expect(rails.dig("strategy", "matrix", "include")).to eq(expected)
    expect(rails.fetch("env")).to include("BUNDLE_COOLDOWN" => "3", "RAILS_LANE" => "${{ matrix.rails }}")
    setup = rails.fetch("steps").find { |step| step.fetch("uses", "").start_with?("ruby/setup-ruby@") }
    gemfile = "${{ github.workspace }}/gems/connectrpc-on-rails/gemfiles/rails_${{ matrix.rails }}.gemfile"
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
      expect(ruby.fetch("with")).to include("bundler" => "4.0.23", "bundler-cache" => !%w[rbs floors].include?(name),
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

  it "provides action-owned pinned Go to the health leaf for grpc-go interop" do
    go = jobs.fetch("health").fetch("steps").find { |step| step.fetch("uses", "").start_with?("actions/setup-go@") }
    expect(go.fetch("uses")).to eq("actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e")
    expect(go.fetch("with")).to include("go-version" => "1.27.3", "check-latest" => false, "cache" => true,
                                        "cache-dependency-path" => "interop/grpc/go.sum")
  end

  it "caches RBS dependency inputs while always running fresh source and installed checks" do
    steps = jobs.fetch("rbs").fetch("steps")
    cache = steps.find { |step| step.fetch("uses", "").start_with?("actions/cache@") }
    expect(cache.fetch("uses")).to eq("actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9")
    settings = cache.fetch("with")
    expect(settings.fetch("path").lines.map(&:strip)).to eq(
      %w[vendor/bundle vendor/rbs/bundle vendor/rack/bundle vendor/testing/bundle
         vendor/async/bundle vendor/falcon/bundle vendor/otel/bundle vendor/prometheus/bundle
         vendor/yabeda/bundle vendor/health/bundle vendor/sentry/bundle vendor/hyper/bundle]
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
  it "keeps Rails setup independent of root setup and preserves the task context in CI" do
    ["setup:rails", "test:rails", "generation:check:rails", "build:rails", "check:package:rails"].each do |name|
      tasks = [nil, "ci"].map do |mode|
        output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info", name, "--json")
        expect(status.success?).to be(true), error
        JSON.parse(output)
      end
      local, ci = tasks
      %w[env tools dir depends run].each { |field| expect(ci.fetch(field)).to eq(local.fetch(field)) }
      expect(local.fetch("env")).to include("BUNDLE_COOLDOWN=3")
      expect(local.fetch("depends")).not_to include("setup")
      expect(local.fetch("depends")).to eq(["setup:bundler"]) if name == "setup:rails"
    end
  end
end

RSpec.describe "Pins-lifted canary workflow" do
  let(:workflows) { File.expand_path("../.github/workflows", __dir__) }
  let(:workflow) { YAML.load_file(File.join(workflows, "canary.yml")) }
  let(:ci) { YAML.load_file(File.join(workflows, "ci.yml")) }
  let(:job) { workflow.fetch("jobs").fetch("canary") }
  let(:tasks) do
    output, error, status = Open3.capture3("mise", "tasks", "--json")
    expect(status.success?).to be(true), error
    JSON.parse(output).to_h { |task| [task.fetch("name"), task] }
  end

  it "never gates pull requests or main and runs daily apart from CI" do
    triggers = workflow.fetch("on", workflow[true])
    expect(triggers.keys).to contain_exactly("schedule", "workflow_dispatch")
    cron = triggers.fetch("schedule").map { |entry| entry.fetch("cron") }
    expect(cron.size).to eq(1)
    expect(cron.first).to match(/\A\d+ \d+ \* \* \*\z/)
    ci_cron = ci.fetch("on", ci[true]).fetch("schedule").first.fetch("cron")
    expect(cron.first.split.first(2)).not_to eq(ci_cron.split.first(2))
    expect(workflow.fetch("permissions")).to eq("contents" => "read")
    expect(workflow.fetch("env")).to eq(ci.fetch("env"))
    ci_commands = ci.fetch("jobs").values.flat_map do |entry|
      entry.fetch("steps", []).filter_map { |step| step["run"] }
    end
    expect(ci_commands.join("\n")).not_to include("canary")
  end

  it "runs each leaf on Ruby 3.4.12 through its Mise canary task with the floors job's pinned actions" do
    expect(workflow.fetch("jobs").keys).to eq(["canary"])
    expect(job.fetch("timeout-minutes")).to be_between(1, 90)
    expect(job.dig("strategy", "fail-fast")).to be(false)
    expect(job.dig("strategy", "matrix", "leaf")).to eq(%w[async falcon hyper])
    expect(job.fetch("env")).to eq("CI_EXPECTED_RUBY_VERSION" => "3.4.12", "BUNDLE_COOLDOWN" => "3")
    actions = ->(steps) { steps.filter_map { |step| step.slice("uses", "with") if step["uses"] } }
    expect(actions.call(job.fetch("steps"))).to eq(actions.call(ci.dig("jobs", "floors", "steps")))
    step = job.fetch("steps").find { |entry| entry["run"]&.include?("canary:") }
    expect(step).to include("run" => 'mise run "canary:$CANARY_LEAF"',
                            "env" => { "CANARY_LEAF" => "${{ matrix.leaf }}" })
    expect(job.fetch("steps").last).to include("if" => "always()")
    expect(job.fetch("steps").last.fetch("run")).to include("git status --porcelain --untracked-files=all")
  end

  it "maps each gem leaf to a fresh canary-mode task that never builds packages, and aggregates every leaf" do
    (job.dig("strategy", "matrix", "leaf") - ["hyper"]).each do |leaf|
      task = tasks.fetch("canary:#{leaf}")
      expect(task.fetch("description")).to start_with("Non-gating pins-lifted canary")
      expect(task.fetch("depends")).to eq(["#{leaf}:tools"])
      expect(task.fetch("env")).to include("CONNECTRPC_DEPENDENCY_CANARY=1")
      launcher = "mise exec -- ruby gems/connectrpc-#{leaf}/scripts/run.rb"
      expect(task.fetch("run")).to eq(["rm -f gems/connectrpc-#{leaf}/Gemfile.canary.lock", "#{launcher} setup",
                                       "#{launcher} canary"])
    end
    expect(tasks.fetch("canary").fetch("depends")).to contain_exactly("canary:async", "canary:falcon", "canary:hyper")
  end

  # The tracked tree must stay clean, so the crate update happens in a copy.
  it "updates hyper's locked crates only in a temporary copy, then rebuilds and runs the specs" do
    task = tasks.fetch("canary:hyper")
    expect(task.fetch("description")).to start_with("Non-gating newest-crates canary")
    expect(task.fetch("depends")).to eq(["hyper:tools", "hyper:peer"])
    expect(task.fetch("tools")).to eq("rust" => "1.98.0")
    expect(task.fetch("run")).to eq(["ruby gems/connectrpc-hyper/scripts/canary.rb"])
    script = File.read(File.expand_path("../gems/connectrpc-hyper/scripts/canary.rb", __dir__))
    expect(script).to include('File.join(root, "vendor/hyper/canary/tree")', '"cargo", "update"')
    hyper_steps = job.fetch("steps").select { |step| step["if"] == "matrix.leaf == 'hyper'" }
    expect(hyper_steps.map { |step| step["uses"]&.split("@")&.first || step.fetch("run")[/libclang-dev/] })
      .to eq(%w[libclang-dev actions/setup-go])
  end
end

RSpec.describe "Hyper CI mapping" do
  let(:root) { File.expand_path("..", __dir__) }
  let(:jobs) { YAML.load_file(File.join(root, ".github/workflows/ci.yml")).fetch("jobs") }
  let(:gems_workflow) { YAML.load_file(File.join(root, ".github/workflows/hyper-gems.yml")) }
  let(:gems_jobs) { gems_workflow.fetch("jobs") }
  let(:commands) { jobs.transform_values { |job| job.fetch("steps", []).filter_map { |step| step["run"] } } }
  let(:gems_commands) { gems_jobs.transform_values { |job| job.fetch("steps").filter_map { |step| step["run"] } } }
  let(:hyper_env) do
    { "BUNDLE_GEMFILE" => "${{ github.workspace }}/gems/connectrpc-hyper/Gemfile",
      "BUNDLE_PATH" => "${{ github.workspace }}/vendor/hyper/bundle",
      "BUNDLE_USER_HOME" => "${{ github.workspace }}/vendor/hyper/bundler", "BUNDLE_COOLDOWN" => "3" }
  end

  def step(job, action) = jobs.fetch(job).fetch("steps").find { |entry| entry.fetch("uses", "").start_with?(action) }

  it "verifies the gem on both Rubies with action-owned Go, libclang and the independent bundle" do
    job = jobs.fetch("hyper")
    expect(job.dig("strategy", "matrix", "ruby")).to eq(%w[3.4.12 4.0.9])
    expect(job.fetch("env")).to include(hyper_env)
    expect(job.fetch("env")).to include("GOMODCACHE" => "${{ github.workspace }}/vendor/hyper/go/mod",
                                        "GOCACHE" => "${{ github.workspace }}/vendor/hyper/go/build")
    expect(step("hyper", "actions/setup-go@").fetch("with"))
      .to include("go-version" => "1.27.3", "cache-dependency-path" => "gems/connectrpc-hyper/interop/go.sum")
    expect(commands.fetch("hyper").first).to include("libclang-dev")
    expect(commands.fetch("hyper")[1]).to eq("mise run hyper:verify")
    expect(commands.fetch("hyper_msrv")).to include("mise run hyper:msrv")
    %w[floors rbs].each { |name| expect(commands.fetch(name).join("\n")).to include("libclang-dev") }
  end

  def gems_step(job, action)
    gems_jobs.fetch(job).fetch("steps").find { |entry| entry.fetch("uses", "").start_with?(action) }
  end

  it "calls the reusable archive workflow read-only and runs the Windows specs after it" do
    caller = jobs.fetch("hyper_gems")
    expect(caller).to eq("name" => "Hyper gems", "uses" => "./.github/workflows/hyper-gems.yml",
                         "permissions" => { "contents" => "read" })
    expect(jobs.fetch("hyper_windows").fetch("needs")).to eq("hyper_gems")
    expect(gems_workflow.fetch("on", gems_workflow[true]).keys).to eq(["workflow_call"])
    expect(gems_workflow.fetch("permissions")).to eq("contents" => "read")
    # Called workflows do not inherit the caller's env; the runtime guard needs these.
    expect(gems_workflow.fetch("env")).to eq(YAML.load_file(File.join(root, ".github/workflows/ci.yml")).fetch("env"))
  end

  it "builds every Rakefile platform gem and the source gem twice without caches, uploaded under one name each" do
    require_relative "../gems/connectrpc-hyper/scripts/platforms"
    job = gems_jobs.fetch("build")
    expect(job.dig("strategy", "matrix", "platform")).to eq([*HyperPlatforms::NATIVE, "ruby"])
    expect(job.fetch("env")).to include(hyper_env)
    cross = job.fetch("steps").find { |entry| entry["run"] == "mise run hyper:cross" }
    expect(cross.fetch("env")).to eq("PLATFORM" => "${{ matrix.platform }}", "HYPER_CROSS_TWICE" => "1")
    expect(gems_step("build", "ruby/setup-ruby@").fetch("with")).to include("bundler-cache" => false)
    expect(gems_step("build", "jdx/mise-action@").fetch("with")).to include("cache" => false)
    upload = gems_step("build", "actions/upload-artifact@").fetch("with")
    expect(upload).to include("name" => "connectrpc-hyper-${{ matrix.platform }}", "if-no-files-found" => "error")
    caches = gems_jobs.values.flat_map { |entry| entry.fetch("steps", []) }
                      .filter_map { |step| step["with"]&.slice("bundler-cache", "cache") }.reject(&:empty?)
    expect(caches.uniq).to contain_exactly({ "bundler-cache" => false }, { "cache" => false })
    expect(gems_jobs.values.flat_map { |entry| entry.fetch("steps", []) }
                    .select { |step| step.fetch("uses", "").start_with?("actions/cache@") }).to be_empty
  end

  it "smokes every archive on its native runner for both Ruby minors, gating, and records evidence" do
    smoke = %w[smoke_linux smoke_musl smoke_darwin smoke_windows smoke_source]
    legs = smoke.flat_map do |name|
      gems_jobs.dig(name, "strategy", "matrix", "include").map do |entry|
        [entry.fetch("platform", "ruby"), entry.fetch("runner", gems_jobs.dig(name, "runs-on")), entry.fetch("minor")]
      end
    end
    runners = { "x86_64-linux" => "ubuntu-24.04", "aarch64-linux" => "ubuntu-24.04-arm",
                "x86_64-linux-musl" => "ubuntu-24.04", "aarch64-linux-musl" => "ubuntu-24.04-arm",
                "arm64-darwin" => "macos-15", "x86_64-darwin" => "macos-15-intel",
                "x64-mingw-ucrt" => "windows-2025", "aarch64-mingw-ucrt" => "windows-11-arm", "ruby" => "ubuntu-24.04" }
    expect(runners.keys).to eq([*HyperPlatforms::NATIVE, "ruby"])
    expect(legs).to match_array(runners.flat_map do |platform, runner|
      HyperPlatforms::RUBY_MINORS.map { |minor| [platform, runner, minor] }
    end)
    musl = gems_jobs.dig("smoke_musl", "strategy", "matrix", "include").map do |entry|
      entry.values_at("image", "minor")
    end
    expect(musl.uniq).to contain_exactly(%w[ruby:3.4-alpine 3.4], %w[ruby:4.0-alpine 4.0])
    smoke.each do |name|
      job = gems_jobs.fetch(name)
      expect(job.fetch("needs")).to eq("build")
      expect(job.dig("strategy", "fail-fast")).to be(false)
      expect(gems_step(name, "actions/download-artifact@").fetch("uses"))
        .to eq("actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333")
      expect(gems_commands.fetch(name).join("\n")).to match(/install_smoke\.rb|hyper:smoke:source/)
        .and include("vendor/hyper/evidence/")
      upload = gems_step(name, "actions/upload-artifact@").fetch("with")
      expect(upload.fetch("name")).to start_with("hyper-smoke-")
      expect(upload).to include("if-no-files-found" => "error")
      expect(job.fetch("steps").last.fetch("run")).to include("git status --porcelain")
    end
    expect(gems_commands.fetch("smoke_source").join("\n")).to include("mise run hyper:smoke:source --")
  end

  it "checks the release candidate from the publish job's inputs, without privileges, after every smoke" do
    job = gems_jobs.fetch("candidate")
    expect(job.fetch("name")).to eq("Check the release candidate")
    expect(job.fetch("needs")).to match_array(gems_jobs.keys - %w[candidate verified])
    expect(job.keys).not_to include("permissions", "environment", "secrets")
    expect(job.to_s).not_to include("secrets.", "id-token")
    expect(gems_step("candidate", "ruby/setup-ruby@").fetch("with"))
      .to eq("ruby-version" => "3.4.12", "bundler" => "4.0.23", "bundler-cache" => false)
    expect(gems_step("candidate", "jdx/mise-action@")).to eq(gems_step("build", "jdx/mise-action@"))
    publish = YAML.load_file(File.join(root, ".github/workflows/release.yml")).dig("jobs", "publish", "steps")
    from_publish = lambda do |steps|
      steps.select do |step|
        step.fetch("uses", "").start_with?("actions/download-artifact@") || step["run"] == "mise run release:build"
      end
    end
    expect(from_publish.call(job.fetch("steps"))).to eq(from_publish.call(publish)).and have_attributes(size: 3)
    expect(job.fetch("steps").last.fetch("run")).to include("git status --porcelain")
  end

  it "reports verified only when every other job succeeded, with no other conditional or non-gating job" do
    verified = gems_jobs.fetch("verified")
    others = gems_jobs.keys - ["verified"]
    expect(verified.fetch("needs")).to match_array(others)
    expect(verified.fetch("if")).to eq("always()")
    expect(verified.dig("outputs", "verified")).to eq("${{ steps.check.outputs.verified }}")
    check = verified.fetch("steps").find { |step| step["id"] == "check" }
    expect(check.fetch("env")).to eq("NEEDS" => "${{ toJSON(needs) }}")
    expect(check.fetch("run")).to include("jq -e", 'all(.[]; .result == "success")', *others.map { "\"#{it}\"" })
    expect(check.fetch("run").lines.last.strip).to eq('echo "verified=true" >> "$GITHUB_OUTPUT"')
    expect(gems_workflow.dig("on", "workflow_call", "outputs", "verified", "value") ||
           gems_workflow.dig(true, "workflow_call", "outputs", "verified", "value"))
      .to eq("${{ jobs.verified.outputs.verified }}")
    gems_jobs.except("verified").each_value do |job|
      expect(job).not_to have_key("continue-on-error")
      expect(job).not_to have_key("if")
    end
    expect(gems_jobs.values.flat_map { |job| job.fetch("steps") }.filter_map { |step| step["if"] }.uniq)
      .to eq(["always()"])
  end

  it "keeps the Windows spec job non-gating and every other hyper CI job gating" do
    expect(jobs.fetch("hyper_windows").fetch("continue-on-error")).to be(true)
    %w[hyper hyper_msrv hyper_gems].each { |name| expect(jobs.fetch(name)).not_to have_key("continue-on-error") }
    windows = commands.fetch("hyper_windows").join("\n")
    expect(windows).to include("scripts/install_smoke.rb", "scripts/use_artifact.rb", "--tag \"~fork\"",
                               "--tag \"~dns_shim\"", "--tag interop")
  end

  it "keeps the hyper tool and peer contexts aligned between local and CI tasks" do
    local, ci = %w[hyper:tools hyper:peer].map do |name|
      [nil, "ci"].map do |mode|
        output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info", name, "--json")
        expect(status.success?).to be(true), error
        JSON.parse(output)
      end
    end.transpose
    local.zip(ci).each do |local_task, ci_task|
      %w[tools env dir depends depends_post].each { |key| expect(ci_task.fetch(key)).to eq(local_task.fetch(key)) }
    end
    expect(ci.last.fetch("run")).to eq(local.last.fetch("run"))
    expect(local.last.fetch("depends")).to eq(["hyper:toolchain:go"])
    # Locally the dependency installs Go; in CI it validates setup-go's Go in
    # the peer's own context.
    toolchain = [nil, "ci"].map do |mode|
      output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info", "hyper:toolchain:go",
                                             "--json")
      expect(status.success?).to be(true), error
      JSON.parse(output)
    end
    expect(toolchain.map { |task| task.fetch("run") })
      .to eq([["mise install --locked go@1.27.3"], ["ruby {{config_root}}/scripts/ci_runtime.rb hyper"]])
    %w[tools env dir].each { |key| expect(toolchain.last.fetch(key)).to eq(local.last.fetch(key)) }
  end
end

RSpec.describe "Package bundle isolation in CI" do
  it "keeps jobs with a package Gemfile from reaching root setup through task dependencies" do
    jobs = %w[ci.yml hyper-gems.yml].map do |name|
      YAML.load_file(File.expand_path("../.github/workflows/#{name}", __dir__)).fetch("jobs")
    end.reduce(:merge)
    output, error, status = Open3.capture3({ "MISE_ENV" => "ci" }, "mise", "tasks", "--json")
    expect(status.success?).to be(true), error
    depends = JSON.parse(output).to_h { |task| [task.fetch("name"), task.fetch("depends")] }
    closure = lambda do |name, seen = []|
      next seen if seen.include?(name)

      depends.fetch(name).reduce(seen + [name]) { |visited, dependency| closure.call(dependency.split.first, visited) }
    end
    commands = jobs.flat_map do |job_name, job|
      job.fetch("steps", []).flat_map do |step|
        gemfile = step.dig("env", "BUNDLE_GEMFILE") || job.dig("env", "BUNDLE_GEMFILE")
        next [] unless gemfile && step["run"]

        step["run"].scan(/mise run "?([^\s";&|]+)/).flatten.map { |task| "#{job_name}: #{task}" }
      end
    end
    expect(commands).to include("async_interop: async:interop:grpc", "falcon_interop: falcon:interop:grpc",
                                "build: hyper:cross", "smoke_source: hyper:smoke:source")
    offenders = commands.select { |command| closure.call(command.split.last).include?("setup") }
    expect(offenders).to be_empty
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

  it "uses independent bundles and separate backend commands in both Ruby lanes" do
    jobs = YAML.load_file(File.join(root, ".github/workflows/ci.yml")).fetch("jobs")
    ["#{backend_name}_specs", "#{backend_name}_interop"].each do |name|
      job = jobs.fetch(name)
      expect(job.fetch("env")).to include(
        "BUNDLE_GEMFILE" => "${{ github.workspace }}/gems/connectrpc-#{backend_name}/Gemfile",
        "BUNDLE_PATH" => "${{ github.workspace }}/vendor/#{backend_name}/bundle",
        "BUNDLE_USER_HOME" => "${{ github.workspace }}/vendor/#{backend_name}/bundler", "BUNDLE_COOLDOWN" => "3"
      )
    end
    commands = jobs.transform_values { |job| job.fetch("steps", []).filter_map { |step| step["run"] } }
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
      "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_DISABLE_SHARED_GEMS" => "true",
      "BUNDLE_VERSION" => "4.0.23", "BUNDLE_COOLDOWN" => "3",
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

  it "uses independent bundles and separate middleware commands in both Ruby lanes" do
    jobs = YAML.load_file(File.join(root, ".github/workflows/ci.yml")).fetch("jobs")
    %w[rack].each do |name|
      job = jobs.fetch(name)
      expect(job.fetch("env")).to include(
        "BUNDLE_GEMFILE" => "${{ github.workspace }}/gems/connectrpc-rack/Gemfile",
        "BUNDLE_PATH" => "${{ github.workspace }}/vendor/rack/bundle",
        "BUNDLE_USER_HOME" => "${{ github.workspace }}/vendor/rack/bundler", "BUNDLE_COOLDOWN" => "3"
      )
    end
    commands = jobs.transform_values { |job| job.fetch("steps", []).filter_map { |step| step["run"] } }
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
      "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_DISABLE_SHARED_GEMS" => "true",
      "BUNDLE_VERSION" => "4.0.23", "BUNDLE_COOLDOWN" => "3",
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

RSpec.describe "Optional OpenTelemetry, metrics, health and Sentry CI boundaries" do
  %w[otel prometheus yabeda health sentry].each do |name|
    it "keeps the independent #{name} context and CI override aligned" do
      root = File.expand_path("..", __dir__)
      local, ci = [nil, "ci"].map do |mode|
        output, error, status = Open3.capture3({ "MISE_ENV" => mode }, "mise", "tasks", "info", "#{name}:tools",
                                               "--json")
        expect(status.success?).to be(true), error
        JSON.parse(output)
      end
      %w[tools env dir depends depends_post].each { |key| expect(ci.fetch(key)).to eq(local.fetch(key)) }
      expect(ci.fetch("run")).to eq(["ruby scripts/ci_runtime.rb"])
      expect(local.fetch("run")).to eq(["mise exec -- ruby scripts/tools.rb"])
      guard = File.read(File.join(root, "gems/connectrpc-#{name}/scripts/ci_runtime.rb"))
      expect(guard).to include("scripts/ci_runtime.rb", "vendor/#{name}/bundle")
    end
  end
end
