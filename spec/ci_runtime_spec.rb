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

  it "rejects unknown guard modes" do
    _output, error, status = guard({}, "unknown")
    expect(status.success?).to be(false)
    expect(error).to include("Usage:")
  end
end

RSpec.describe "CI check mapping" do
  let(:workflow) { YAML.load_file(File.expand_path("../.github/workflows/ci.yml", __dir__)) }
  let(:jobs) { workflow.fetch("jobs") }

  it "runs nine core and three Rails checks with each Ruby syntax check separate from its specs" do
    expect(jobs.values.sum do |job|
      job.dig("strategy", "matrix", "ruby")&.size || job.dig("strategy", "matrix", "include")&.size || 1
    end).to eq(12)
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
    jobs.each_value do |job|
      steps = job.fetch("steps")
      ruby = steps.find { |step| step.fetch("uses", "").start_with?("ruby/setup-ruby@") }
      expect(ruby.fetch("with")).to include("bundler" => "4.0.23", "bundler-cache" => true,
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
    interop = jobs.fetch("interop")
    expect(interop.fetch("env")).to include("GOMODCACHE" => "${{ github.workspace }}/vendor/interop/mod",
                                            "GOCACHE" => "${{ github.workspace }}/vendor/interop/build")
    go = interop.fetch("steps").find { |step| step.fetch("uses", "").start_with?("actions/setup-go@") }
    expect(go.fetch("with")).to include("go-version" => "1.27.3", "check-latest" => false, "cache" => true,
                                        "cache-dependency-path" => "interop/go.sum")
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
