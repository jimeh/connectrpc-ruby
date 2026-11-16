# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "tmpdir"
require "yaml"
require_relative "../scripts/release_train"
require_relative "../scripts/release_candidate"
require_relative "../scripts/release_preparation"

RSpec.describe ReleaseTrain do
  def captured_train
    Dir.mktmpdir("connectrpc-release-spec-") do |temporary|
      root = File.expand_path("..", __dir__)
      owners = described_class::NAMES.map do |name|
        name == "connectrpc" ? "connectrpc.gemspec" : "gems/#{name}/#{name}.gemspec"
      end
      inputs = [*owners, *described_class::VERSION_FILES.values, *described_class::GUIDES,
                ".github/release-please-config.json", ".github/.release-please-manifest.json"]
      inputs.each do |file|
        destination = File.join(temporary, file)
        FileUtils.mkdir_p(File.dirname(destination))
        FileUtils.cp(File.join(root, file), destination)
      end
      yield described_class.new(temporary)
    end
  end

  def edit(train, file)
    File.write(File.join(train.root, file), yield(train.read(file)))
  end

  # Input to our preparation is a Release Please version edit, not a replacement
  # implementation of its updater. The real pinned updater is probed separately.
  def propose_version(train, version)
    config = JSON.parse(train.read(".github/release-please-config.json")).fetch("packages").fetch(".")
    [config.fetch("version-file"), *config.fetch("extra-files")].each do |file|
      edit(train, file) { |content| content.sub(/((?:VERSION|spec.version) = ")[^"]+/, "\\1#{version}") }
    end
    File.write(File.join(train.root, ".github/.release-please-manifest.json"), JSON.generate({ "." => version }))
  end

  def next_version(train, bump = :patch)
    major, minor, patch = train.version.split(".").map(&:to_i)
    bump == :patch ? "#{major}.#{minor}.#{patch + 1}" : "#{major}.#{minor + 1}.0"
  end

  it "accepts the synchronized train and only allows an empty bootstrap at 0.1.0" do
    expect(described_class.new.check!).to be_a(described_class)
    captured_train do |train|
      manifest = JSON.parse(train.read(".github/.release-please-manifest.json"))
      expect(train.version).to eq("0.1.0") if manifest.empty?
      expect(train.check!).to equal(train)
    end
  end

  it "loads each static version file without loading optional runtime dependencies" do
    train = described_class.new
    files = described_class::VERSION_FILES.values.map { |file| File.join(train.root, file) }
    command = <<~RUBY
      require "json"
      ARGV.each { |file| require file }
      abort "Loaded optional dependency" if defined?(::Rack) || defined?(::Rails) || defined?(::Async) ||
                                            defined?(::Falcon) || defined?(::OpenTelemetry) ||
                                            defined?(::Prometheus) || defined?(::Yabeda) ||
                                            defined?(::Google::Protobuf) || defined?(::Sentry)
      puts JSON.generate([ConnectRPC::VERSION, ConnectRPC::Rack::VERSION, ConnectRPC::OnRails::VERSION,
                          ConnectRPC::Async::VERSION, ConnectRPC::Falcon::VERSION,
                          ConnectRPC::Testing::VERSION, ConnectRPC::OTel::VERSION,
                          ConnectRPC::Prometheus::VERSION, ConnectRPC::Yabeda::VERSION,
                          ConnectRPC::Health::VERSION, ConnectRPC::Sentry::VERSION])
    RUBY
    stdout, stderr, status = Open3.capture3(RbConfig.ruby, "-e", command, *files)
    expect(status.success?).to be(true), stderr
    expect(JSON.parse(stdout)).to eq(Array.new(11, train.version))
  end

  def internal_requirements(train)
    train.specifications.flat_map(&:runtime_dependencies).select { |dep| described_class::NAMES.include?(dep.name) }
         .map { |dep| dep.requirement.to_s }.uniq
  end

  it "accepts a patch release that changes only version files and the manifest" do
    captured_train do |train|
      version = next_version(train, :patch)
      guides = described_class::GUIDES.to_h { |file| [file, train.read(file)] }
      propose_version(train, version)
      expect(train.check!).to equal(train)
      expect(train.specifications.map { |spec| spec.version.to_s }.uniq).to eq([version])
      expect(internal_requirements(train)).to eq(["~> #{version.split('.').first(2).join('.')}.0"])
      expect(described_class::GUIDES.to_h { |file| [file, train.read(file)] }).to eq(guides)
    end
  end

  it "requires guide preparation for a minor release, then accepts it" do
    captured_train do |train|
      version = next_version(train, :minor)
      floor = "~> #{version.split('.').first(2).join('.')}.0"
      propose_version(train, version)
      expect { train.check! }.to raise_error(/Stale release preparation: .*run mise run release:prepare/)
      train.prepare!
      expect(train.check!).to equal(train)
      expect(internal_requirements(train)).to eq([floor])
      expect(train.read("README.md")).to include("gem \"connectrpc-rack\", \"#{floor}\"")
      snapshot = train.prepared_files
      train.prepare!
      expect(train.prepared_files).to eq(snapshot)
    end
  end

  it "refuses a non-empty candidate directory without replacing existing artifacts" do
    captured_train do |train|
      output = File.join(train.root, "candidate")
      FileUtils.mkdir_p(output)
      artifact = File.join(output, "old.gem")
      File.binwrite(artifact, "retained artifact")
      expect do
        ReleaseCandidate.new(train).build!(output: output, revision: "fixture", epoch: 1_800_000_000)
      end.to raise_error(/Candidate output must be missing or empty/)
      expect(Dir.children(output)).to eq(["old.gem"])
      expect(File.binread(artifact)).to eq("retained artifact")
    end
  end

  {
    "gems/connectrpc-falcon/lib/connectrpc/falcon/version.rb" => /Version mismatch: connectrpc-falcon/,
    "gems/connectrpc-otel/lib/connectrpc/otel/version.rb" => /Version mismatch: connectrpc-otel/,
    "gems/connectrpc-yabeda/lib/connectrpc/yabeda/version.rb" => /Version mismatch: connectrpc-yabeda/,
    "gems/connectrpc-sentry/lib/connectrpc/sentry/version.rb" => /Version mismatch: connectrpc-sentry/
  }.each do |file, failure|
    it "rejects a mismatched #{file} before building artifacts" do
      captured_train do |train|
        edit(train, file) { |text| text.sub("\"#{train.version}\"", "\"#{next_version(train)}\"") }
        output = File.join(train.root, "output")
        expect do
          ReleaseCandidate.new(train).build!(output: output, revision: "fixture", epoch: 1_800_000_000)
        end.to raise_error(failure)
        expect(File.exist?(output)).to be(false)
      end
    end
  end

  it "rejects a changed push host and a dropped MFA requirement" do
    captured_train do |train|
      edit(train, "connectrpc.gemspec") { |text| text.sub("https://rubygems.org", "https://gems.example") }
      expect { train.check! }.to raise_error(/Push host changed/)
    end
    captured_train do |train|
      edit(train, "gems/connectrpc-otel/connectrpc-otel.gemspec") do |text|
        text.sub(/^.*rubygems_mfa_required.*\n/, "")
      end
      expect { train.check! }.to raise_error(/MFA requirement lost: connectrpc-otel/)
    end
  end

  it "rejects narrowing patch dependency ranges" do
    captured_train do |train|
      version = next_version(train)
      propose_version(train, version)
      edit(train, "gems/connectrpc-rack/connectrpc-rack.gemspec") do |text|
        text.sub('spec.add_dependency "connectrpc", local_requirement',
                 "spec.add_dependency \"connectrpc\", \"~> #{version}\"")
      end
      expect { train.check! }.to raise_error(/Local dependency policy changed/)
      expect { train.prepare! }.to raise_error(/Local dependency policy changed/)
    end
  end
end

RSpec.describe "Release workflow boundaries" do
  def load_workflow(name)
    workflow = YAML.load_file(File.expand_path("../.github/workflows/#{name}", __dir__))
    [workflow, workflow.fetch("on", workflow[true])]
  end

  it "keeps Release Please on main with App credentials and no publishing permissions" do
    workflow, triggers = load_workflow("release-please.yml")
    expect(triggers).to eq("push" => { "branches" => ["main"] }, "workflow_dispatch" => nil)
    expect(workflow.fetch("permissions")).to eq({})
    expect(workflow.fetch("jobs").keys).to eq(["release-please"])
    job = workflow.fetch("jobs").fetch("release-please")
    expect(job).not_to have_key("permissions")
    expect(job.fetch("environment")).to eq("release-please")
    expect(job.fetch("if")).to eq("github.ref == 'refs/heads/main'")
    release = job.fetch("steps").find { |step| step.fetch("uses", "").start_with?("googleapis/release-please-action@") }
    expect(release.fetch("with").keys).to match_array(%w[token config-file manifest-file])
    token = job.fetch("steps").find { |step| step.fetch("uses", "").start_with?("actions/create-github-app-token@") }
    expect(token.fetch("with")).to include("client-id" => "${{ vars.RELEASE_BOT_CLIENT_ID }}",
                                           "private-key" => "${{ secrets.RELEASE_BOT_PRIVATE_KEY }}")
    expect(token.fetch("with").keys.grep(/^permission-/)).to match_array(%w[permission-contents permission-issues
                                                                            permission-pull-requests])
  end

  # The App token and the release PR head meet only as data: tooling comes from
  # main and nothing runs from the release-pr checkout.
  it "prepares the open release PR from main tooling after Release Please" do
    workflow, = load_workflow("release-please.yml")
    expect(workflow.fetch("env")).to eq("MISE_ENV" => "ci", "MISE_LOCKED" => "1")
    steps = workflow.fetch("jobs").fetch("release-please").fetch("steps")
    release = steps.index { |step| step.fetch("uses", "").start_with?("googleapis/release-please-action@") }
    find, main, data, ruby, mise, prepare = steps.drop(release + 1)
    expect(steps.size).to eq(release + 7)

    token = "${{ steps.app-token.outputs.token }}"
    head = "${{ steps.release-pr.outputs.sha }}"
    expect(find).to include("id" => "release-pr")
    expect(find).not_to have_key("if")
    expect(find.fetch("env")).to eq("GH_TOKEN" => token, "RELEASE_BRANCH" => ReleasePreparation::BRANCH)
    expect(find.fetch("run")).to include("pulls?state=open&base=main&head=$GITHUB_REPOSITORY_OWNER:$RELEASE_BRANCH")
      .and include("git/ref/heads/$RELEASE_BRANCH\" --jq '.object.sha'")
      .and include('"sha=$sha" >> "$GITHUB_OUTPUT"')
    expect([main, data, ruby, mise, prepare].map { |step| step["if"] }).to all(eq("steps.release-pr.outputs.sha != ''"))

    checkout = steps.find { |step| step.fetch("uses", "").start_with?("actions/checkout@") }.fetch("uses")
    expect([main, data].map { |step| step.fetch("uses") }).to eq([checkout, checkout])
    expect(main.fetch("with")).to eq("token" => token, "persist-credentials" => false)
    expect(data.fetch("with")).to eq("ref" => head, "path" => "release-pr", "token" => token,
                                     "persist-credentials" => false)

    # Runtime setup matches the release workflow exactly, with no caches.
    published = load_workflow("release.yml").first.fetch("jobs").fetch("publish").fetch("steps")
    [ruby, mise].each do |step|
      expect(published).to include(step.except("if"))
    end
    expect(prepare.fetch("run")).to eq("mise run release:prepare:pr")
    expect(prepare.fetch("env")).to eq("GH_TOKEN" => token, "RELEASE_HEAD" => head, "RELEASE_ROOT" => "release-pr")

    expect(steps.filter_map { |step| step["run"] && step.fetch("name") })
      .to eq(["Check App references", find.fetch("name"), prepare.fetch("name")])
    expect(steps.select { |step| step.key?("working-directory") }).to be_empty
    expect(steps.select { |step| step.fetch("env", {}).value?(token) }).to eq([find, prepare])
  end

  # RubyGems.org trusted publishers match this exact filename and environment.
  it "publishes only from v* tags in the release environment with job-scoped OIDC" do
    workflow, triggers = load_workflow("release.yml")
    expect(triggers).to eq("push" => { "tags" => ["v*"] }, "workflow_dispatch" => nil)
    expect(workflow.fetch("permissions")).to eq({})
    expect(workflow.fetch("jobs").keys).to eq(["publish"])
    job = workflow.fetch("jobs").fetch("publish")
    expect(job.fetch("if")).to eq("startsWith(github.ref, 'refs/tags/v')")
    expect(job.fetch("environment")).to eq("release")
    expect(job.fetch("permissions")).to eq("contents" => "write", "id-token" => "write")
    expect(job.fetch("steps").filter_map { |step| step["run"] })
      .to eq(["mise run release:tools", "mise run release:build", "mise run release:publish"])
    expect(job.fetch("steps").last.fetch("env")).to include("RUBYGEMS_OWNER" => "jimeh")
    caches = job.fetch("steps").filter_map { |step| step["with"]&.slice("bundler-cache", "cache") }.reject(&:empty?)
    expect(caches).to eq([{ "cache" => false }])
  end
end
