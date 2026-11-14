# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "tmpdir"
require "yaml"
require_relative "../scripts/release_train"
require_relative "../scripts/release_candidate"

RSpec.describe ReleaseTrain do
  def captured_train
    Dir.mktmpdir("connectrpc-release-spec-") do |temporary|
      root = File.expand_path("..", __dir__)
      owners = described_class::NAMES.map do |name|
        name == "connectrpc" ? "connectrpc.gemspec" : "gems/#{name}/#{name}.gemspec"
      end
      inputs = [*owners, *described_class::VERSION_FILES.values, *described_class::LOCKS, *described_class::GUIDES,
                "docs/development.md", ".github/release-please-config.json", ".github/.release-please-manifest.json"]
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
                                            defined?(::Falcon) || defined?(::OpenTelemetry)
      puts JSON.generate([ConnectRPC::VERSION, ConnectRPC::Rack::VERSION, ConnectRPC::Rails::VERSION,
                          ConnectRPC::Async::VERSION, ConnectRPC::Falcon::VERSION,
                          ConnectRPC::Testing::VERSION, ConnectRPC::OTel::VERSION])
    RUBY
    stdout, stderr, status = Open3.capture3(RbConfig.ruby, "-e", command, *files)
    expect(status.success?).to be(true), stderr
    expect(JSON.parse(stdout)).to eq(Array.new(7, train.version))
  end

  %i[patch minor].each do |bump|
    it "prepares every frozen local record for a #{bump} without changing registry resolution" do
      captured_train do |train|
        version = next_version(train, bump)
        originals = described_class::LOCKS.to_h { |file| [file, train.read(file)] }
        propose_version(train, version)
        expect { train.check! }.to raise_error(/Stale release preparation/)
        train.prepare!
        expect(train.specifications.map { |spec| spec.version.to_s }.uniq).to eq([version])
        floor = "~> #{version.split('.').first(2).join('.')}.0"
        train.specifications.each do |spec|
          spec.runtime_dependencies.select { |dep| described_class::NAMES.include?(dep.name) }.each do |dependency|
            expect(dependency.requirement.to_s).to eq(floor)
          end
        end
        originals.each do |file, original|
          # Independent assertions cover PATH specs, local CHECKSUMS, and the
          # unmodified registry records, not just a call to the preparer twice.
          names = described_class::LOCK_PACKAGES.fetch(file)
          sections = train.read(file).split(/(?=^[A-Z][A-Z ]*\n)/)
          paths = sections.select { |section| section.start_with?("PATH\n") }.join
          checksums = sections.find { |section| section.start_with?("CHECKSUMS\n") }
          names.each do |name|
            expect(paths.lines).to include("    #{name} (#{version})\n")
            expect(checksums.lines).to include("  #{name} (#{version})\n"),
                                       "#{file}: CHECKSUMS must contain #{name} (#{version})"
          end
          local_dependency = /^ {6}(connectrpc(?:-[a-z]+)?) \(/
          original_paths = original.split(/(?=^[A-Z][A-Z ]*\n)/).select { |section| section.start_with?("PATH\n") }.join
          dependencies = original_paths.scan(local_dependency).flatten
          expected_dependencies = dependencies.map { |name| "      #{name} (#{floor})\n" }
          expect(paths.lines.grep(local_dependency)).to eq(expected_dependencies),
                                                        "#{file}: PATH dependencies must use #{floor}"
          registry_sections = lambda { |text|
            text.split(/(?=^[A-Z][A-Z ]*\n)/).reject do |s|
              s.start_with?("PATH\n", "CHECKSUMS\n")
            end
          }
          expect(registry_sections.call(train.read(file))).to eq(registry_sections.call(original))
          external_checksums = ->(text) { text.lines.grep(/sha256=/) }
          expect(external_checksums.call(train.read(file))).to eq(external_checksums.call(original))
        end
        expect(train.read("README.md")).to include("gem \"connectrpc-rack\", \"#{floor}\"")
        snapshot = train.prepared_files
        train.prepare!
        expect(train.prepared_files).to eq(snapshot)
      end
    end
  end

  it "rejects the partial root-lock update made by the upstream Ruby updater" do
    captured_train do |train|
      previous = train.version
      version = next_version(train)
      propose_version(train, version)
      edit(train, "Gemfile.lock") { |text| text.sub("connectrpc (#{previous})", "connectrpc (#{version})") }
      expect { train.check! }.to raise_error(/Stale release preparation: Gemfile.lock/)
      train.prepare!
      expect(train.check!).to equal(train)
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
    "gems/connectrpc-otel/lib/connectrpc/otel/version.rb" => /Version mismatch: connectrpc-otel/
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

  it "rejects a missing local checksum and a changed publication guard" do
    captured_train do |train|
      edit(train, "Gemfile.lock") { |text| text.sub("  connectrpc (#{train.version})\n", "") }
      expect { train.check! }.to raise_error(%r{Local path/checksum inventory})
    end
    captured_train do |train|
      edit(train, "connectrpc.gemspec") { |text| text.sub("https://gems.invalid", "https://rubygems.org") }
      expect { train.check! }.to raise_error(/Publication guard lost/)
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
      expect { train.prepare! }.to raise_error(/Local dependency policy changed/)
    end
  end
end

RSpec.describe "Release preparation workflow boundary" do
  it "prepares PRs from main pushes with no GitHub token permissions, release creation or publishing job" do
    workflow = YAML.load_file(File.expand_path("../.github/workflows/release-please.yml", __dir__))
    expect(workflow.fetch("on", workflow[true])).to eq("push" => { "branches" => ["main"] }, "workflow_dispatch" => nil)
    expect(workflow.fetch("permissions")).to eq({})
    job = workflow.fetch("jobs").fetch("release-please")
    expect(job).not_to have_key("permissions")
    expect(job.fetch("environment")).to eq("release-please")
    expect(workflow.fetch("jobs").keys).to eq(["release-please"])
    expect(job.fetch("if")).to eq("github.ref == 'refs/heads/main'")
    release = job.fetch("steps").find { |step| step.fetch("uses", "").start_with?("googleapis/release-please-action@") }
    expect(release.fetch("with")).to include("skip-github-release" => true)
    expect(release.fetch("with")).not_to have_key("release-type")
    token = job.fetch("steps").find { |step| step.fetch("uses", "").start_with?("actions/create-github-app-token@") }
    expect(token.fetch("with")).to include("client-id" => "${{ vars.RELEASE_BOT_CLIENT_ID }}",
                                           "private-key" => "${{ secrets.RELEASE_BOT_PRIVATE_KEY }}")
    expect(token.fetch("with").keys.grep(/^permission-/)).to match_array(%w[permission-contents permission-issues
                                                                            permission-pull-requests])
  end
end
