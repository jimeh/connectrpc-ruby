# frozen_string_literal: true

require "fileutils"
require "json"
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
      inputs = [*owners, *described_class::LOCKS, *described_class::GUIDES,
                "docs/development.md", "lib/connectrpc/version.rb",
                ".github/release-please-config.json", ".github/.release-please-manifest.json",
                "gems/connectrpc-otel/lib/connectrpc/otel/subscriber.rb"]
      inputs.each do |file|
        destination = File.join(temporary, file)
        FileUtils.mkdir_p(File.dirname(destination))
        FileUtils.cp(File.join(root, file), destination)
      end
      train = described_class.new(temporary)
      propose_version(train, "0.1.0")
      train.prepare!
      File.write(File.join(temporary, ".github/.release-please-manifest.json"), "{}\n")
      yield train
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

  it "accepts initial 0.1.0 without inventing an already-released manifest entry" do
    expect(described_class.new.check!).to be(true)
    captured_train do |train|
      expect(train.version).to eq("0.1.0")
      expect(JSON.parse(train.read(".github/.release-please-manifest.json"))).to eq({})
      expect(train.check!).to be(true)
    end
  end

  %w[0.1.1 0.2.0].each do |version|
    it "prepares every frozen local record for #{version} without changing registry resolution" do
      captured_train do |train|
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
          names.each do |name|
            expect(train.read(file)).to include("    #{name} (#{version})\n", "  #{name} (#{version})\n")
          end
          registry_sections = ->(text) { text.split(/(?=^[A-Z][A-Z ]*\n)/).reject { |s| s.start_with?("PATH\n", "CHECKSUMS\n") } }
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
      propose_version(train, "0.1.1")
      edit(train, "Gemfile.lock") { |text| text.sub("connectrpc (0.1.0)", "connectrpc (0.1.1)") }
      expect { train.check! }.to raise_error(/Stale release preparation: Gemfile.lock/)
      train.prepare!
      expect(train.check!).to be(true)
    end
  end

  {
    "gems/connectrpc-falcon/connectrpc-falcon.gemspec" => /Version mismatch: connectrpc-falcon/,
    "gems/connectrpc-otel/lib/connectrpc/otel/subscriber.rb" => /Instrumentation version mismatch/
  }.each do |file, failure|
    it "rejects a mismatched #{file} before building artifacts" do
      captured_train do |train|
        edit(train, file) { |text| text.sub('"0.1.0"', '"0.1.1"') }
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
      edit(train, "Gemfile.lock") { |text| text.sub("  connectrpc (0.1.0)\n", "") }
      expect { train.check! }.to raise_error(/Local path\/checksum inventory/)
    end
    captured_train do |train|
      edit(train, "connectrpc.gemspec") { |text| text.sub("https://gems.invalid", "https://rubygems.org") }
      expect { train.check! }.to raise_error(/Publication guard lost/)
    end
  end

  it "rejects narrowing patch dependency ranges" do
    captured_train do |train|
      propose_version(train, "0.1.1")
      edit(train, "gems/connectrpc-rack/connectrpc-rack.gemspec") do |text|
        text.sub('spec.add_dependency "connectrpc", local_requirement', 'spec.add_dependency "connectrpc", "~> 0.1.1"')
      end
      expect { train.prepare! }.to raise_error(/Local dependency policy changed/)
    end
  end
end

RSpec.describe "Release preparation workflow boundary" do
  it "only dispatches PR preparation on main with no GitHub token permissions or publishing job" do
    workflow = YAML.load_file(File.expand_path("../.github/workflows/release-please.yml", __dir__))
    expect(workflow.fetch("on", workflow[true]).keys).to eq(["workflow_dispatch"])
    expect(workflow.fetch("permissions")).to eq({})
    job = workflow.fetch("jobs").fetch("release-please")
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
