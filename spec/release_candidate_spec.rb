# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "rubygems/package"
require "tmpdir"
require_relative "../scripts/release_candidate"

# Platform archives come from rb-sys-dock in CI. These synthetic archives are
# what RbSys::ExtensionTask produces from the real source gemspec: the Ruby
# files without the crate sources, one fake extension per Ruby minor, no
# extension build and no rb_sys dependency.
module ReleaseCandidateFixtures
  def self.source(train)
    train.specifications.find { |spec| spec.name == "connectrpc-hyper" }
  end

  def self.platform_gem(train, directory, platform, binary_minors: HyperPlatforms::RUBY_MINORS) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    source = source(train)
    spec = source.dup
    spec.platform = Gem::Platform.new(platform)
    spec.extensions = []
    spec.dependencies.reject! { |dependency| dependency.name == "rb_sys" }
    spec.required_ruby_version = [">= 3.4", "< 4.1.dev"]
    spec.required_rubygems_version = ">= 3.3.22" if platform.end_with?("-linux-musl")
    extension = platform.include?("darwin") ? "bundle" : "so"
    binaries = binary_minors.map { |minor| "lib/connectrpc/hyper/#{minor}/connectrpc_hyper.#{extension}" }
    spec.files = source.files.grep_v(ReleaseCandidate::CRATE_SOURCES) + binaries
    contents = {}
    yield spec, contents if block_given?
    archive = File.join(directory, spec.file_name)
    Dir.mktmpdir("connectrpc-platform-gem-") do |staging|
      spec.files.each do |file|
        destination = File.join(staging, file)
        FileUtils.mkdir_p(File.dirname(destination))
        binary = file.match?(/connectrpc_hyper\.(?:so|bundle)\z/)
        bytes = contents.fetch(file) do
          next "fake #{platform} extension for #{file}" if binary

          File.binread(File.join(train.directory(source.name), file))
        end
        File.binwrite(destination, bytes)
        File.chmod(binary ? 0o755 : 0o644, destination)
      end
      Gem::DefaultUserInteraction.use_ui(Gem::SilentUI.new) do
        Dir.chdir(staging) { Gem::Package.build(spec, true, false, archive) }
      end
    end
    archive
  end

  def self.evidence(directory, archive, minor, overrides = {})
    spec = Gem::Package.new(archive).spec
    ruby = { "3.4" => "3.4.12", "4.0" => "4.0.9" }.fetch(minor)
    record = { "file" => File.basename(archive), "sha256" => Digest::SHA256.file(archive).hexdigest,
               "platform" => spec.platform.to_s, "ruby" => ruby, "ruby_platform" => "test" }.merge(overrides)
    path = File.join(directory, "#{spec.platform}-#{minor}.json")
    File.write(path, JSON.generate(record.compact))
    path
  end
end

RSpec.describe ReleaseCandidate do
  let(:train) { ReleaseTrain.new }
  let(:version) { train.version }
  let(:candidate) { described_class.new(train) }
  let(:epoch) { 1_800_000_000 }

  around do |example|
    Dir.mktmpdir("connectrpc-candidate-spec-") do |temporary|
      @temporary = temporary
      example.run
    end
  end

  # Gem::Specification.load caches each gemspec with the date set while
  # building under this file's SOURCE_DATE_EPOCH; later specs must load fresh.
  after { Gem::Specification.send(:clear_load_cache) }

  def directory(name)
    File.join(@temporary, name).tap { |path| FileUtils.mkdir_p(path) }
  end

  describe ".collect!" do
    def write(path, bytes = File.basename(path))
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, bytes)
    end

    it "flattens explicit `gh run download` directories into the release:build inputs" do
      archives = directory("downloaded/archives")
      evidence = directory("downloaded/evidence")
      write(File.join(archives, "connectrpc-hyper-x86_64-linux", "connectrpc-hyper-#{version}-x86_64-linux.gem"))
      write(File.join(archives, "connectrpc-hyper-ruby", "connectrpc-hyper-#{version}.gem"))
      write(File.join(evidence, "hyper-smoke-x86_64-linux-3.4", "x86_64-linux-3.4.json"))
      root = directory("root")
      write(File.join(root, "vendor/hyper/release-evidence/stale.json"))

      collected = described_class.collect!(root, [archives, evidence])

      expect(collected.map(&:last)).to eq([2, 1])
      expect(Dir.children(File.join(root, "vendor/hyper/release-artifacts")).sort)
        .to eq(["connectrpc-hyper-#{version}-x86_64-linux.gem", "connectrpc-hyper-#{version}.gem"])
      expect(Dir.children(File.join(root, "vendor/hyper/release-evidence"))).to eq(["x86_64-linux-3.4.json"])
    end

    it "rejects inputs that share a name, or one path, before replacing anything" do
      archives = directory("archives")
      evidence = directory("evidence")
      write(File.join(evidence, "first", "x86_64-linux-3.4.json"))
      write(File.join(evidence, "second", "x86_64-linux-3.4.json"))
      root = directory("root")
      write(File.join(root, "vendor/hyper/release-artifacts/kept.gem"))

      expect { described_class.collect!(root, [archives, evidence]) }
        .to raise_error(/Several inputs share a name: x86_64-linux-3\.4\.json/)
      expect { described_class.collect!(root, [archives]) }.to raise_error(ArgumentError)
      expect(Dir.children(File.join(root, "vendor/hyper/release-artifacts"))).to eq(["kept.gem"])
    end

    it "rejects a missing source, or one that overlaps an output, before replacing anything" do
      root = directory("root")
      artifacts = File.join(root, "vendor/hyper/release-artifacts")
      archive = File.join(artifacts, "connectrpc-hyper-#{version}.gem")
      write(archive)
      evidence = directory("evidence")

      expect { described_class.collect!(root, [File.join(root, "missing"), evidence]) }
        .to raise_error(ArgumentError, /source is missing/)
      [artifacts, File.join(root, "vendor/hyper"), File.join(artifacts, "nested")].each do |source|
        FileUtils.mkdir_p(source)
        expect { described_class.collect!(root, [source, evidence]) }
          .to raise_error(ArgumentError, /overlaps the collection output/)
      end
      expect(File.exist?(archive)).to be(true)
    end

    it "keeps an input that a symlink resolves into the output it replaces" do
      root = directory("root")
      archive = File.join(root, "vendor/hyper/release-artifacts/connectrpc-hyper-#{version}.gem")
      write(archive, "tested bytes")
      archives = directory("linked")
      File.symlink(archive, File.join(archives, File.basename(archive)))

      described_class.collect!(root, [archives, directory("evidence")])

      expect(File.symlink?(archive)).to be(false)
      expect(File.read(archive)).to eq("tested bytes")
      expect(File.stat(File.dirname(archive)).mode & 0o7777).to eq(0o777 & ~File.umask)
    end

    it "sets an old output aside under a fresh name, never over an existing file" do
      root = directory("root")
      artifacts = File.join(root, "vendor/hyper/release-artifacts")
      write(File.join(artifacts, "old.gem"), "old")
      stale = "#{artifacts}.collect-old-taken"
      write(stale, "someone's file")
      allow(SecureRandom).to receive(:hex).and_return("taken", "fresh")
      archives = directory("archives")
      write(File.join(archives, "connectrpc-hyper-#{version}.gem"), "new")

      described_class.collect!(root, [archives, directory("evidence")])

      expect(File.read(stale)).to eq("someone's file")
      expect(Dir.children(artifacts)).to eq(["connectrpc-hyper-#{version}.gem"])
      expect(Dir.children(File.join(root, "vendor/hyper")).sort)
        .to eq(%w[release-artifacts release-artifacts.collect-old-taken release-evidence])
    end

    it "restores both old outputs, and so a linked input, when a later replacement fails" do
      root = directory("root")
      artifacts = File.join(root, "vendor/hyper/release-artifacts")
      evidence_output = File.join(root, "vendor/hyper/release-evidence")
      only_copy = File.join(artifacts, "x86_64-linux-3.4.json")
      write(only_copy, "only copy")
      write(File.join(evidence_output, "old.json"), "old evidence")
      archives = directory("archives")
      write(File.join(archives, "connectrpc-hyper-#{version}.gem"), "new archive")
      evidence = directory("linked-evidence")
      File.symlink(only_copy, File.join(evidence, File.basename(only_copy)))
      allow(FileUtils).to receive(:cp).and_call_original
      allow(FileUtils).to receive(:cp).with(anything, evidence_output).and_raise(Errno::ENOSPC)

      expect { described_class.collect!(root, [archives, evidence]) }.to raise_error(Errno::ENOSPC)

      expect(Dir.children(artifacts)).to eq(["x86_64-linux-3.4.json"])
      expect(File.read(File.join(evidence, File.basename(only_copy)))).to eq("only copy")
      expect(File.read(File.join(evidence_output, "old.json"))).to eq("old evidence")
      expect(Dir.children(File.join(root, "vendor/hyper")).sort).to eq(%w[release-artifacts release-evidence])
    end
  end

  describe "#check_platform_archive!" do
    def check(archive, platform)
      candidate.check_platform_archive!(archive, platform, ReleaseCandidateFixtures.source(train))
    end

    it "accepts glibc, musl, darwin and mingw archives built from the source files" do
      %w[x86_64-linux aarch64-linux-musl arm64-darwin aarch64-mingw-ucrt].each do |platform|
        archive = ReleaseCandidateFixtures.platform_gem(train, directory(platform), platform)
        expect { check(archive, platform) }.not_to raise_error
      end
    end

    {
      "another platform" => [->(_spec, _contents) {}, "aarch64-linux", /is x86_64-linux, expected aarch64-linux/],
      "an extension build" => [->(spec, _contents) { spec.extensions = ["ext/connectrpc_hyper/extconf.rb"] },
                               "x86_64-linux", /declares an extension build/],
      "the rb_sys dependency" => [->(spec, _contents) { spec.add_dependency "rb_sys", ">= 0.9.130", "< 1" },
                                  "x86_64-linux", /dependencies differ/],
      "a wider Ruby requirement" => [->(spec, _contents) { spec.required_ruby_version = ">= 3.4" },
                                     "x86_64-linux", /Ruby requirement differs/],
      "the musl RubyGems floor on glibc" => [->(spec, _contents) { spec.required_rubygems_version = ">= 3.3.22" },
                                             "x86_64-linux", /RubyGems requirement differs/],
      "changed metadata" => [->(spec, _contents) { spec.metadata = spec.metadata.merge("homepage_uri" => "x") },
                             "x86_64-linux", /metadata differs/],
      "a crate source member" => [->(spec, _contents) { spec.files += ["Cargo.lock"] },
                                  "x86_64-linux", /members differ/],
      "changed Ruby source bytes" => [->(_spec, contents) { contents["lib/connectrpc/hyper/tls.rb"] = "# changed\n" },
                                      "x86_64-linux", %r{bytes/modes differ}],
      "an empty extension" => [->(_spec, contents) { contents["lib/connectrpc/hyper/4.0/connectrpc_hyper.so"] = "" },
                               "x86_64-linux", /non-empty 0755 file/]
    }.each do |label, (change, platform, failure)|
      it "rejects an archive with #{label}" do
        archive = ReleaseCandidateFixtures.platform_gem(train, directory("bad"), "x86_64-linux", &change)
        expect { check(archive, platform) }.to raise_error(failure)
      end
    end

    it "rejects an archive missing a Ruby minor's extension" do
      archive = ReleaseCandidateFixtures.platform_gem(train, directory("bad"), "x86_64-linux-musl",
                                                      binary_minors: ["3.4"])
      expect { check(archive, "x86_64-linux-musl") }.to raise_error(/members differ/)
    end
  end

  describe "#check_evidence!" do
    let(:archive) { ReleaseCandidateFixtures.platform_gem(train, directory("archives"), "x86_64-linux") }
    let(:records) do
      [{ "file" => File.basename(archive), "platform" => "x86_64-linux",
         "sha256" => Digest::SHA256.file(archive).hexdigest }]
    end
    let(:evidence) { directory("evidence") }

    def record(minor, overrides = {}) = ReleaseCandidateFixtures.evidence(evidence, archive, minor, overrides)

    it "accepts one matching record per archive and Ruby minor" do
      %w[3.4 4.0].each { |minor| record(minor) }
      expect { candidate.check_evidence!(evidence, records) }.not_to raise_error
    end

    {
      "a missing Ruby minor" => [-> { record("3.4") }, /Missing smoke evidence: .*x86_64-linux.gem on Ruby 4.0/],
      "different bytes" => [-> { record("3.4") && record("4.0", "sha256" => "0" * 64) }, /does not match/],
      "another platform" => [-> { record("3.4") && record("4.0", "platform" => "aarch64-linux") }, /does not match/],
      "an unknown archive" => [-> { record("3.4") && record("4.0", "file" => "other.gem") }, /unknown archive/],
      "an unexpected Ruby" => [-> { record("3.4") && record("4.0", "ruby" => "3.3.9") }, /unexpected Ruby 3.3.9/],
      "a duplicate minor" => [-> { record("3.4") && record("4.0", "ruby" => "3.4.7") }, /Duplicate smoke evidence/],
      "a missing field" => [-> { record("3.4") && record("4.0", "ruby_platform" => nil) }, /Malformed smoke evidence/],
      "an extra file" => [lambda {
        record("3.4") && record("4.0") && File.write(File.join(evidence, "notes.txt"), "x")
      }, /Unexpected smoke evidence entry: notes.txt/]
    }.each do |label, (setup, failure)|
      it "rejects #{label}" do
        instance_exec(&setup)
        expect { candidate.check_evidence!(evidence, records) }.to raise_error(failure)
      end
    end
  end

  describe "#build!" do
    let(:archives) { directory("archives") }
    let(:evidence) { directory("evidence") }
    let(:output) { File.join(@temporary, "release", version, "abc123") }

    before do
      previous = ENV.fetch("SOURCE_DATE_EPOCH", nil)
      ENV["SOURCE_DATE_EPOCH"] = epoch.to_s
      source = silently { PackageBuild.build(directory: train.directory("connectrpc-hyper"), output: archives) }
      ENV["SOURCE_DATE_EPOCH"] = previous
      platform_archives = HyperPlatforms::NATIVE.map do |platform|
        ReleaseCandidateFixtures.platform_gem(train, archives, platform)
      end
      [source, *platform_archives].product(HyperPlatforms::RUBY_MINORS) do |archive, minor|
        ReleaseCandidateFixtures.evidence(evidence, archive, minor)
      end
    end

    def silently(&) = Gem::DefaultUserInteraction.use_ui(Gem::SilentUI.new, &)

    def build = silently { build_quietly }

    def build_quietly
      candidate.build!(output: output, revision: "abc123", epoch: epoch, platform_archives: archives,
                       evidence: evidence)
    end

    def leftovers = Dir.children(File.dirname(output))

    it "records all twenty archives once and keeps attempt-specific evidence out of candidate.json" do
      expect(build).to eq(output)
      files = train.archives.map(&:file)
      expect(Dir.children(output)).to match_array([*files, "candidate.json"])
      record = JSON.parse(File.read(File.join(output, "candidate.json")))
      artifacts = record.fetch("artifacts")
      expect(artifacts.map { |artifact| artifact.fetch("file") }).to match_array(files)
      expect(artifacts.to_h { |artifact| [artifact.fetch("file"), artifact.fetch("platform")] })
        .to eq(train.archives.to_h { |archive| [archive.file, archive.platform] })
      artifacts.each do |artifact|
        expect(artifact.fetch("sha256")).to eq(Digest::SHA256.file(File.join(output, artifact.fetch("file"))).hexdigest)
      end
      expect(record.to_s).not_to include("ruby_platform")
      expect(leftovers).to eq(["abc123"])
    end

    it "rejects a tested source gem that differs from the canonical build without writing output" do
      previous = ENV.fetch("SOURCE_DATE_EPOCH", nil)
      ENV["SOURCE_DATE_EPOCH"] = (epoch + 1).to_s
      source = silently { PackageBuild.build(directory: train.directory("connectrpc-hyper"), output: archives) }
      ENV["SOURCE_DATE_EPOCH"] = previous
      Dir.glob(File.join(evidence, "ruby-*.json")).each { |path| File.delete(path) }
      ReleaseCandidateFixtures.evidence(evidence, source, "3.4")
      ReleaseCandidateFixtures.evidence(evidence, source, "4.0")
      expect { build }.to raise_error(/Tested source gem differs from the canonical build/)
      expect(File.exist?(output)).to be(false)
      expect(leftovers).to be_empty
    end

    it "rejects missing smoke evidence without writing output" do
      File.delete(File.join(evidence, "aarch64-mingw-ucrt-4.0.json"))
      expect { build }.to raise_error(/Missing smoke evidence: connectrpc-hyper-.*-aarch64-mingw-ucrt.gem on Ruby 4.0/)
      expect(File.exist?(output)).to be(false)
      expect(leftovers).to be_empty
    end

    {
      "a missing platform archive" => [->(dir) { File.delete(Dir.glob(File.join(dir, "*-x86_64-darwin.gem")).first) },
                                       /Missing tested archives .*x86_64-darwin.gem/],
      "an extra archive" => [->(dir) { File.write(File.join(dir, "connectrpc-hyper-9.9.9-x86_64-linux.gem"), "x") },
                             /Unexpected entries .*9.9.9-x86_64-linux.gem/]
    }.each do |label, (change, failure)|
      it "rejects #{label} before building anything" do
        change.call(archives)
        expect(PackageBuild).not_to receive(:build)
        expect { build }.to raise_error(failure)
        expect(File.exist?(File.dirname(output))).to be(false)
      end
    end

    it "requires the tested archives directory" do
      expect do
        candidate.build!(output: output, revision: "abc123", epoch: epoch, evidence: evidence)
      end.to raise_error(/Tested platform archives directory is missing/)
    end
  end
end
