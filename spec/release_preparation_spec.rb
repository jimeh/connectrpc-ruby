# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "stringio"
require "tmpdir"
require_relative "../scripts/release_preparation"

module ReleasePreparationFakes
  # GitHub is the external boundary; the fake records each commit input.
  class FakeGitHub
    attr_reader :inputs

    def initialize
      @inputs = []
    end

    def create_commit(input)
      @inputs << input
      "c" * 40
    end
  end
end

RSpec.describe ReleasePreparation do
  let(:github) { ReleasePreparationFakes::FakeGitHub.new }
  let(:log) { StringIO.new }

  def git(root, *arguments)
    output, error, status = Open3.capture3("git", "-c", "user.name=Release Spec", "-c",
                                           "user.email=spec@example.invalid", "-c", "commit.gpgsign=false",
                                           "-c", "core.hooksPath=/dev/null", *arguments, chdir: root)
    raise "git #{arguments.first} failed: #{error}" unless status.success?

    output.strip
  end

  def commit_all(root, message)
    git(root, "add", "--all")
    git(root, "commit", "--quiet", "--no-verify", "-m", message)
    git(root, "rev-parse", "HEAD")
  end

  # A Release Please branch at a proposed version: the same inputs as the
  # captured release train, committed in a throwaway repository.
  def release_branch(bump)
    Dir.mktmpdir("connectrpc-preparation-spec-") do |root|
      source = File.expand_path("..", __dir__)
      owners = ReleaseTrain::NAMES.map do |name|
        name == "connectrpc" ? "connectrpc.gemspec" : "gems/#{name}/#{name}.gemspec"
      end
      [*owners, *ReleaseTrain::VERSION_FILES.values, *ReleaseTrain::GUIDES,
       ".github/release-please-config.json", ".github/.release-please-manifest.json"].each do |file|
        FileUtils.mkdir_p(File.dirname(File.join(root, file)))
        FileUtils.cp(File.join(source, file), File.join(root, file))
      end
      git(root, "init", "--quiet")
      commit_all(root, "chore: base")
      train = ReleaseTrain.new(root)
      version = propose_version(train, bump)
      yield train, commit_all(root, "chore(main): release #{version}"), version
    end
  end

  def propose_version(train, bump)
    major, minor, patch = train.version.split(".").map(&:to_i)
    version = bump == :patch ? "#{major}.#{minor}.#{patch + 1}" : "#{major}.#{minor + 1}.0"
    config = JSON.parse(train.read(".github/release-please-config.json")).fetch("packages").fetch(".")
    [config.fetch("version-file"), *config.fetch("extra-files")].each do |file|
      path = File.join(train.root, file)
      File.write(path, File.read(path).sub(/((?:VERSION|spec.version) = ")[^"]+/, "\\1#{version}"))
    end
    File.write(File.join(train.root, ".github/.release-please-manifest.json"), JSON.generate({ "." => version }))
    version
  end

  # The local release:prepare result for the same tree, from a separate copy.
  def locally_prepared(train)
    Dir.mktmpdir("connectrpc-preparation-reference-") do |copy|
      FileUtils.cp_r("#{train.root}/.", copy)
      ReleaseTrain.new(copy).prepare!
      ReleaseTrain::GUIDES.to_h { |file| [file, File.read(File.join(copy, file))] }
                          .reject { |file, text| File.exist?(File.join(train.root, file)) && text == train.read(file) }
    end
  end

  def preparation(train, head)
    described_class.new(train: train, repository: "owner/repo", head: head, github: github, log: log)
  end

  def decoded_additions(input)
    input.dig(:fileChanges, :additions).to_h do |addition|
      [addition.fetch(:path), addition.fetch(:contents).unpack1("m0")]
    end
  end

  it "commits exactly the guides release:prepare would change for a minor release, against the checked-out head" do
    release_branch(:minor) do |train, _head, version|
      expected = locally_prepared(train)
      # A guide that already quotes the new requirement is left out of the commit.
      File.write(File.join(train.root, "docs/design.md"), expected.delete("docs/design.md"))
      head = commit_all(train.root, "docs: prepare one guide by hand")
      allow(Gem::Specification).to receive(:load).and_call_original

      expect(preparation(train, head).commit!).to eq("c" * 40)

      expect(github.inputs.size).to eq(1)
      input = github.inputs.first
      expect(input.fetch(:branch)).to eq(repositoryNameWithOwner: "owner/repo",
                                         branchName: "release-please--branches--main--components--connectrpc")
      expect(input.fetch(:expectedHeadOid)).to eq(head)
      expect(input.dig(:message, :headline)).to eq("chore: prepare installation guides for #{version}")
      expect(expected.size).to eq(ReleaseTrain::GUIDES.size - 1)
      expect(decoded_additions(input)).to eq(expected)
      expect(input.dig(:fileChanges, :additions).map { |addition| addition.fetch(:path) }).to eq(expected.keys.sort)
      # Nothing from the release tree is evaluated or written.
      expect(Gem::Specification).not_to have_received(:load)
      expect(git(train.root, "status", "--porcelain")).to be_empty
      expect(git(train.root, "rev-parse", "HEAD")).to eq(head)
    end
  end

  it "makes no API call for a patch release" do
    release_branch(:patch) do |train, head, _version|
      expect(preparation(train, head).commit!).to be_nil
      expect(github.inputs).to be_empty
      expect(log.string).to include("nothing to commit")
    end
  end

  it "refuses a checkout that is not the expected head before calling GitHub" do
    release_branch(:minor) do |train, _head, _version|
      expect { preparation(train, "0" * 40).commit! }.to raise_error(/Release checkout is at \h{40}, expected 0{40}/)
      expect(github.inputs).to be_empty
    end
  end

  it "skips a guide that main lists but the release branch lacks" do
    release_branch(:minor) do |train, _head, _version|
      missing = "gems/connectrpc-sentry/README.md"
      expected = locally_prepared(train)
      expect(expected).to include(missing)
      git(train.root, "rm", "--quiet", missing)
      head = commit_all(train.root, "chore: drop a guide")

      preparation(train, head).commit!

      expect(decoded_additions(github.inputs.first)).to eq(expected.except(missing))
      expect(log.string).to include("Skipping #{missing}: not in the release tree")
    end
  end

  it "refuses a guide symlinked outside the release tree before calling GitHub" do
    release_branch(:minor) do |train, _head, _version|
      Dir.mktmpdir("connectrpc-preparation-outside-") do |runner|
        outside = File.join(runner, "secret.md")
        File.write(outside, "gem \"connectrpc\", \"~> 0.0.1\"\n")
        FileUtils.rm(File.join(train.root, "docs/unary.md"))
        File.symlink(outside, File.join(train.root, "docs/unary.md"))
        head = commit_all(train.root, "chore: link a guide")

        expect { preparation(train, head).commit! }.to raise_error(%r{Refusing docs/unary.md: not a regular file})
        expect(github.inputs).to be_empty
      end
    end
  end

  let(:script) { File.expand_path("../scripts/release.rb", __dir__) }
  let(:workflow_ref) { "o/r/.github/workflows/release-please.yml@refs/heads/main" }

  it "commits through gh when run from the Release Please workflow on main" do
    release_branch(:minor) do |train, head, _version|
      Dir.mktmpdir("connectrpc-preparation-gh-") do |bin|
        body = File.join(bin, "body.json")
        File.write(File.join(bin, "gh"), <<~SH)
          #!/bin/sh
          cat > "#{body}"
          echo '{"data":{"createCommitOnBranch":{"commit":{"oid":"#{'d' * 40}"}}}}'
        SH
        File.chmod(0o755, File.join(bin, "gh"))
        environment = { "GITHUB_ACTIONS" => "true", "GITHUB_REPOSITORY" => "o/r", "GITHUB_WORKFLOW_REF" => workflow_ref,
                        "RELEASE_ROOT" => train.root, "RELEASE_HEAD" => head,
                        "PATH" => "#{bin}:#{ENV.fetch('PATH')}" }

        stdout, stderr, status = Open3.capture3(environment, RbConfig.ruby, script, "prepare-pr")

        expect(status.success?).to be(true), stderr
        expect(stdout).to include("d" * 40)
        input = JSON.parse(File.read(body)).dig("variables", "input")
        expect(input.fetch("branch")).to eq("repositoryNameWithOwner" => "o/r", "branchName" => described_class::BRANCH)
        expect(input.fetch("expectedHeadOid")).to eq(head)
        expect(input.dig("fileChanges", "additions").map { |addition| addition.fetch("path") })
          .to eq(locally_prepared(train).keys.sort)
      end
    end
  end

  it "refuses to run outside the Release Please workflow on main" do
    {
      "local" => {},
      "release tag" => { "GITHUB_ACTIONS" => "true", "GITHUB_REPOSITORY" => "o/r",
                         "GITHUB_WORKFLOW_REF" => "o/r/.github/workflows/release.yml@refs/tags/v0.2.0" },
      "other branch" => { "GITHUB_ACTIONS" => "true", "GITHUB_REPOSITORY" => "o/r",
                          "GITHUB_WORKFLOW_REF" => "o/r/.github/workflows/release-please.yml@refs/heads/topic" },
      "branch prefixed with main" => { "GITHUB_ACTIONS" => "true", "GITHUB_REPOSITORY" => "o/r",
                                       "GITHUB_WORKFLOW_REF" => "#{workflow_ref}tenance" },
      "other repository" => { "GITHUB_ACTIONS" => "true", "GITHUB_REPOSITORY" => "o/r",
                              "GITHUB_WORKFLOW_REF" => workflow_ref.sub("o/r", "x/y") }
    }.each do |label, environment|
      cleared = { "GITHUB_ACTIONS" => nil, "GITHUB_REPOSITORY" => nil, "GITHUB_WORKFLOW_REF" => nil, "GH_TOKEN" => nil }
      _stdout, stderr, status = Open3.capture3(cleared.merge(environment), RbConfig.ruby, script, "prepare-pr")
      expect(status.success?).to be(false), label
      expect(stderr).to include("release:prepare:pr only runs in .github/workflows/release-please.yml on main"), label
    end
  end
end

RSpec.describe ReleasePreparation::GitHub do
  let(:input) { { branch: { repositoryNameWithOwner: "owner/repo", branchName: "branch" }, expectedHeadOid: "a" * 40 } }

  def status(success)
    instance_double(Process::Status, success?: success)
  end

  it "sends the mutation and its input to gh on stdin and returns the new commit" do
    response = { data: { createCommitOnBranch: { commit: { oid: "b" * 40, url: "https://github.invalid/c" } } } }
    allow(Open3).to receive(:capture3).and_return([JSON.generate(response), "", status(true)])

    expect(described_class.new.create_commit(input)).to eq("b" * 40)
    expect(Open3).to have_received(:capture3) do |*command, stdin_data:|
      expect(command).to eq(%w[gh api graphql --input -])
      body = JSON.parse(stdin_data)
      expect(body.keys).to eq(%w[query variables])
      expect(body.fetch("query")).to include("createCommitOnBranch(input: $input)")
      expect(body.fetch("variables")).to eq("input" => JSON.parse(JSON.generate(input)))
    end
  end

  {
    "a failed gh call" => [["", "HTTP 401: Bad credentials", false], /gh api graphql failed: HTTP 401/],
    "a GraphQL error" => [[JSON.generate(errors: [{ type: "STALE_DATA", message: "Expected head" }]), "", true],
                          /createCommitOnBranch failed: .*STALE_DATA/],
    "a response without a commit" => [[JSON.generate(data: { createCommitOnBranch: nil }), "", true],
                                      /createCommitOnBranch returned no commit/]
  }.each do |label, ((stdout, stderr, success), failure)|
    it "raises on #{label}" do
      allow(Open3).to receive(:capture3).and_return([stdout, stderr, status(success)])
      expect { described_class.new.create_commit(input) }.to raise_error(failure)
    end
  end
end
