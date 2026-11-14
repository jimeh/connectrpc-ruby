# frozen_string_literal: true

require "digest"
require "json"
require "tmpdir"
require_relative "../scripts/release_publisher"

module ReleasePublisherFakes
  # GitHub, RubyGems.org and Sigstore are external boundaries. The fakes record
  # one shared event log so ordering across them is observable.
  class FakeGitHub
    attr_reader :release_record

    def initialize(events, draft:, assets:)
      @events = events
      @release_record = ReleasePublisher::Release.new(id: 1, tag: nil, draft: draft, assets: assets.dup)
    end

    def release(tag)
      @events << [:lookup, tag]
      @release_record.tag = tag
      @release_record
    end

    def asset_sha256(release, file)
      Digest::SHA256.hexdigest(release.assets.fetch(file))
    end

    def upload(release, path)
      @events << [:upload, File.basename(path)]
      release.assets[File.basename(path)] = File.binread(path)
    end

    def publish(release)
      @events << [:publish, release.id]
      release.draft = false
    end
  end

  class FakeRegistry
    def initialize(events, published)
      @events = events
      @published = published
    end

    def sha256(name, _version)
      @published[name]
    end

    def push(path, attestation)
      @events << [:push, File.basename(path), File.basename(attestation)]
      @published[File.basename(path).delete_suffix("-#{ReleaseTrain.new.version}.gem")] = Digest::SHA256.file(path).hexdigest
    end
  end

  class FakeSigner
    def initialize(events)
      @events = events
    end

    def sign(path)
      @events << [:sign, File.basename(path)]
      "#{path}.sigstore.json"
    end
  end
end

RSpec.describe ReleasePublisher do
  let(:train) { ReleaseTrain.new }
  let(:version) { train.version }
  let(:events) { [] }
  let(:files) { ReleaseTrain::NAMES.map { |name| "#{name}-#{version}.gem" } }

  around do |example|
    Dir.mktmpdir("connectrpc-publish-spec-") do |candidate|
      @candidate = candidate
      example.run
    end
  end

  before do
    artifacts = files.map do |file|
      File.binwrite(File.join(@candidate, file), "archive #{file}")
      { "file" => file, "sha256" => Digest::SHA256.hexdigest("archive #{file}") }
    end
    record = { "version" => version, "revision" => "abc123", "artifacts" => artifacts }
    File.write(File.join(@candidate, "candidate.json"), "#{JSON.generate(record)}\n")
  end

  def bytes(file)
    File.binread(File.join(@candidate, file))
  end

  def sha(file)
    Digest::SHA256.hexdigest(bytes(file))
  end

  def publisher(github, registry: ReleasePublisherFakes::FakeRegistry.new(events, {}), tag: "v#{version}")
    described_class.new(train: train, candidate: @candidate, revision: "abc123", tag: tag, github: github,
                        registry: registry, signer: ReleasePublisherFakes::FakeSigner.new(events), log: StringIO.new)
  end

  it "uploads every asset, attests and pushes gems in dependency order, then publishes the release last" do
    github = ReleasePublisherFakes::FakeGitHub.new(events, draft: true, assets: {})
    publisher(github).publish!

    expect(events.first).to eq([:lookup, "v#{version}"])
    expect(events.select { |event| event.first == :upload }.map(&:last)).to match_array([*files, "candidate.json"])
    pushes = events.select { |event| event.first == :push }
    expect(pushes.map { |event| event[1] }).to eq(files)
    expect(pushes.map(&:last)).to eq(files.map { |file| "#{file}.sigstore.json" })
    expect(events.index([:sign, files.first])).to be > events.rindex { |event| event.first == :upload }
    expect(events.last).to eq([:publish, 1])
    expect(github.release_record.assets.transform_values { |content| Digest::SHA256.hexdigest(content) })
      .to eq([*files, "candidate.json"].to_h { |file| [file, sha(file)] })
  end

  it "resumes from retained assets and published gems without replacing either" do
    retained = ["candidate.json", files[0], files[1]].to_h { |file| [file, bytes(file)] }
    github = ReleasePublisherFakes::FakeGitHub.new(events, draft: true, assets: retained)
    registry = ReleasePublisherFakes::FakeRegistry.new(events,
                                                       { "connectrpc" => sha(files[0]),
                                                         "connectrpc-rack" => sha(files[1]) })
    publisher(github, registry: registry).publish!

    expect(events.select { |event| event.first == :upload }.map(&:last)).to match_array(files.drop(2))
    expect(events.select { |event| event.first == :push }.map { |event| event[1] }).to eq(files.drop(2))
    expect(events.last).to eq([:publish, 1])
  end

  it "stops without publishing when RubyGems.org has different bytes for a version" do
    github = ReleasePublisherFakes::FakeGitHub.new(events, draft: true, assets: {})
    registry = ReleasePublisherFakes::FakeRegistry.new(events, { "connectrpc-async" => "0" * 64 })

    expect { publisher(github, registry: registry).publish! }
      .to raise_error(/connectrpc-async #{Regexp.escape(version)} on RubyGems.org differs/)
    expect(events.select { |event| event.first == :push }.map { |event| event[1] }).to eq(files.first(2))
    expect(events).not_to include([:publish, 1])
    expect(github.release_record.draft).to be(true)
  end

  {
    "a retained asset with different bytes" => [
      { "connectrpc-#{ReleaseTrain.new.version}.gem" => "other" }, /asset connectrpc-.*differs from the candidate/
    ],
    "an unexpected asset" => [{ "notes.txt" => "x" }, /unexpected assets: notes.txt/]
  }.each do |label, (assets, failure)|
    it "refuses a draft with #{label} before uploading or pushing" do
      github = ReleasePublisherFakes::FakeGitHub.new(events, draft: true, assets: assets)
      expect { publisher(github).publish! }.to raise_error(failure)
      expect(events).to eq([[:lookup, "v#{version}"]])
    end
  end

  it "rejects a mismatched tag or a changed candidate before contacting GitHub" do
    github = ReleasePublisherFakes::FakeGitHub.new(events, draft: true, assets: {})
    expect { publisher(github, tag: "v9.9.9").publish! }.to raise_error(/Tag v9.9.9 does not match/)

    File.binwrite(File.join(@candidate, files[3]), "rebuilt")
    expect { publisher(github).publish! }.to raise_error(/Candidate hash differs: #{Regexp.escape(files[3])}/)
    expect(events).to be_empty
  end

  it "verifies an already published release without changing it and rejects an incomplete one" do
    assets = [*files, "candidate.json"].to_h { |file| [file, bytes(file)] }
    complete = ReleaseTrain::NAMES.zip(files).to_h { |name, file| [name, sha(file)] }
    github = ReleasePublisherFakes::FakeGitHub.new(events, draft: false, assets: assets)
    publisher(github, registry: ReleasePublisherFakes::FakeRegistry.new(events, complete)).publish!
    expect(events).to eq([[:lookup, "v#{version}"]])

    incomplete = complete.except("connectrpc-falcon")
    expect { publisher(github, registry: ReleasePublisherFakes::FakeRegistry.new(events, incomplete)).publish! }
      .to raise_error(/no matching connectrpc-falcon-.*on RubyGems.org/)
  end
end
