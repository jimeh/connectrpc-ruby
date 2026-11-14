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
    def initialize(events, published, foreign: [])
      @events = events
      @published = published
      @foreign = foreign
    end

    def sha256(name, _version)
      @published[name]
    end

    def claimable?(name)
      !@foreign.include?(name)
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

  # Net::HTTP is the network boundary; requests are captured per host.
  class FakeHTTP
    def initialize(host, requests, responses)
      @host = host
      @requests = requests
      @responses = responses
    end

    def request(request)
      @requests << [@host, request]
      @responses.fetch([@host, request.path]).call
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

  {
    "different bytes for a version" => [{ published: { "connectrpc-async" => "0" * 64 } },
                                        /connectrpc-async .* on RubyGems.org differs/],
    "other owners for a new gem name" => [{ published: {}, foreign: ["connectrpc-otel"] },
                                          /connectrpc-otel on RubyGems.org has other owners/]
  }.each do |label, (registry_state, failure)|
    it "stops before uploading or pushing anything when RubyGems.org has #{label}" do
      github = ReleasePublisherFakes::FakeGitHub.new(events, draft: true, assets: {})
      registry = ReleasePublisherFakes::FakeRegistry.new(events, registry_state[:published],
                                                         foreign: registry_state.fetch(:foreign, []))

      expect { publisher(github, registry: registry).publish! }.to raise_error(failure)
      expect(events).to eq([[:lookup, "v#{version}"]])
      expect(github.release_record.draft).to be(true)
    end
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

RSpec.describe ReleasePublisher::RubyGems do
  def http_response(klass, code, body)
    klass.new("1.1", code, "").tap do |response|
      response.instance_variable_set(:@body, body)
      response.instance_variable_set(:@read, true)
    end
  end

  let(:requests) { [] }
  let(:log) { StringIO.new }
  let(:environment) do
    { "ACTIONS_ID_TOKEN_REQUEST_URL" => "https://token.actions.example/idtoken?api-version=2.0",
      "ACTIONS_ID_TOKEN_REQUEST_TOKEN" => "request-token" }
  end
  let(:exchange_path) { "/api/v1/oidc/trusted_publisher/exchange_token" }

  def stub_http(responses)
    allow(Net::HTTP).to receive(:start) do |host, _port, use_ssl:, &block|
      raise "Plain HTTP to #{host}" unless use_ssl

      block.call(ReleasePublisherFakes::FakeHTTP.new(host, requests, responses))
    end
  end

  it "exchanges a GitHub OIDC token asserting the owner and gives the key only to gem push" do
    stub_http(
      ["token.actions.example", "/idtoken?api-version=2.0&audience=rubygems.org"] =>
        -> { http_response(Net::HTTPOK, "200", '{"value":"github-jwt"}') },
      ["rubygems.org", exchange_path] =>
        -> { http_response(Net::HTTPCreated, "201", '{"rubygems_api_key":"rubygems-key"}') }
    )
    allow(Open3).to receive(:capture2e).and_return(["Successfully registered gem", instance_double(Process::Status,
                                                                                                   success?: true)])

    described_class.new(owner: "jimeh", environment: environment, log: log).push("/c/a.gem", "/s/a.gem.sigstore.json")

    oidc, exchange = requests.map(&:last)
    expect(oidc["Authorization"]).to eq("Bearer request-token")
    expect(JSON.parse(exchange.body)).to eq("jwt" => "github-jwt", "owner" => "jimeh")
    expect(Open3).to have_received(:capture2e).with({ "GEM_HOST_API_KEY" => "rubygems-key" }, RbConfig.ruby, "-S",
                                                    "gem", "push", "/c/a.gem", "--attestation",
                                                    "/s/a.gem.sigstore.json")
    expect(log.string).to eq("::add-mask::rubygems-key\n")
  end

  it "does not push when the trusted publisher exchange fails" do
    stub_http(
      ["token.actions.example", "/idtoken?api-version=2.0&audience=rubygems.org"] =>
        -> { http_response(Net::HTTPOK, "200", '{"value":"github-jwt"}') },
      ["rubygems.org", exchange_path] => -> { http_response(Net::HTTPNotFound, "404", "no publisher") }
    )
    allow(Open3).to receive(:capture2e)

    expect { described_class.new(owner: "jimeh", environment: environment, log: log).push("/c/a.gem", "/s/a") }
      .to raise_error(/trusted publisher exchange failed: HTTP 404 no publisher/)
    expect(Open3).not_to have_received(:capture2e)
  end

  it "treats a missing gem or one the owner already owns as claimable" do
    owners = '[{"handle":"someone"},{"handle":"JimEh"}]'
    stub_http(
      ["rubygems.org", "/api/v1/gems/new-gem/owners.json"] => -> { http_response(Net::HTTPNotFound, "404", "") },
      ["rubygems.org", "/api/v1/gems/ours/owners.json"] => -> { http_response(Net::HTTPOK, "200", owners) },
      ["rubygems.org", "/api/v1/gems/theirs/owners.json"] =>
        -> { http_response(Net::HTTPOK, "200", '[{"handle":"someone"}]') }
    )
    registry = described_class.new(owner: "jimeh", environment: environment, log: log)

    expect(%w[new-gem ours theirs].map { |name| registry.claimable?(name) }).to eq([true, true, false])
  end
end

RSpec.describe ReleasePublisher::GitHub do
  it "uses uploaded asset digests and rejects an interrupted upload without calling gh" do
    assets = { "a.gem" => { "id" => 1, "state" => "uploaded", "digest" => "sha256:#{'a' * 64}" },
               "b.gem" => { "id" => 2, "state" => "starter", "digest" => nil } }
    release = ReleasePublisher::Release.new(id: 9, tag: "v0.1.0", draft: true, assets: assets)
    github = described_class.new(repository: "owner/repo")
    allow(Open3).to receive(:capture3)

    expect(github.asset_sha256(release, "a.gem")).to eq("a" * 64)
    expect { github.asset_sha256(release, "b.gem") }.to raise_error(/b.gem is incomplete \(starter\)/)
    expect(Open3).not_to have_received(:capture3)
  end
end
