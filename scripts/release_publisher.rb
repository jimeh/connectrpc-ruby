# frozen_string_literal: true

require "digest"
require "json"
require "net/http"
require "open3"
require "rbconfig"
require "uri"
require_relative "release_tools"
require_relative "release_train"

# Publishes one canonical candidate from its release tag. Every step can be
# re-run: draft assets and registry versions that already exist must match the
# candidate bytes, and the GitHub release is published only after all seven gems.
class ReleasePublisher # rubocop:disable Metrics/ClassLength
  Artifact = Struct.new(:name, :file, :path, :sha256, keyword_init: true)
  # assets maps each asset name to an adapter-specific handle.
  Release = Struct.new(:id, :tag, :draft, :assets, keyword_init: true)

  def initialize(train:, candidate:, revision:, tag:, github:, registry:, signer:, log: $stdout)
    @train = train
    @candidate = candidate
    @revision = revision
    @tag = tag
    @github = github
    @registry = registry
    @signer = signer
    @log = log
  end

  def publish! # rubocop:disable Metrics/MethodLength
    gems, record = verify_candidate!
    release = @github.release(@tag)
    if release.draft
      published = registry_state!(gems)
      stage!(release, [*gems, record])
      push!(gems, published)
      @github.publish(release)
      @log.puts "Published #{@tag}: seven gems on RubyGems.org and the GitHub release"
    else
      verify_published!(release, gems, record)
      @log.puts "#{@tag} is already published with matching assets and gems"
    end
  end

  private

  def version
    @train.version
  end

  def verify_candidate! # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    raise "Tag #{@tag} does not match release train v#{version}" unless @tag == "v#{version}"

    record_path = File.join(@candidate, "candidate.json")
    record = JSON.parse(File.read(record_path))
    unless record["version"] == version && record["revision"] == @revision
      raise "Candidate #{@candidate} is not #{version} at #{@revision}"
    end

    hashes = record.fetch("artifacts").to_h { |artifact| [artifact.fetch("file"), artifact.fetch("sha256")] }
    specifications = @train.specifications
    files = specifications.map(&:file_name)
    unless hashes.keys.sort == files.sort && Dir.children(@candidate).sort == [*files, "candidate.json"].sort
      raise "Candidate artifact inventory differs: #{@candidate}"
    end

    published = []
    gems = specifications.map do |spec|
      local = spec.runtime_dependencies.map(&:name) & ReleaseTrain::NAMES
      raise "Publication order puts #{spec.name} before #{local - published}" unless (local - published).empty?

      published << spec.name
      artifact(spec.name, spec.file_name).tap do |gem|
        raise "Candidate hash differs: #{gem.file}" unless gem.sha256 == hashes.fetch(gem.file)
      end
    end
    [gems, artifact(nil, "candidate.json")]
  end

  def artifact(name, file)
    path = File.join(@candidate, file)
    Artifact.new(name: name, file: file, path: path, sha256: Digest::SHA256.file(path).hexdigest)
  end

  def verify_assets!(release, artifacts) # rubocop:disable Metrics/AbcSize
    unexpected = release.assets.keys - artifacts.map(&:file)
    raise "GitHub release #{@tag} has unexpected assets: #{unexpected.sort.join(', ')}" if unexpected.any?

    artifacts.select { |artifact| release.assets.key?(artifact.file) }.each do |artifact|
      next if @github.asset_sha256(release, artifact.file) == artifact.sha256

      raise "GitHub release #{@tag} asset #{artifact.file} differs from the candidate; stop for a maintainer decision"
    end
  end

  def stage!(release, artifacts)
    verify_assets!(release, artifacts)
    artifacts.reject { |artifact| release.assets.key?(artifact.file) }.each do |artifact|
      @github.upload(release, artifact.path)
      @log.puts "Uploaded #{artifact.file} to draft #{@tag}"
    end
  end

  # Check every gem before uploading or pushing anything, so a conflict cannot
  # strand a partially published train. Ownership is checked even when bytes
  # match: reproducible builds let anyone push identical archives first.
  def registry_state!(gems)
    gems.to_h do |gem|
      unless @registry.claimable?(gem.name)
        raise "#{gem.name} on RubyGems.org has other owners; stop for a maintainer decision"
      end

      published = @registry.sha256(gem.name, version)
      if published && published != gem.sha256
        raise "#{gem.name} #{version} on RubyGems.org differs from the candidate; stop for a maintainer decision"
      end

      [gem.name, published]
    end
  end

  def push!(gems, published)
    gems.each do |gem|
      if published[gem.name]
        @log.puts "#{gem.file} is already on RubyGems.org with the candidate's bytes"
      else
        @registry.push(gem.path, @signer.sign(gem.path))
        @log.puts "Pushed #{gem.file} with a Sigstore attestation"
      end
    end
  end

  def verify_published!(release, gems, record)
    verify_assets!(release, [*gems, record])
    missing = [*gems, record].map(&:file) - release.assets.keys
    raise "Published release #{@tag} is missing assets: #{missing.join(', ')}" if missing.any?

    gems.each do |gem|
      next if @registry.sha256(gem.name, version) == gem.sha256

      raise "Published release #{@tag} has no matching #{gem.file} on RubyGems.org; stop for a maintainer decision"
    end
  end

  # Release lookup, assets and publication through the preinstalled gh CLI.
  class GitHub
    def initialize(repository:, wait: 300, interval: 10)
      @repository = repository
      @wait = wait
      @interval = interval
    end

    # Release Please creates the tag immediately before its draft release, so
    # the tag-triggered run can start first. Poll the external API, bounded.
    def release(tag)
      deadline = now + @wait
      loop do
        found = find(tag)
        return found if found
        raise "No GitHub release for #{tag} after #{@wait}s" if now > deadline

        sleep @interval
      end
    end

    def asset_sha256(release, file)
      asset = release.assets.fetch(file)
      unless asset["state"] == "uploaded"
        raise "Draft asset #{file} is incomplete (#{asset['state']}); delete that asset and re-run"
      end

      digest = asset["digest"].to_s.delete_prefix("sha256:")
      return digest unless digest.empty?

      Digest::SHA256.hexdigest(gh("api", "-H", "Accept: application/octet-stream",
                                  "repos/#{@repository}/releases/assets/#{asset.fetch('id')}"))
    end

    def upload(release, path)
      gh("release", "upload", release.tag, path, "--repo", @repository)
    end

    def publish(release)
      gh("api", "--method", "PATCH", "repos/#{@repository}/releases/#{release.id}",
         "-F", "draft=false", "-f", "make_latest=true")
    end

    private

    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def find(tag)
      query = ".[] | select(.tag_name == #{JSON.generate(tag)}) | " \
              "{id, draft, assets: [.assets[] | {id, name, digest, state}]}"
      found = gh("api", "--paginate", "repos/#{@repository}/releases?per_page=100", "--jq", query)
              .lines.map { |line| JSON.parse(line) }
      raise "Multiple GitHub releases use #{tag}" if found.size > 1
      return if found.empty?

      data = found.first
      Release.new(id: data.fetch("id"), tag: tag, draft: data.fetch("draft"),
                  assets: data.fetch("assets").to_h { |asset| [asset.fetch("name"), asset] })
    end

    def gh(*arguments)
      output, error, status = Open3.capture3("gh", *arguments, binmode: true)
      raise "gh #{arguments.first(2).join(' ')} failed: #{error}" unless status.success?

      output
    end
  end

  # RubyGems.org trusted publishing. Each push exchanges a fresh GitHub OIDC
  # token for a 15-minute push key; the owner lets a pending publisher create a
  # new gem.
  class RubyGems
    HOST = "https://rubygems.org"

    def initialize(owner:, environment: ENV, log: $stdout)
      @owner = owner
      @environment = environment
      @log = log
    end

    def sha256(name, version)
      response = request(Net::HTTP::Get.new(URI("#{HOST}/api/v1/versions/#{name}.json")))
      return if response.is_a?(Net::HTTPNotFound)
      raise "RubyGems.org versions lookup for #{name} failed: HTTP #{response.code}" unless response.is_a?(Net::HTTPOK)

      JSON.parse(response.body).find do |entry|
        entry["number"] == version && entry["platform"] == "ruby"
      end&.fetch("sha")
    end

    # Pending publishers do not reserve names, so an existing gem must already
    # belong to the owner.
    def claimable?(name)
      response = request(Net::HTTP::Get.new(URI("#{HOST}/api/v1/gems/#{name}/owners.json")))
      return true if response.is_a?(Net::HTTPNotFound)

      owners = JSON.parse(checked(response, "RubyGems.org owners lookup for #{name}").body)
      owners.any? { |owner| owner["handle"].to_s.casecmp?(@owner) }
    end

    def push(path, attestation)
      output, status = Open3.capture2e({ "GEM_HOST_API_KEY" => api_key }, RbConfig.ruby, "-S", "gem", "push",
                                       path, "--attestation", attestation)
      raise "gem push #{File.basename(path)} failed:\n#{output}" unless status.success?
    end

    private

    def api_key # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      url = @environment.fetch("ACTIONS_ID_TOKEN_REQUEST_URL") do
        raise "Trusted publishing needs a GitHub Actions job with id-token: write"
      end
      uri = URI(url)
      uri.query = URI.encode_www_form([*URI.decode_www_form(uri.query.to_s), %w[audience rubygems.org]])
      oidc = Net::HTTP::Get.new(uri)
      oidc["Authorization"] = "Bearer #{@environment.fetch('ACTIONS_ID_TOKEN_REQUEST_TOKEN')}"
      jwt = JSON.parse(checked(request(oidc), "GitHub OIDC token request").body).fetch("value")

      exchange = Net::HTTP::Post.new(URI("#{HOST}/api/v1/oidc/trusted_publisher/exchange_token"))
      exchange["Content-Type"] = "application/json"
      exchange["Accept"] = "application/json"
      exchange.body = JSON.generate(jwt: jwt, owner: @owner)
      key = JSON.parse(checked(request(exchange), "RubyGems.org trusted publisher exchange").body)
                .fetch("rubygems_api_key")
      @log.puts "::add-mask::#{key}"
      key
    end

    def checked(response, label)
      return response if response.is_a?(Net::HTTPSuccess)

      raise "#{label} failed: HTTP #{response.code} #{response.body}"
    end

    def request(request)
      request["User-Agent"] = "connectrpc-ruby-release"
      Net::HTTP.start(request.uri.host, request.uri.port, use_ssl: true) { |http| http.request(request) }
    end
  end

  # Keyless Sigstore signing with the job's GitHub OIDC identity.
  class Signer
    def initialize(directory)
      @directory = directory
    end

    def sign(path)
      bundle = File.join(@directory, "#{File.basename(path)}.sigstore.json")
      output, status = Open3.capture2e(*ReleaseTools.bundle("exec", "sigstore-cli", "sign", path, "--bundle", bundle))
      return bundle if status.success? && File.size?(bundle)

      raise "Sigstore signing failed for #{File.basename(path)}:\n#{output}"
    end
  end
end
