# frozen_string_literal: true

require_relative "release_train"

train = ReleaseTrain.new
case ARGV
when ["check"]
  train.check!
  puts "Release train #{train.version}: eleven versions, instrumentation, guides and push hosts agree"
when ["prepare"]
  train.prepare!
  puts "Prepared installation guides for #{train.version}; review and sign the diff"
when ["prepare-pr"]
  require_relative "release_preparation"
  # The App-token commit only runs from the Release Please workflow on main.
  workflow = "#{ENV.fetch('GITHUB_REPOSITORY', '')}/.github/workflows/release-please.yml@refs/heads/main"
  unless ENV["GITHUB_ACTIONS"] == "true" && ENV["GITHUB_WORKFLOW_REF"] == workflow
    abort "release:prepare:pr only runs in .github/workflows/release-please.yml on main"
  end

  ReleasePreparation.new(
    train: ReleaseTrain.new(ENV.fetch("RELEASE_ROOT")), repository: ENV.fetch("GITHUB_REPOSITORY"),
    head: ENV.fetch("RELEASE_HEAD"), github: ReleasePreparation::GitHub.new
  ).commit!
when ["build"]
  require_relative "release_candidate"
  puts "Candidate archives: #{ReleaseCandidate.new(train).build_from_git!}"
when ["tools"]
  require_relative "release_tools"
  system(*ReleaseTools.bundle("install"), exception: true)
when ["publish"]
  require "open3"
  require "tmpdir"
  require_relative "release_publisher"
  # Publication only runs from the tag-triggered release workflow, never locally.
  workflow = ENV.fetch("GITHUB_WORKFLOW_REF", "")
  unless ENV["GITHUB_ACTIONS"] == "true" && workflow.include?("/.github/workflows/release.yml@refs/tags/v")
    abort "release:publish only runs in .github/workflows/release.yml on a v* tag"
  end
  revision = Open3.capture2("git", "rev-parse", "HEAD", chdir: train.root).first.strip
  abort "Checkout #{revision} is not GITHUB_SHA #{ENV.fetch('GITHUB_SHA')}" unless revision == ENV.fetch("GITHUB_SHA")

  Dir.mktmpdir("connectrpc-attestations-") do |attestations|
    ReleasePublisher.new(
      train: train, candidate: File.join(train.root, "vendor/release", train.version, revision),
      revision: revision, tag: ENV.fetch("GITHUB_REF_NAME"),
      github: ReleasePublisher::GitHub.new(repository: ENV.fetch("GITHUB_REPOSITORY")),
      registry: ReleasePublisher::RubyGems.new(owner: ENV.fetch("RUBYGEMS_OWNER")),
      signer: ReleasePublisher::Signer.new(attestations)
    ).publish!
  end
else
  abort "Usage: ruby scripts/release.rb check|prepare|prepare-pr|build|tools|publish"
end
