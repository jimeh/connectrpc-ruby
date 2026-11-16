# frozen_string_literal: true

require "json"
require "open3"
require_relative "release_train"

# Commits prepared installation guides to the open release PR as the release
# App. The release checkout is data: its version file and guides are read as
# text and nothing from it runs while the App token is in the environment.
class ReleasePreparation
  BRANCH = "release-please--branches--main--components--connectrpc"

  def initialize(train:, repository:, head:, github:, log: $stdout)
    @train = train
    @repository = repository
    @head = head
    @github = github
    @log = log
  end

  def commit!
    verify_head!
    changes = changed_guides
    if changes.empty?
      @log.puts "Installation guides already quote #{@train.requirement}; nothing to commit"
      return
    end

    oid = @github.create_commit(commit_input(changes))
    @log.puts "Committed #{changes.size} prepared installation guides for #{@train.version} to #{BRANCH}: #{oid}"
    oid
  end

  private

  def verify_head!
    revision, error, status = Open3.capture3("git", "rev-parse", "HEAD", chdir: @train.root)
    raise "Cannot read the release checkout HEAD: #{error}" unless status.success?
    return if revision.strip == @head

    raise "Release checkout is at #{revision.strip}, expected #{@head}"
  end

  def changed_guides
    root = File.realpath(@train.root)
    ReleaseTrain::GUIDES.sort.each_with_object({}) do |file, changes|
      current = guide(root, file)
      next unless current

      prepared = @train.prepared_guide(current)
      changes[file] = prepared unless prepared == current
    end
  end

  # Main may list a guide that an older release branch lacks.
  def guide(root, file)
    path = File.join(root, file)
    unless File.exist?(path)
      @log.puts "Skipping #{file}: not in the release tree"
      return
    end
    # A symlink could make the commit carry runner files instead of guide text.
    unless File.file?(path) && File.realpath(path) == path
      raise "Refusing #{file}: not a regular file inside the release tree"
    end

    File.read(path)
  end

  def commit_input(changes)
    {
      branch: { repositoryNameWithOwner: @repository, branchName: BRANCH },
      expectedHeadOid: @head,
      message: { headline: "chore: prepare installation guides for #{@train.version}" },
      # pack("m0") is strict Base64; the base64 gem is no longer a default gem on Ruby 3.4.
      fileChanges: { additions: changes.map { |path, text| { path: path, contents: [text].pack("m0") } } }
    }
  end

  # GraphQL createCommitOnBranch through the preinstalled gh CLI. An App
  # installation token makes GitHub sign the commit, and a stale
  # expectedHeadOid fails the mutation instead of overwriting the branch.
  class GitHub
    MUTATION = <<~GRAPHQL
      mutation($input: CreateCommitOnBranchInput!) {
        createCommitOnBranch(input: $input) { commit { oid url } }
      }
    GRAPHQL

    def create_commit(input)
      body = JSON.generate(query: MUTATION, variables: { input: input })
      output, error, status = Open3.capture3("gh", "api", "graphql", "--input", "-", stdin_data: body)
      raise "gh api graphql failed: #{error}#{output}" unless status.success?

      response = JSON.parse(output)
      raise "createCommitOnBranch failed: #{JSON.generate(response['errors'])}" if response["errors"]

      oid = response.dig("data", "createCommitOnBranch", "commit", "oid")
      raise "createCommitOnBranch returned no commit: #{output}" unless oid

      oid
    end
  end
end
