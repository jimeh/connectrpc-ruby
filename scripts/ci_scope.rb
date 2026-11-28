# frozen_string_literal: true

require "json"
require "open3"
require_relative "release_train"

# Selects the ci.yml jobs a change can affect. The pure part (CIScope.select,
# Graph#affected) classifies changed paths; Git and CIScope.main read the
# range from the event and write the selection to the step outputs. Anything
# it cannot classify, or any git failure, selects every job.
module CIScope # rubocop:disable Metrics/ModuleLength
  ROOT = File.expand_path("..", __dir__)
  ROOT_PACKAGE = "connectrpc"
  # Selects the job for any affected package.
  ANY = :any

  # ci.yml job id => the packages whose bundles or files the job exercises.
  # A job runs when one of them is affected; the path rules below add fixed
  # triggers. Specs read every gem's gemspec, scripts and versions, and RBS
  # checks every package, so both take any gem change.
  JOBS = {
    "static" => [],
    "specs" => ANY,
    # generation:check reads the health gem's vendored schema.
    "package" => %w[connectrpc connectrpc-health],
    "interop" => %w[connectrpc],
    "conformance" => %w[connectrpc],
    "rails" => %w[connectrpc-on-rails],
    # Selected through FLOORS.
    "floors" => [],
    "async_specs" => %w[connectrpc-async],
    "async_interop" => %w[connectrpc-async],
    "rack" => %w[connectrpc-rack],
    "testing" => %w[connectrpc-testing],
    "rbs" => ANY,
    "falcon_specs" => %w[connectrpc-falcon],
    "falcon_interop" => %w[connectrpc-falcon],
    "otel" => %w[connectrpc-otel],
    "prometheus" => %w[connectrpc-prometheus],
    "yabeda" => %w[connectrpc-yabeda],
    "health" => %w[connectrpc-health],
    "sentry" => %w[connectrpc-sentry],
    "hyper" => %w[connectrpc-hyper],
    "hyper_msrv" => %w[connectrpc-hyper],
    "hyper_gems" => %w[connectrpc-hyper],
    "hyper_windows" => %w[connectrpc-hyper],
    # Browser tasks run from the OTel bundle, then the Async, Falcon and Rack web conformance.
    "browser_grpc_web" => %w[connectrpc-otel connectrpc-async connectrpc-falcon connectrpc-rack],
    # Rack, Async and Falcon example bundles plus the Rails 8.1 lane.
    "examples" => %w[connectrpc-rack connectrpc-on-rails connectrpc-async connectrpc-falcon]
  }.freeze

  # Lowest-allowed matrix leaf => package, in matrix order.
  FLOORS = {
    "async" => "connectrpc-async", "falcon" => "connectrpc-falcon", "rack" => "connectrpc-rack",
    "rails" => "connectrpc-on-rails", "otel" => "connectrpc-otel", "prometheus" => "connectrpc-prometheus",
    "yabeda" => "connectrpc-yabeda", "health" => "connectrpc-health", "sentry" => "connectrpc-sentry",
    "hyper" => "connectrpc-hyper"
  }.freeze

  # A diff that only bumps versions changes archive versions and the internal
  # `~> M.m.0` requirement every gemspec derives: release:check (Static), the
  # core and Rails package checks, rbs:package and the hyper candidate cover it.
  RELEASE_JOBS = %w[static package rails rbs hyper_gems].freeze
  RELEASE_FILES = ["CHANGELOG.md", ".github/.release-please-manifest.json", *ReleaseTrain::VERSION_FILES.values].freeze
  VERSION_LITERAL = /^(\s*VERSION = )"[^"\n]*"/
  # ReleaseTrain requires this file, and Static, Package, Rails and every
  # package check load ReleaseTrain.
  FULL_GEM_FILES = %w[gems/connectrpc-hyper/scripts/platforms.rb].freeze
  # Paths outside docs and gems, in order: the first match decides. A nil job
  # list selects every job.
  PATH_RULES = [
    [%r{\Aspec/[^/]+_spec\.rb\z}, "root spec", %w[static specs]],
    # Gem specs, interop, generation, packages and typecheck read these.
    [%r{\Aspec/}, "shared spec support or fixture", nil],
    # release_candidate.rb and package_build_spec use it.
    [%r{\Atypecheck/archive\.rb\z}, "release candidate archive check", nil],
    # Examples runs rbs:cache-platform for its cache key.
    [%r{\Atypecheck/cache_platform\.rb\z}, "cache platform", %w[static specs rbs examples]],
    [%r{\Atypecheck/}, "typecheck", %w[static specs rbs]],
    # Browser tasks run from the OTel bundle; rake syntax parses browser/**/*.rb.
    [%r{\Abrowser/}, "browser", %w[static specs browser_grpc_web]],
    *FULL_GEM_FILES.map { |path| [/\A#{Regexp.escape(path)}\z/, "loaded by ReleaseTrain", nil] }
  ].freeze
  REGULAR_MODES = %w[100644 100755].freeze
  # Symlinks and submodules.
  FULL_MODES = %w[120000 160000].freeze
  SUPPORTED_STATUSES = %w[A M D].freeze

  # One `git diff --raw` entry. Contents are loaded only for version files.
  Change = Struct.new(:status, :path, :old_mode, :new_mode, :old_blob, :new_blob, :old_content, :new_content,
                      keyword_init: true)
  # How one path selects jobs: fixed job ids plus affected packages, or a
  # reason that selects every job.
  Rule = Struct.new(:label, :jobs, :packages, :full, keyword_init: true)
  Selection = Struct.new(:mode, :reason, :jobs, :floors, :files, keyword_init: true)

  module_function

  def full(reason, files = [])
    Selection.new(mode: "full", reason: reason, jobs: JOBS.keys, floors: FLOORS.keys, files: files)
  end

  # changes: Change records for the whole diff. graph: the package Graph.
  def select(changes, graph)
    return full("the diff is empty") if changes.empty?
    return release(changes) if release_only?(changes)

    files = changes.map { |change| [change, classify(change, graph)] }
    fulls = files.select { |_change, rule| rule.full }
    return full(full_reason(fulls), files) unless fulls.empty?

    scoped(files)
  end

  def scoped(files)
    affected = files.flat_map { |_change, rule| rule.packages }.uniq
    floors = FLOORS.filter_map { |leaf, package| leaf if affected.include?(package) }
    jobs = ["static", *files.flat_map { |_change, rule| rule.jobs }, *package_jobs(affected)]
    jobs << "floors" unless floors.empty?
    Selection.new(mode: "scoped", reason: affected_reason(affected), jobs: JOBS.keys & jobs, floors: floors,
                  files: files)
  end

  def affected_reason(affected)
    affected.empty? ? "no package changed" : "affected packages: #{affected.sort.join(', ')}"
  end

  def package_jobs(affected)
    JOBS.filter_map { |id, packages| id if packages == ANY ? affected.any? : packages.intersect?(affected) }
  end

  def full_reason(fulls)
    change, rule = fulls.first
    more = fulls.size > 1 ? " (and #{fulls.size - 1} more)" : ""
    "#{display(change.path)}: #{rule.full}#{more}"
  end

  def release(changes)
    files = changes.map { |change| [change, Rule.new(label: "release", jobs: RELEASE_JOBS, packages: [])] }
    Selection.new(mode: "scoped", reason: "release-owned files only, version literals only",
                  jobs: JOBS.keys & RELEASE_JOBS, floors: [], files: files)
  end

  # Release eligibility covers the whole diff: it bumps at least one version
  # file, every file is release-owned and modified in place as a regular file,
  # and each version file changes only its VERSION literal.
  def release_only?(changes)
    changes.any? { |change| version_file?(change.path) } && changes.all? { |change| release_change?(change) }
  end

  def release_change?(change)
    RELEASE_FILES.include?(change.path) && change.status == "M" && change.old_mode == change.new_mode &&
      REGULAR_MODES.include?(change.new_mode) && (!version_file?(change.path) || version_literal_only?(change))
  end

  def version_file?(path) = ReleaseTrain::VERSION_FILES.value?(path)

  def version_literal_only?(change)
    contents = [change.old_content, change.new_content]
    return false unless contents.all? { |content| content&.scan(VERSION_LITERAL)&.size == 1 }

    contents.map { |content| content.sub(VERSION_LITERAL, '\1"*"') }.uniq.size == 1
  end

  def classify(change, graph)
    unsupported = unsupported(change) || outside_package(change.path, graph)
    return full_rule(unsupported) if unsupported
    return docs_rule(change) if docs?(change.path)

    _pattern, label, jobs = PATH_RULES.find { |pattern, *| pattern.match?(change.path) }
    return jobs ? rule(label, jobs) : full_rule(label) if label

    gem_rule(change.path, graph)
  end

  def unsupported(change)
    return "path is not valid UTF-8" unless change.path.valid_encoding?
    return "unsupported diff status #{change.status}" unless SUPPORTED_STATUSES.include?(change.status)

    mode = [change.old_mode, change.new_mode].find { |entry| FULL_MODES.include?(entry) }
    "symlink or submodule (mode #{mode})" if mode
  end

  # A gems/ path must sit inside a package directory other than the root
  # package's, even for the README exception.
  def outside_package(path, graph)
    return unless path.start_with?("gems/")

    package = path[%r{\Agems/([^/]+)/}, 1]
    return "not inside a package directory" unless package
    return if package != ROOT_PACKAGE && graph.node?(package)

    "gems/#{display(package)} is not a package directory"
  end

  # Gemspecs list docs and READMEs by name, so a deletion can break a build.
  # Package checks assert shipped doc names, not content; Static's
  # release:check reads the installation guides, as do two root specs.
  def docs_rule(change)
    return full_rule("docs deletion: gemspecs list docs by name") if change.status == "D"
    return full_rule("docs with mode #{change.new_mode}") unless REGULAR_MODES.include?(change.new_mode)

    ReleaseTrain::GUIDES.include?(change.path) ? rule("installation guide", %w[static specs]) : rule("docs", %w[static])
  end

  # outside_package has already checked any gems/ path.
  def gem_rule(path, graph)
    package = path[%r{\Agems/([^/]+)/}, 1]
    return full_rule("outside the scoped paths") unless package

    Rule.new(label: "gem #{package}", jobs: [], packages: graph.affected(package))
  end

  def rule(label, jobs) = Rule.new(label: label, jobs: jobs, packages: [])

  def full_rule(reason) = Rule.new(label: "full", jobs: [], packages: [], full: reason)

  def docs?(path)
    path.start_with?("docs/", "img/") || (!path.include?("/") && path.end_with?(".md")) ||
      path.match?(%r{\Agems/[^/]+/README\.md\z})
  end

  # Package nodes and their path dependencies, read from each package's
  # Gemfile and gemfiles/*.gemfile.
  class Graph
    PATH_OPTION = /\bpath:\s*(["'])([^"']+)\1/

    attr_reader :directories, :edges

    def self.load(root = ROOT)
      root = File.expand_path(root)
      packages = Dir[File.join(root, "gems/*/Gemfile")].map { |file| File.basename(File.dirname(file)) }.sort
      directories = { ROOT_PACKAGE => root, **packages.to_h { |name| [name, File.join(root, "gems", name)] } }
      new(directories)
    end

    # Absolute directories each path: option names, relative to the file.
    def self.path_sources(file)
      File.read(file).scan(PATH_OPTION).map { |_quote, path| File.expand_path(path, File.dirname(file)) }.uniq
    end

    def self.gemfiles(directory)
      [File.join(directory, "Gemfile"), *Dir[File.join(directory, "gemfiles/*.gemfile")]]
        .select { |file| File.file?(file) }
    end

    def initialize(directories)
      @directories = directories
      @edges = directories.to_h do |name, directory|
        targets = self.class.gemfiles(directory).flat_map { |file| self.class.path_sources(file) }
        [name, (targets.map { |target| package_at(target) }.uniq - [name, ROOT_PACKAGE]).sort]
      end
    end

    def node?(name) = directories.key?(name)

    # The package plus every package whose path dependency closure contains it.
    def affected(name)
      raise ArgumentError, "unknown package #{name}" unless node?(name)

      [name, *directories.keys.select { |other| other != name && closure(other).include?(name) }]
    end

    def closure(name, seen = [])
      edges.fetch(name).each do |target|
        next if seen.include?(target)

        seen << target
        closure(target, seen)
      end
      seen
    end

    private

    def package_at(target)
      found = directories.find { |_name, directory| File.expand_path(directory) == target }
      raise "#{target} is not a known package directory" unless found

      found.first
    end
  end

  # Reads changes and commits through the git CLI. Every failure raises Error.
  class Git
    class Error < StandardError; end
    SHA = /\A\h{40}\z/

    def initialize(root = ROOT)
      @root = root
    end

    def run(*args)
      output, error, status = Open3.capture3("git", *args, chdir: @root)
      raise Error, "git #{args.first} failed: #{error.strip}" unless status.success?

      output
    end

    def commit?(revision)
      _output, _error, status = Open3.capture3("git", "cat-file", "-e", "#{revision}^{commit}", chdir: @root)
      status.success?
    end

    def fetch(sha) = run("fetch", "--no-tags", "--depth=1", "origin", sha)

    def resolve(revision) = run("rev-parse", "--verify", "--end-of-options", "#{revision}^{commit}").strip

    def parents(revision) = run("rev-list", "--parents", "-n", "1", revision).split

    # Paths are raw bytes; one that is not valid UTF-8 keeps its bytes as an
    # invalid UTF-8 string, which classify selects every job for.
    def changes(base, head)
      output = run("diff", "--raw", "-z", "--no-renames", "--no-abbrev", base, head, "--").b
      fields = output.split("\0".b).map { |field| field.force_encoding(Encoding::UTF_8) }
      raise Error, "unexpected git diff --raw output" unless fields.size.even?

      fields.each_slice(2).map { |meta, path| change(meta, path) }
    end

    private

    def change(meta, path)
      old_mode, new_mode, old_blob, new_blob, status = meta.delete_prefix(":").split
      record = Change.new(status: status, path: path, old_mode: old_mode, new_mode: new_mode,
                          old_blob: old_blob, new_blob: new_blob)
      if status == "M" && CIScope.version_file?(path)
        record.old_content = run("cat-file", "blob", old_blob)
        record.new_content = run("cat-file", "blob", new_blob)
      end
      record
    end
  end

  def from_event(env, git, graph)
    event = env.fetch("CI_SCOPE_EVENT")
    range = case event
            when "pull_request" then pull_request_range(git)
            when "push" then push_range(env.fetch("CI_SCOPE_BEFORE", ""), env.fetch("CI_SCOPE_AFTER", ""), git)
            else return full("#{event} runs every job")
            end
    range.is_a?(Selection) ? range : select(git.changes(*range), graph)
  rescue Git::Error => e
    full(e.message)
  end

  def preview(base, head, git, graph)
    select(git.changes(git.resolve(base), git.resolve(head)), graph)
  rescue Git::Error => e
    full(e.message)
  end

  # The pull_request checkout is the merge of the head into the base.
  def pull_request_range(git)
    commits = git.parents("HEAD")
    return full("the pull request checkout is not a two-parent merge commit") unless commits.size == 3

    [commits[1], commits[0]]
  end

  def push_range(before, after, git)
    return full("push before #{before.inspect} is not a commit") if !before.match?(Git::SHA) || before.match?(/\A0+\z/)
    return full("push after #{after.inspect} is not a commit") unless after.match?(Git::SHA)

    begin
      git.fetch(before) unless git.commit?(before)
    rescue Git::Error => e
      return full("cannot fetch push before #{before}: #{e.message}")
    end
    return full("push before #{before} is not a commit after fetching") unless git.commit?(before)

    [before, after]
  end

  def text(selection)
    lines = ["CI scope: #{selection.mode} (#{selection.reason})", "jobs: #{selection.jobs.join(' ')}",
             "floors: #{selection.floors.join(' ')}"]
    lines + selection.files.map { |change, rule| "  #{change.status} #{display(change.path)} -> #{describe(rule)}" }
  end

  # Git allows control characters in paths, and a printed line that starts
  # with :: is a workflow command, so such paths, and invalid UTF-8, print
  # escaped and quoted.
  def display(path)
    path.valid_encoding? && !path.match?(/[[:cntrl:]]/) ? path : path.dump
  end

  # A Markdown code span whose fence is longer than any backtick run inside,
  # with table pipes escaped.
  def code_span(text)
    fence = "`" * (text.scan(/`+/).map(&:size).max.to_i + 1)
    pad = fence.size > 1 ? " " : ""
    "#{fence}#{pad}#{text.gsub('|', '\|')}#{pad}#{fence}"
  end

  def describe(rule)
    return "full: #{rule.full}" if rule.full

    selects = [*rule.jobs, *rule.packages.map { |package| "package #{package}" }]
    "#{rule.label}: #{selects.empty? ? 'static' : selects.join(', ')}"
  end

  SUMMARY_ROWS = 300

  def summary_rows(files)
    rows = files.first(SUMMARY_ROWS).map do |change, rule|
      "| #{change.status} | #{code_span(display(change.path))} | #{describe(rule).gsub('|', '\|')} |"
    end
    files.size > SUMMARY_ROWS ? [*rows, "| | #{files.size - SUMMARY_ROWS} more files | |"] : rows
  end

  def summary(selection)
    jobs = selection.jobs.map { |job| "`#{job}`" }.join(", ")
    <<~MARKDOWN
      ## CI scope

      Mode: **#{selection.mode}** (#{selection.reason})

      Jobs: #{jobs}

      Lowest-allowed leaves: #{selection.floors.empty? ? 'none' : selection.floors.join(', ')}

      | Status | File | Selects |
      | --- | --- | --- |
      #{summary_rows(selection.files).join("\n")}
    MARKDOWN
  end

  def write(selection, env, out)
    out.puts text(selection)
    append(env["GITHUB_OUTPUT"], "jobs=#{JSON.generate(selection.jobs)}", "floors=#{JSON.generate(selection.floors)}")
    append(env["GITHUB_STEP_SUMMARY"], summary(selection))
  end

  def append(file, *lines)
    File.open(file, "a") { |io| io.puts(*lines) } if file && !file.empty?
  end

  def main(argv = ARGV, env = ENV, root: ROOT, out: $stdout)
    graph = Graph.load(root)
    git = Git.new(root)
    selection = case argv.size
                when 0 then from_event(env, git, graph)
                when 2 then preview(*argv, git, graph)
                else abort "Usage: ruby scripts/ci_scope.rb [BASE HEAD]"
                end
    write(selection, env, out)
    selection
  end
end

CIScope.main if $PROGRAM_NAME == __FILE__
