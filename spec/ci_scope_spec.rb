# frozen_string_literal: true

require "bundler"
require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "stringio"
require "tmpdir"
require "yaml"
require_relative "../scripts/ci_scope"

RSpec::Matchers.define_negated_matcher :exclude, :include

RSpec.describe CIScope do
  let(:root) { File.expand_path("..", __dir__) }
  let(:graph) { CIScope::Graph.load(root) }

  def change(path, status: "M", old_mode: "100644", new_mode: status == "D" ? "000000" : "100644", **contents)
    old_mode = "000000" if status == "A"
    CIScope::Change.new(status: status, path: path, old_mode: old_mode, new_mode: new_mode, **contents)
  end

  def select(*changes) = described_class.select(changes, graph)

  def version_bump(path, from = "0.3.0", to = "0.3.1")
    template = "module ConnectRPC\n  VERSION = \"%s\" # x-release-please-version\nend\n"
    change(path, old_content: format(template, from), new_content: format(template, to))
  end

  let(:release_files) do
    [change("CHANGELOG.md"), change(".github/.release-please-manifest.json"),
     *ReleaseTrain::VERSION_FILES.values.map { |path| version_bump(path) }]
  end

  describe "path classes" do
    it "runs only Static for ordinary docs, adding Specs for installation guides" do
      expect(select(change("docs/streaming.md"), change("img/logo.svg"), change("SECURITY.md"),
                    change("gems/connectrpc-async/README.md", status: "A")))
        .to have_attributes(mode: "scoped", jobs: %w[static specs], floors: [])
      expect(select(change("docs/streaming.md"), change("docs/plans/hyper.md")).jobs).to eq(["static"])
      ReleaseTrain::GUIDES.each { |guide| expect(select(change(guide)).jobs).to eq(%w[static specs]), guide }
    end

    it "runs everything when a doc is deleted or renamed away, or is not a regular file" do
      expect(select(change("docs/streaming.md", status: "D")))
        .to have_attributes(mode: "full", reason: include("docs/streaming.md", "docs deletion"))
      renamed = [change("docs/old.md", status: "D"), change("docs/new.md", status: "A")]
      expect(described_class.select(renamed, graph).mode).to eq("full")
      expect(select(change("docs/link.md", status: "A", new_mode: "120000")).mode).to eq("full")
    end

    it "maps root specs, typecheck and browser paths to their fixed jobs" do
      expect(select(change("spec/grpc_spec.rb")).jobs).to eq(%w[static specs])
      expect(select(change("typecheck/surface.json")).jobs).to eq(%w[static specs rbs])
      expect(select(change("typecheck/cache_platform.rb")).jobs).to eq(%w[static specs rbs examples])
      expect(select(change("browser/reference/main.go")).jobs).to eq(%w[static specs browser_grpc_web])
    end

    it "runs everything for shared spec support, the archive check, core paths and unknown paths" do
      %w[spec/spec_helper.rb spec/support/socket_peer.rb spec/fixtures/test.binpb typecheck/archive.rb
         lib/connectrpc/client.rb sig/connectrpc.rbs exe/protoc-gen-connectrpc-ruby connectrpc.gemspec Gemfile
         mise.toml mise.ci.toml Rakefile scripts/ci_scope.rb .github/workflows/ci.yml conformance/cases.json
         interop/main.go rbs/collection.yaml third_party/README examples/README.rb .rubocop.yml LICENSE
         .gitignore release/Gemfile gems/connectrpc-unknown/lib/x.rb gems/README].each do |path|
        expect(select(change(path))).to have_attributes(mode: "full", jobs: described_class::JOBS.keys,
                                                        floors: described_class::FLOORS.keys), path
      end
    end

    it "runs everything under a gems directory that is not a package, including its README" do
      %w[gems/connectrpc-unknown/README.md gems/connectrpc/README.md gems/connectrpc/lib/connectrpc.rb].each do |path|
        expect(select(change(path))).to have_attributes(mode: "full", reason: include("is not a package directory")),
                                        path
      end
    end

    it "runs everything for the hyper platform list that ReleaseTrain loads" do
      expect(select(change("gems/connectrpc-hyper/scripts/platforms.rb")))
        .to have_attributes(mode: "full", reason: include("loaded by ReleaseTrain"))
      requires = File.read(File.join(root, "scripts/release_train.rb")).scan(%r{require_relative "\.\./(gems/[^"]+)"})
      expect(requires.flatten.map { |path| "#{path}.rb" }).to match_array(described_class::FULL_GEM_FILES)
    end

    it "runs everything for statuses it does not understand, symlinks and submodules" do
      expect(select(change("gems/connectrpc-rack/lib/x.rb",
                           status: "T")).reason).to include("unsupported diff status T")
      expect(select(change("gems/connectrpc-rack/lib/x.rb", status: "U")).mode).to eq("full")
      expect(select(change("gems/connectrpc-rack/link", status: "A", new_mode: "120000")).mode).to eq("full")
      expect(select(change("gems/connectrpc-rack/vendor", status: "A", new_mode: "160000")).mode).to eq("full")
    end

    it "runs everything when a symlink or submodule is deleted" do
      %w[120000 160000].each do |mode|
        expect(select(change("gems/connectrpc-rack/link", status: "D", old_mode: mode)))
          .to have_attributes(mode: "full", reason: include("symlink or submodule (mode #{mode})")), mode
      end
    end

    it "runs everything for an empty diff" do
      expect(described_class.select([], graph)).to have_attributes(mode: "full", reason: "the diff is empty")
    end

    it "runs everything when a gem change and a core change mix" do
      expect(select(change("gems/connectrpc-hyper/lib/connectrpc/hyper.rb"), change("lib/connectrpc.rb")).mode)
        .to eq("full")
    end
  end

  describe "gem selection" do
    it "runs the hyper jobs, its floors leaf and the generic gem jobs for a hyper-only change" do
      expect(select(change("gems/connectrpc-hyper/ext/connectrpc_hyper/src/lib.rs"),
                    change("gems/connectrpc-hyper/spec/write_spec.rb", status: "D")))
        .to have_attributes(mode: "scoped", floors: ["hyper"],
                            jobs: %w[static specs floors rbs hyper hyper_msrv hyper_gems hyper_windows])
    end

    it "pulls every package that path-depends on Async, with their floors, browser and examples" do
      expect(select(change("gems/connectrpc-async/lib/connectrpc/async/connection.rb")))
        .to have_attributes(jobs: %w[static specs package floors async_specs async_interop rbs falcon_specs
                                     falcon_interop otel prometheus health sentry browser_grpc_web examples],
                            floors: %w[async falcon otel prometheus health sentry])
    end

    it "selects the core jobs for a Rack change through the root Gemfile, with its dependents" do
      expect(select(change("gems/connectrpc-rack/lib/connectrpc/rack.rb")))
        .to have_attributes(jobs: %w[static specs package interop conformance rails floors rack rbs otel prometheus
                                     health sentry browser_grpc_web examples],
                            floors: %w[rack rails otel prometheus health sentry])
    end

    it "selects the browser and examples jobs through bundles that include the changed package" do
      expect(select(change("gems/connectrpc-testing/lib/connectrpc/testing.rb")).jobs)
        .to include("testing", "browser_grpc_web", "yabeda").and(exclude("examples", "rack"))
      expect(select(change("gems/connectrpc-on-rails/lib/connectrpc/on_rails.rb")).jobs)
        .to include("rails", "otel", "sentry", "browser_grpc_web", "examples").and(exclude("async_specs"))
    end
  end

  describe "release selection" do
    it "runs the release set when every file is release-owned and versions change only their literal" do
      expect(described_class.select(release_files, graph))
        .to have_attributes(mode: "scoped", jobs: %w[static package rails rbs hyper_gems], floors: [])
    end

    it "needs a version file in the diff for the release set" do
      expect(select(change("CHANGELOG.md"))).to have_attributes(mode: "scoped", jobs: ["static"])
      expect(select(change(".github/.release-please-manifest.json"), change("CHANGELOG.md")))
        .to have_attributes(mode: "full", reason: include(".github/.release-please-manifest.json"))
      expect(select(version_bump(ReleaseTrain::VERSION_FILES.fetch("connectrpc-yabeda")), change("CHANGELOG.md")).jobs)
        .to eq(described_class::RELEASE_JOBS)
    end

    it "classifies every file by the normal rules when any file is not release-owned" do
      selection = described_class.select([*release_files, change("docs/upgrading.md")], graph)
      expect(selection.mode).to eq("full")
      rules = selection.files.to_h { |entry, rule| [entry.path, rule] }
      expect(rules.fetch("lib/connectrpc/version.rb").full).to eq("outside the scoped paths")
      expect(rules.fetch(".github/.release-please-manifest.json").full).to eq("outside the scoped paths")
      expect(rules.fetch("CHANGELOG.md").jobs).to eq(["static"])
      expect(rules.fetch(ReleaseTrain::VERSION_FILES.fetch("connectrpc-hyper")).packages).to eq(["connectrpc-hyper"])
      expect(described_class.select([*release_files, change("lib/connectrpc.rb")], graph).mode).to eq("full")
    end

    it "requires each version file to change only its VERSION literal" do
      file = ReleaseTrain::VERSION_FILES.fetch("connectrpc-rack")
      edited = change(file, old_content: "VERSION = \"0.3.0\"\n", new_content: "VERSION = \"0.3.1\"\nX = 1\n")
      expect(select(edited)).to have_attributes(mode: "scoped", jobs: include("rack", "specs"))
      expect(select(change(file, old_content: "VERSION = \"0.3.0\"\n", new_content: "VERSION = 1\n")).jobs)
        .to include("rack")
      expect(select(change(file)).jobs).to include("rack")
      expect(select(version_bump(file, "0.3.0", "0.3.1")).jobs).to eq(described_class::RELEASE_JOBS)
      expect(select(change(file, old_mode: "100644", new_mode: "100755", **version_bump(file).to_h
        .slice(:old_content, :new_content))).jobs).to include("rack")
    end

    it "accepts only a numeric x.y.z VERSION literal, and never raises on unreadable contents" do
      file = ReleaseTrain::VERSION_FILES.fetch("connectrpc-rack")
      interpolated = change(file, old_content: "VERSION = \"0.3.0\"\n", new_content: "VERSION = \"0.3.\#{1}\"\n")
      expect(select(interpolated).jobs).to include("rack")
      invalid = change(file, old_content: "VERSION = \"0.3.0\"\n", new_content: "VERSION = \"0.3.1\"\n# \xFF\n")
      expect(select(invalid).jobs).to include("rack")
    end
  end

  describe CIScope::Graph do
    it "derives one node per package Gemfile and the root's only edge from its development dependency" do
      packages = Dir[File.join(root, "gems/*/")].map { |dir| File.basename(dir) }
      expect(graph.directories.keys).to match_array(["connectrpc", *packages])
      expect(graph.edges.fetch("connectrpc")).to eq(["connectrpc-rack"])
      expect(graph.edges.fetch("connectrpc-on-rails")).to eq(["connectrpc-rack"])
      expect(graph.edges.values.flatten).not_to include("connectrpc")
      expect(graph.affected("connectrpc-yabeda")).to eq(["connectrpc-yabeda"])
    end

    it "parses the same path sources Bundler evaluates from every Gemfile" do
      graph.directories.each_value do |directory|
        described_class.gemfiles(directory).each do |file|
          dsl = Bundler::Dsl.new
          dsl.eval_gemfile(file)
          bundler = dsl.dependencies.map(&:source).grep(Bundler::Source::Path)
                       .map { |source| source.expanded_original_path.to_s }.uniq - [directory]
          expect(described_class.path_sources(file) - [directory]).to match_array(bundler), file
        end
      end
    end

    it "refuses a path dependency outside the known packages" do
      Dir.mktmpdir("connectrpc-ci-scope-graph-") do |temporary|
        FileUtils.mkdir_p(File.join(temporary, "gems/connectrpc-demo"))
        File.write(File.join(temporary, "Gemfile"), "gemspec\n")
        File.write(File.join(temporary, "gems/connectrpc-demo/Gemfile"), "gem \"other\", path: \"../other\"\n")
        expect { described_class.load(temporary) }.to raise_error(RuntimeError, %r{gems/other is not a known package})
      end
    end

    it "keeps the root package when a gems/connectrpc directory has a Gemfile" do
      Dir.mktmpdir("connectrpc-ci-scope-graph-") do |temporary|
        %w[gems/connectrpc gems/connectrpc-demo].each { |dir| FileUtils.mkdir_p(File.join(temporary, dir)) }
        File.write(File.join(temporary, "Gemfile"), "gemspec\n")
        File.write(File.join(temporary, "gems/connectrpc/Gemfile"), "gemspec\n")
        File.write(File.join(temporary, "gems/connectrpc-demo/Gemfile"), "gem \"connectrpc\", path: \"../..\"\n")
        loaded = described_class.load(temporary)
        expect(loaded.directories).to eq("connectrpc" => temporary,
                                         "connectrpc-demo" => File.join(temporary, "gems/connectrpc-demo"))
        expect(CIScope.select([change("gems/connectrpc/Gemfile")], loaded).mode).to eq("full")
      end
    end

    it "reads Gemfiles with bytes that are not valid UTF-8" do
      Dir.mktmpdir("connectrpc-ci-scope-graph-") do |temporary|
        FileUtils.mkdir_p(File.join(temporary, "gems/connectrpc-demo"))
        File.write(File.join(temporary, "Gemfile"), "gemspec\n")
        File.binwrite(File.join(temporary, "gems/connectrpc-demo/Gemfile"),
                      "# \xFF\ngem \"connectrpc\", path: \"../..\"\n")
        expect(described_class.load(temporary).edges.fetch("connectrpc-demo")).to eq([])
      end
    end

    it "keeps Unicode word boundaries when a Gemfile is valid UTF-8" do
      Dir.mktmpdir("connectrpc-ci-scope-graph-") do |temporary|
        FileUtils.mkdir_p(File.join(temporary, "gems/connectrpc-demo"))
        File.write(File.join(temporary, "Gemfile"), "gemspec\n")
        File.write(File.join(temporary, "gems/connectrpc-demo/Gemfile"), "# 実際のpath: \"../unused\"\ngemspec\n")
        expect(described_class.load(temporary).edges.fetch("connectrpc-demo")).to eq([])
      end
    end

    it "keeps every cross-gem path reference in a gem's files inside its path dependency closure" do
      graph.directories.except("connectrpc").each do |name, directory|
        files, _error, status = Open3.capture3("git", "ls-files", "-z", chdir: directory)
        expect(status.success?).to be(true)
        references = files.split("\0").reject { |file| file.end_with?(".md") }.flat_map do |file|
          path = File.join(directory, file)
          next [] unless File.file?(path)

          File.binread(path).scan(%r{(?:\.\./|gems/)(connectrpc-[a-z-]+)}).flatten
              .select { |target| graph.node?(target) && target != name }.map { |target| [file, target] }
        end
        outside = references.reject { |_file, target| graph.closure(name).include?(target) }
        expect(outside).to be_empty, "#{name}: #{outside.inspect}"
      end
    end
  end

  describe "CI workflow inventory" do
    let(:jobs) { YAML.load_file(File.join(root, ".github/workflows/ci.yml")).fetch("jobs") }
    let(:gated) { jobs.except("scope", "ci_result") }

    it "maps every gated ci.yml job, in workflow order" do
      expect(described_class::JOBS.keys).to eq(gated.keys)
    end

    it "gates every job on the scope output" do
      gated.each do |id, job|
        expect(job.fetch("needs")).to eq(id == "hyper_windows" ? %w[scope hyper_gems] : "scope"), id
        expect(job.fetch("if")).to eq("contains(fromJSON(needs.scope.outputs.jobs), '#{id}')"), id
      end
      floors = jobs.fetch("floors")
      expect(floors.dig("strategy", "matrix", "leaf")).to eq("${{ fromJSON(needs.scope.outputs.floors) }}")
      expect(floors.fetch("name")).to eq("Lowest allowed ${{ matrix.leaf }} / Ruby 3.4.12")
    end

    it "computes the scope from the event range in a two-commit checkout" do
      scope = jobs.fetch("scope")
      expect(scope.fetch("name")).to eq("CI scope")
      expect(scope).not_to have_key("needs")
      expect(scope.fetch("outputs")).to eq("jobs" => "${{ steps.scope.outputs.jobs }}",
                                           "floors" => "${{ steps.scope.outputs.floors }}")
      checkout = scope.fetch("steps").first
      expect(checkout.fetch("with")).to eq("fetch-depth" => 2, "persist-credentials" => false)
      step = scope.fetch("steps").find { |entry| entry["id"] == "scope" }
      expect(step).to eq("name" => "Select affected jobs", "id" => "scope", "run" => "mise run ci:scope",
                         "env" => { "CI_SCOPE_EVENT" => "${{ github.event_name }}",
                                    "CI_SCOPE_BEFORE" => "${{ github.event.before }}",
                                    "CI_SCOPE_AFTER" => "${{ github.sha }}" })
    end

    it "decides CI result after every job with read access to the run's jobs" do
      result = jobs.fetch("ci_result")
      expect(result.fetch("name")).to eq("CI result")
      expect(result.fetch("needs")).to eq(["scope", *gated.keys])
      expect(result.fetch("if")).to eq("always()")
      expect(result.fetch("permissions")).to eq("actions" => "read", "contents" => "read")
      step = result.fetch("steps").find { |entry| entry["run"] == "mise run ci:result" }
      expect(step.fetch("env")).to eq("NEEDS" => "${{ toJSON(needs) }}", "GH_TOKEN" => "${{ github.token }}")
    end

    it "follows the job conventions in both control jobs" do
      %w[scope ci_result].each do |id|
        steps = jobs.fetch(id).fetch("steps")
        actions = steps.filter_map { |step| step["uses"] }
        examples = jobs.fetch("examples").fetch("steps").filter_map { |step| step["uses"] }
        expect(actions).to eq(examples.reject { |uses| uses.start_with?("actions/cache@") })
        ruby = steps.find { |step| step.fetch("uses", "").start_with?("ruby/setup-ruby@") }
        expect(ruby.fetch("with")).to include("ruby-version" => "3.4.12", "bundler" => "4.0.23",
                                              "bundler-cache" => false)
        mise = steps.find { |step| step.fetch("uses", "").start_with?("jdx/mise-action@") }
        expect(mise.fetch("with")).to include("install" => false, "cache" => false, "add_shims_to_path" => false,
                                              "env" => false, "export_path" => false)
        expect(steps.first.dig("with", "persist-credentials")).to be(false)
        expect(steps.last).to include("if" => "always()")
        expect(steps.last.fetch("run")).to include("git status --porcelain --untracked-files=all")
      end
    end

    it "lists each package a job's bundle selects among the packages that job consumes" do
      gated.each do |id, job|
        steps = job.fetch("steps", []).map { |step| step.dig("env", "BUNDLE_GEMFILE") }
        gemfiles = [job.dig("env", "BUNDLE_GEMFILE"), *steps]
        gemfiles.compact.map { |gemfile| gemfile[%r{/gems/([^/]+)/}, 1] }.uniq.each do |package|
          consumed = described_class::JOBS.fetch(id)
          expect(consumed == CIScope::ANY || consumed.include?(package)).to be(true), "#{id}: #{package}"
        end
      end
    end

    it "lists each package a job's Mise tasks reach among the packages that job consumes" do
      output, error, status = Open3.capture3({ "MISE_ENV" => "ci" }, "mise", "tasks", "--json")
      expect(status.success?).to be(true), error
      tasks = JSON.parse(output).to_h { |task| [task.fetch("name"), task] }
      invoked = ->(text) { text.scan(/mise run "?([^\s";&|]+)/).flatten }
      closure = lambda do |name, seen = []|
        next seen if seen.include?(name)

        task = tasks.fetch(name)
        depends = task.fetch("depends").map { |dependency| dependency.split.first }
        nested = [*depends, *invoked.call(task.fetch("run").join("\n"))]
        nested.reduce(seen + [name]) { |visited, dependency| closure.call(dependency, visited) }
      end
      # Each floors leg runs its own leaf's task and consumes only that leaf's package.
      legs = gated.flat_map do |id, job|
        commands = invoked.call(job.fetch("steps", []).filter_map { |step| step["run"] }.join("\n")).uniq
        next [[id, commands, described_class::JOBS.fetch(id)]] unless id == "floors"

        described_class::FLOORS.map do |leaf, package|
          ["floors #{leaf}", commands.map { |command| command.sub("$FLOORS_LEAF", leaf) }, [package]]
        end
      end
      packages = lambda do |task|
        text = [*task.fetch("run"), task.fetch("dir").to_s, *task.fetch("env")].join("\n")
        text.scan(%r{gems/(connectrpc-[a-z-]+)}).flatten.uniq
      end
      references = legs.flat_map do |leg, commands, consumed|
        reach = consumed == CIScope::ANY ? nil : consumed.flat_map { |package| [package, *graph.closure(package)] }
        commands.flat_map { |command| closure.call(command) }.uniq.flat_map do |name|
          packages.call(tasks.fetch(name)).map { |package| [leg, name, package, reach.nil? || reach.include?(package)] }
        end
      end
      expect(references.map { |leg, _name, package, _ok| [leg, package] })
        .to include(%w[browser_grpc_web connectrpc-falcon], %w[examples connectrpc-async],
                    ["floors hyper", "connectrpc-hyper"])
      expect(references.reject(&:last)).to be_empty
    end

    it "gives every package at least one job of its own beyond Static, Specs and RBS" do
      exempt = []
      generic = %w[static specs rbs]
      owned = described_class::JOBS.except(*generic).values.reject { |packages| packages == CIScope::ANY }.flatten
      expect(graph.directories.keys - ["connectrpc"] - exempt - owned).to be_empty
      expect(described_class::FLOORS.values - graph.directories.keys).to be_empty
    end
  end

  describe CIScope::Git do
    let(:clean_env) do
      { "GIT_CONFIG_GLOBAL" => File::NULL, "GIT_CONFIG_NOSYSTEM" => "1", "GIT_AUTHOR_NAME" => "CI scope",
        "GIT_AUTHOR_EMAIL" => "scope@example.invalid", "GIT_COMMITTER_NAME" => "CI scope",
        "GIT_COMMITTER_EMAIL" => "scope@example.invalid" }
    end

    def sh(directory, *args)
      output, error, status = Open3.capture3(clean_env, "git", *args, chdir: directory)
      raise "git #{args.join(' ')}: #{error}" unless status.success?

      output.strip
    end

    def commit(directory, files, message)
      files.each do |path, content|
        target = File.join(directory, path)
        if content.nil?
          sh(directory, "rm", "-q", path)
        else
          FileUtils.mkdir_p(File.dirname(target))
          File.write(target, content)
          sh(directory, "add", path)
        end
      end
      sh(directory, "commit", "-q", "--allow-empty", "-m", message)
      sh(directory, "rev-parse", "HEAD")
    end

    def repository(directory)
      sh(directory, "init", "-q", "-b", "main")
      commit(directory, { "docs/guide.md" => "guide\n", "lib/connectrpc/version.rb" => version(1) }, "base")
    end

    def version(patch) = "module ConnectRPC\n  VERSION = \"0.3.#{patch}\"\nend\n"

    def scope(directory, env)
      described_class.new(directory).then { |git| CIScope.from_event(env, git, graph) }
    end

    around do |example|
      Dir.mktmpdir("connectrpc-ci-scope-git-") do |directory|
        @directory = directory
        example.run
      end
    end

    let(:directory) { @directory }

    it "diffs a pull request merge commit against its base parent only" do
      repository(directory)
      sh(directory, "switch", "-q", "-c", "topic")
      commit(directory, { "gems/connectrpc-hyper/src/lib.rs" => "fn main() {}\n" }, "topic")
      sh(directory, "switch", "-q", "main")
      commit(directory, { "lib/connectrpc.rb" => "# base moved\n" }, "base moved")
      sh(directory, "merge", "-q", "--no-ff", "-m", "merge", "topic")
      expect(sh(directory, "rev-list", "--parents", "-n", "1", "HEAD").split.size).to eq(3)
      expect(scope(directory, "CI_SCOPE_EVENT" => "pull_request"))
        .to have_attributes(mode: "scoped", jobs: include("hyper", "hyper_gems"), floors: ["hyper"])
    end

    it "runs everything for a pull request checkout that is not a merge commit" do
      repository(directory)
      commit(directory, { "docs/other.md" => "other\n" }, "docs")
      expect(scope(directory, "CI_SCOPE_EVENT" => "pull_request"))
        .to have_attributes(mode: "full", reason: include("not a two-parent merge commit"))
    end

    it "fetches a push's missing before commit, then diffs the pushed range" do
      origin = File.join(directory, "origin")
      FileUtils.mkdir_p(origin)
      before = repository(origin)
      commit(origin, { "docs/guide.md" => "changed\n" }, "docs")
      after = commit(origin, { "docs/new.md" => "new\n" }, "more docs")
      checkout = File.join(directory, "checkout")
      sh(directory, "clone", "-q", "--depth=1", "file://#{origin}", checkout)
      git = described_class.new(checkout)
      expect(git.commit?(before)).to be(false)
      selection = CIScope.from_event({ "CI_SCOPE_EVENT" => "push", "CI_SCOPE_BEFORE" => before,
                                       "CI_SCOPE_AFTER" => after }, git, graph)
      expect(selection).to have_attributes(mode: "scoped", jobs: ["static"])
      expect(selection.files.map { |entry, _rule| entry.path }).to eq(%w[docs/guide.md docs/new.md])
      expect(git.commit?(before)).to be(true)
    end

    it "runs everything for a zero, malformed or unfetchable push before" do
      after = repository(directory)
      sh(directory, "remote", "add", "origin", File.join(directory, "missing"))
      { "0" * 40 => "is not a commit", "main" => "is not a commit", "" => "is not a commit",
        "1" * 40 => "cannot fetch push before" }.each do |before, reason|
        expect(scope(directory, "CI_SCOPE_EVENT" => "push", "CI_SCOPE_BEFORE" => before, "CI_SCOPE_AFTER" => after))
          .to have_attributes(mode: "full", reason: include(reason)), before
      end
    end

    it "runs everything for schedule, dispatch and an empty push range" do
      head = repository(directory)
      %w[schedule workflow_dispatch].each do |event|
        expect(scope(directory, "CI_SCOPE_EVENT" => event)).to have_attributes(mode: "full", reason: include(event))
      end
      expect(scope(directory, "CI_SCOPE_EVENT" => "push", "CI_SCOPE_BEFORE" => head, "CI_SCOPE_AFTER" => head))
        .to have_attributes(mode: "full", reason: "the diff is empty")
    end

    it "reads a rename as a deletion and an addition, and a symlink with its mode" do
      base = repository(directory)
      sh(directory, "mv", "docs/guide.md", "docs/moved.md")
      FileUtils.mkdir_p(File.join(directory, "gems/connectrpc-rack"))
      File.symlink("../../docs/moved.md", File.join(directory, "gems/connectrpc-rack/link"))
      sh(directory, "add", "-A")
      head = commit(directory, {}, "rename")
      changes = described_class.new(directory).changes(base, head)
      expect(changes.map { |entry| [entry.status, entry.path, entry.new_mode] })
        .to contain_exactly(["D", "docs/guide.md", "000000"], ["A", "docs/moved.md", "100644"],
                            ["A", "gems/connectrpc-rack/link", "120000"])
      expect(CIScope.select(changes, graph).mode).to eq("full")
    end

    it "loads version file blobs so a literal-only bump selects the release set" do
      base = repository(directory)
      bump = commit(directory, { "lib/connectrpc/version.rb" => version(2) }, "bump")
      git = described_class.new(directory)
      expect(git.changes(base, bump).first).to have_attributes(old_content: version(1), new_content: version(2))
      expect(CIScope.preview(base, bump, git, graph).jobs).to eq(CIScope::RELEASE_JOBS)
      edit = commit(directory, { "lib/connectrpc/version.rb" => "#{version(3)}# more\n" }, "edit")
      expect(CIScope.preview(bump, edit, git, graph).mode).to eq("full")
    end

    it "runs everything for a path that is not valid UTF-8" do
      base = repository(directory)
      FileUtils.mkdir_p(File.join(directory, "gems/connectrpc-rack"))
      File.write(File.join(directory, "gems/connectrpc-rack/caf\xE9.rb".b), "x\n")
      sh(directory, "add", "-A")
      head = commit(directory, {}, "Latin-1 name")
      selection = CIScope.preview(base, head, described_class.new(directory), graph)
      expect(selection).to have_attributes(mode: "full", reason: include("path is not valid UTF-8"))
      expect(selection.reason).to include('"gems/connectrpc-rack/caf\\xE9.rb"')
    end

    it "runs everything when git cannot read the range" do
      repository(directory)
      expect(CIScope.preview("HEAD", "no-such-revision", described_class.new(directory), graph))
        .to have_attributes(mode: "full", reason: include("git rev-parse failed"))
    end
  end

  describe "command line" do
    it "writes the selection to the step outputs and summary" do
      Dir.mktmpdir("connectrpc-ci-scope-cli-") do |directory|
        output, summary = %w[output summary].map { |name| File.join(directory, name) }
        selection = CIScope::Selection.new(mode: "scoped", reason: "affected packages: connectrpc-hyper",
                                           jobs: %w[static hyper], floors: ["hyper"],
                                           files: [[CIScope::Change.new(status: "M", path: "gems/a|b"),
                                                    CIScope::Rule.new(label: "gem", jobs: [], packages: ["x"])]])
        printed = StringIO.new
        described_class.write(selection, { "GITHUB_OUTPUT" => output, "GITHUB_STEP_SUMMARY" => summary }, printed)
        expect(File.read(output)).to eq("jobs=[\"static\",\"hyper\"]\nfloors=[\"hyper\"]\n")
        expect(File.read(summary)).to include("Mode: **scoped**", "`static`, `hyper`", "| M | `gems/a\\|b` |")
        expect(printed.string).to include("CI scope: scoped", "M gems/a|b -> gem: package x")
      end
    end

    it "escapes control characters and backticks in printed and summarized paths" do
      injected = "\n::set-output name=jobs::[]"
      selection = described_class.select([change("gems/connectrpc-rack/x#{injected}`y`.rb"), change("docs/a`b|c.md"),
                                          change("x#{injected}")], graph)
      Dir.mktmpdir("connectrpc-ci-scope-cli-") do |directory|
        summary = File.join(directory, "summary")
        printed = StringIO.new
        described_class.write(selection, { "GITHUB_STEP_SUMMARY" => summary }, printed)
        lines = printed.string.lines + File.read(summary).lines
        expect(lines.grep(/\A::/)).to be_empty
        expect(printed.string).to include('CI scope: full ("x\\n::set-output name=jobs::[]": outside the scoped paths)',
                                          '  M "gems/connectrpc-rack/x\\n::set-output name=jobs::[]`y`.rb" -> gem')
        rows = File.read(summary).lines.grep(/\A\| M \|/)
        expect(rows.size).to eq(3)
        expect(rows.first)
          .to start_with('| M | `` "gems/connectrpc-rack/x\\n::set-output name=jobs::[]`y`.rb" `` | gem')
        expect(rows.drop(1)).to eq(["| M | `` docs/a`b\\|c.md `` | docs: static |\n",
                                    "| M | `\"x\\n::set-output name=jobs::[]\"` | full: outside the scoped paths |\n"])
      end
    end

    it "stops workflow command processing around the printed selection inside Actions" do
      selection = described_class.select([change("gems/connectrpc-rack/##[set-output name=jobs;][].rb")], graph)
      printed = StringIO.new
      described_class.write(selection, { "GITHUB_ACTIONS" => "true" }, printed)
      lines = printed.string.lines(chomp: true)
      token = lines.first[/\A::stop-commands::(\h{32})\z/, 1]
      expect(token).not_to be_nil
      expect(lines.last).to eq("::#{token}::")
      expect(lines[1..-2].join("\n")).to include("##[set-output name=jobs;][].rb").and(exclude(token))
      plain = StringIO.new
      described_class.write(selection, {}, plain)
      expect(plain.string).to start_with("CI scope: scoped")
    end

    it "previews a range from the real checkout and fails without an event" do
      script = File.join(root, "scripts/ci_scope.rb")
      # A hosted Specs job sets these; the child must not write to that job.
      runner = { "GITHUB_ACTIONS" => nil, "GITHUB_OUTPUT" => nil, "GITHUB_STEP_SUMMARY" => nil }
      Dir.mktmpdir("connectrpc-ci-scope-cli-") do |directory|
        output = File.join(directory, "output")
        printed, error, status = Open3.capture3(runner.merge("GITHUB_OUTPUT" => output), RbConfig.ruby, script,
                                                "HEAD", "HEAD")
        expect(status.success?).to be(true), error
        expect(printed).to start_with("CI scope: full (the diff is empty)")
        expect(JSON.parse(File.read(output)[/^jobs=(.*)$/, 1])).to eq(described_class::JOBS.keys)
      end
      _printed, error, status = Open3.capture3(runner.merge("CI_SCOPE_EVENT" => nil), RbConfig.ruby, script)
      expect(status.success?).to be(false)
      expect(error).to include("CI_SCOPE_EVENT")
    end
  end
end
