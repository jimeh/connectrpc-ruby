# Development

## Setup

Use [Mise](https://mise.jdx.dev/) >= 2026.9.14. CI pins 2026.10.19.
Install the [Ruby source-build prerequisites](https://github.com/rbenv/ruby-build/wiki#suggested-build-environment)
for your OS first; setup does not change system packages or global tool config.
Network access is required on first setup.

```sh
mise run setup
mise exec -- ruby --version
mise exec -- bundle --version
```

`mise.toml` pins CRuby 3.4.12 and 4.0.9, actionlint, and zizmor. Both Rubies
compile from upstream source. Local setup installs both, with 3.4.12 first on
PATH; CI selects one version per job with `MISE_RUBY_VERSION`. Run a targeted
check on the other version with:

```sh
MISE_RUBY_VERSION=4.0.9 mise run test
```

The source-build installer is pinned to [ruby-build v20261023](https://github.com/rbenv/ruby-build/releases/tag/v20261023)
by commit in `ruby.ruby_build_repo`. Review its release age and commit alongside
Ruby updates. Compilation uses the host's compiler and system libraries, so
these locks do not promise bit-identical Ruby binaries across machines.

Setup installs only the declared project tools, then Bundler 4.0.23 into the
active Ruby, then the frozen `Gemfile.lock` into ignored `vendor/bundle/`.
Re-running setup is safe. Bundler config files are ignored so user-level
settings cannot silently change the bundle location or frozen behavior.
`mise.lock` currently locks Linux x64 downloads and checksums, including both
Ruby source archives. Add and review locks for other development platforms
with `mise lock --platform <platform>` before installing there.

## Commands and evidence

`mise tasks` is the command index. Ruby operations use Rake where useful;
Mise owns setup ordering and verification composition.

| Command | Purpose |
| --- | --- |
| `mise run setup` | Install locked tools, pinned Bundler, frozen dependencies. |
| `mise run format` | Apply RuboCop Layout fixes. |
| `mise run format:check` | Check Layout without writing. |
| `mise run lint` | Run remaining RuboCop cops. |
| `mise run syntax` | Parse maintained library, specs, scripts, gemspec, Gemfile, Rakefile. |
| `mise run test -- spec/connectrpc_spec.rb` | Run focused RSpec examples; accepts `file:line` and RSpec options. |
| `mise run test` | Run all specs and generate coverage. |
| `mise run lint:workflows` | Run actionlint and offline zizmor. |
| `mise run check` | Run format, lint, syntax, and workflow checks. |
| `mise run build` | Build `pkg/connectrpc-0.1.0.gem`. |
| `mise run check:package` | Build, install, and require the gem in isolation. |
| `mise run verify` | Run check, test, build, and isolated package verification. |

Every entry point installs missing prerequisites through the same task graph.
During Ruby iteration, use focused tests and relevant static checks. Run
`check` for fast static feedback and `verify` for package/tooling changes and
final implementation handoff. CI runs the same setup and verify commands on
both pinned Rubies. Hosted clean-environment and matrix results remain CI
evidence; a local pass cannot establish them.

Markdown is maintained manually: check relative links, commands, and fenced
examples when changing prose. There is no automatic Markdown formatter or
Node toolchain. Mise parses TOML, while actionlint and zizmor check workflows.
Docs-only edits do not require a test rerun unless an example changes behavior;
packaged-doc changes can be checked with `check:package`.

### Coverage

SimpleCov 1.3.2 starts in `spec/spec_helper.rb` before loading the library.
All `lib/**/*.rb` files, including unloaded files, are in scope; specs and
tooling are outside that scope. The test command prints line and branch
summaries and writes an ignored local HTML report to `coverage/index.html`.
Reports describe the current run, without merging prior focused runs. There is
no coverage threshold, upload, external service, or artifact job. A skeleton
with no branches may report zero total branches; its high line percentage
proves no protocol behavior. The version example and isolated package require
establish bootstrap behavior only, not interoperability or production readiness.

### Package isolation

`check:package` asserts the exact intended package files, no runtime
dependencies, and the private push guard. It runs `gem install --norc --local`
and a require/version assertion from a separate temporary working directory,
inside `Bundler.with_unbundled_env`, with explicit fresh `GEM_HOME`/`GEM_PATH`
and cleared Ruby/Bundler options. Temporary files are removed automatically.
The check neither publishes nor contacts a gem registry.

The gemspec's file selection is independent of Git: only `lib/**/*.rb`,
`README.md`, and these two development/design documents ship. Specs, workflow
files, tool configuration, coverage, and installed development gems do not.

## Dependency and action updates

Keep exact direct dependency and tool pins. Check release dates against the
[primary RubyGems API](https://guides.rubygems.org/rubygems-org-api/) or upstream
GitHub releases before updating: stable releases must be at least seven days
old. Verify Ruby requirements for both supported Rubies. For actions, resolve
the upstream release tag to its commit and retain a full SHA with a tag comment.
Mise's minimum age filters fuzzy resolution, not explicit pins; review pinned
release ages manually. Bundler's source cooldown is seven days; old registries
or index entries lacking timestamps may not enforce it, so audit newly resolved
versions too. See [Bundler cooldown](https://bundler.io/man/bundle-config.1.html).

For a deliberate dependency update, edit the Gemfile, then resolve the lock
with frozen mode temporarily disabled. Keep the generic Ruby platform and
checksums, and do not add a `ruby` Gemfile directive or `RUBY VERSION` field
that excludes the other CI Ruby:

```sh
mise exec -- env BUNDLE_FROZEN=false bundle lock --update --add-platform ruby --add-checksums
mise lock --platform linux-x64
mise run setup
mise run verify
```

Review all lock changes, including transitive dependencies. CI uses frozen
installation. For an urgent security fix inside the seven-day window, obtain
an explicit maintainer exception, record the advisory/version/reason in the PR,
and override cooldown only for that resolution (for example,
`mise exec -- env BUNDLE_COOLDOWN=0 BUNDLE_FROZEN=false bundle lock --update <gem>`).
Do not weaken the persistent policy. Licensing remains undecided; the invalid
push host is an accidental-push guard, not permission to distribute or a
security boundary against someone editing metadata.

## Hooks and pull requests

Local hooks: deferred (`n/a`). This repository has only a tiny library and
canonical checks; automatic hooks add installation and partial-staging
semantics before there is evidence of recurring missed checks. Run `check`
while working and rely on CI for required verification. Reconsider a small,
non-writing pre-commit check if contributors repeatedly miss checks, after
measuring warm runtime and validating worktree/partial-staging behavior. No
pre-push hook is installed.

Keep PRs scoped. Describe the concrete result, relevant design boundaries,
commands and results (including RSpec example counts), and remaining limits.
Reuse valid evidence. Design changes should update [design.md](design.md).
No release workflow, publication task, external bot, or secret is required.
Publishing needs a separate licensing/publication decision and authorization.
