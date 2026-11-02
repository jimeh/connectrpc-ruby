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
PATH. CI uses official prebuilt runtimes instead; see [CI](ci.md). Run a
targeted local check on the other version with:

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
| `mise run check:static` | Run format, lint, and workflow checks once. |
| `mise run check` | Run static checks and Ruby syntax checks. |
| `mise run build` | Build `pkg/connectrpc-0.1.0.gem`. |
| `mise run check:package` | Build, install, and require the gem in isolation. |
| `mise run example` | Run a typed ephemeral localhost Rack/NetHTTP example in both codecs. |
| `mise run fixture:build` | Rebuild the shared descriptor fixture from its explicit Ruby schema builder. |
| `mise run fixture:check` | Compare the checked-in descriptor bytes to the builder. |
| `mise run setup:interop` | Install task-local Go 1.27.3 and download checksum-locked modules. |
| `mise run test:interop` | Run the Go-to-Go reference baseline and both cross-language directions/codecs, 18 cases total. |
| `mise run setup:conformance` | Install task-local official runner 1.0.6 and protoc 36.2 with locked checksums. |
| `mise run conformance:build` | Generate descriptors from unchanged official schemas using protoc. |
| `mise run conformance:check` | Verify schema hashes and byte-for-byte descriptor regeneration. |
| `mise run test:conformance` | Run all 94 client and 88 server profile cases with strict logs/telemetry accounting. |
| `mise run verify` | Run check, test, installed package integration, fixture check, Go interop and official conformance. |

Every entry point installs missing prerequisites through the same task graph.
During Ruby iteration, use focused tests and relevant static checks. Run
`check` for fast static feedback and `verify` for package/tooling changes and
final implementation handoff. [CI](ci.md) splits the same canonical component
tasks into nine independent checks, with syntax, specs, package isolation,
interop and official conformance on both Rubies. Hosted clean-environment and
matrix results remain CI evidence; a local pass cannot establish them.

Markdown is maintained manually: check relative links, commands, and fenced
examples when changing prose. There is no automatic Markdown formatter or
Node toolchain. Mise parses TOML, while actionlint and zizmor check workflows.
Docs-only edits do not require a test rerun unless an example changes behavior;
packaged-doc changes can be checked with `check:package`.

### Official conformance

[The conformance harness](../conformance/README.md) documents exact pins,
provenance, supported axes, lifecycle, named selectors and result artifacts.
`test:conformance` accepts an optional `client` or `server` direction and then
one exact case name from `conformance/cases.json` for troubleshooting. CI and
`verify` run every configured case in both directions on both pinned Rubies.
The 18-case independent Go fixture stays unchanged.

The profile has 92 client passes and two expected caller-cancellation
failures, plus 88 server passes. Both unsupported caller instructions still
invoke real uncanceled RPCs and return faithful results. Only the two exact
caller-cancellation cases are marked known-failing, and every assertion and
actual RPC result must match the documented limitation. Unexpected passes or
unrelated failures still fail verification.
Timeouts, duplicate metadata and remote canceled errors must pass normally.
Raw runner logs, JSON telemetry and strict JSON summaries are preserved under
ignored `vendor/conformance/results/`. An accounting mismatch fails even if
the official runner exits successfully. This evidence is distinct from full
conformance or unsupported configuration-space totals.

### Coverage

SimpleCov 1.3.2 starts in `spec/spec_helper.rb` before loading the library.
All `lib/**/*.rb` files, including unloaded files, are in scope; specs and
tooling are outside that scope. The test command prints line and branch
summaries and writes an ignored local HTML report to `coverage/index.html`.
Reports describe the current run, without merging prior focused runs. There is
no coverage threshold, upload, external service, or artifact job. A skeleton
with no branches may report zero total branches; its high line percentage
proves no protocol behavior. Coverage is regression evidence, not interoperability or production-readiness
proof. The separate Go peer tests establish the advertised unary subset.

### Package isolation

`check:package` asserts an explicit reviewed package manifest, zero runtime
dependencies, and the private push guard. It runs `gem install --norc --local`
from a separate temporary working directory inside `Bundler.with_unbundled_env`,
with fresh `GEM_HOME`/`GEM_PATH` and cleared Ruby/Bundler options. It verifies
core require without Google/Rack and useful missing-optional-require errors.
It then installs the cached, locked optional artifacts, including native
`bigdecimal`, Google protobuf and its `rake` dependency, Rack, rackup and
WEBrick. The installed gem performs real typed Rack/NetHTTP calls with both
binary and JSON scalar-wrapper payloads. The helper scripts are copied to the
external cwd; library code loads exclusively from the fresh installed gem.
The check does not install the development JSON gem: it exercises the Ruby
default JSON and Net::HTTP gems. All installs are offline and temporary files
are removed. Nothing is published.

Only `lib/**/*.rb`, `README.md`, and the two design/development documents ship.
The exact manifest in `scripts/check_package.rb` must be deliberately updated
when library files are added. Specs, fixtures, examples, peer code, tooling,
coverage and installed development dependencies do not ship.

### Independent interoperability and fixture provenance

`test:interop` builds a reference peer with Go 1.27.3,
`connectrpc.com/connect` v1.21.1 and `google.golang.org/protobuf` v1.36.12.
Go is task-local, with its Linux x64 download checksum in `mise.lock`;
ordinary setup/focused Ruby checks do not need it. `GOTOOLCHAIN=local`,
`GOWORK=off`, `GOFLAGS=-mod=readonly` and checked-in `go.sum` prevent implicit
toolchain/module updates. Go's module/build caches are project-local under
ignored `vendor/interop/`. Setup downloads the pinned modules; tests run
`go mod verify` and a readonly build.

The 18 fixture cases are success, deliberate error/details/metadata, and a
request limit, for both codecs across Go-to-Go, Ruby-to-Go and Go-to-Ruby.
They use the actual connect-go Client and Handler with dynamic message
initialization. The Go-to-Go baseline runs while the existing Go host is alive,
using the same client scenarios and configured limits as Go-to-Ruby. Twelve
cases establish cross-language interoperability; six validate the reference
baseline. Success compares typed messages semantically, covering optional
presence, int64, enum, timestamp and bytes. The Go baseline asserts separate raw
request/response metadata, trailers and error metadata. Go-to-Ruby keeps exact
WEBrick coalescing expectations for those known fixture fields. Raw repeated Go
response values remain separate at NetHTTP. NetHTTP joins outgoing request
arrays with comma+space, and the WEBrick host also coalesces incoming and
response values. Assertions preserve those observed semantics without splitting
arbitrary comma-containing values. This is not the Connect conformance suite.

`spec/fixtures/echo.proto` documents the schema. `scripts/build_fixture.rb`
explicitly builds a matching `FileDescriptorProto`; it is not protoc output or
a protobuf parser. The checked-in `echo.bin` is the single descriptor used by
both peers. `fixture:check` compares it byte-for-byte with a rebuilt descriptor,
Ruby mapping specs exercise its types, and Go reflection checks presence,
int64, enum and timestamp fields. If changing the schema, update both the
`.proto` and builder and regenerate the bytes with `fixture:build`.

Localhost tests/examples use port 0 and bounded readiness/shutdown signals.
Socket lifecycle tests establish request progress, then exercise stalled body,
aggregate response-header trickle and blocked outbound writes. Header-trickle
pacing measures real elapsed network behavior, not readiness. The Ruby 3.4
stdlib may lazily retain its process-wide Timeout service thread; tests exclude
that named service from call-owned worker checks. ConnectRPC watchdogs and
peer/host workers must all stop, and sockets must close.
Pre-dispatch race tests use a controlled monotonic clock and peer-observed EOF
before resuming dispatch, then check for queued replacement connections after
the call completes. Wire timeout assertions consume budget during preparation
and connection. Huge finite budgets exercise real localhost I/O, with capped
native waits and an unchanged logical deadline. Scaled-cap socket tests verify
`unavailable` before logical expiry and `deadline_exceeded` after actual expiry,
with closure and no replacement connection. Huge-budget tests exercise native
conversions, not the full elapsed native-operation ceiling. Proxy configuration assertions
set hostile proxy environment variables and restore their previous state.

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
Approved merges use squash merging; merge authorization remains separate.
Reuse valid evidence. Design changes should update [design.md](design.md).
No release workflow, publication task, external bot, or secret is required.
Publishing needs a separate licensing/publication decision and authorization.
