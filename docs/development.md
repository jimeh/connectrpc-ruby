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
PATH. CI uses official prebuilt runtimes instead; see
[CI](https://github.com/jimeh/connectrpc-ruby/blob/main/docs/ci.md). Run a
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
| `mise run setup:generation` | Install task-local protoc 36.2 and development-only Buf 1.75.0. |
| `mise run generation:build` | Regenerate checked-in official messages and service glue using protoc; verify actual Buf glue parity/runtime. |
| `mise run generation:check` | Regenerate into temp trees, compare fixture bytes/glue parity, exercise both protoc and Buf output. |
| `mise run example:generated` | Compile the greeting proto and run its generated localhost client/handlers in both codecs. |
| `mise run example` | Run a typed ephemeral localhost Rack/NetHTTP example in both codecs. |
| `mise run fixture:build` | Rebuild the shared descriptor fixture from its explicit Ruby schema builder. |
| `mise run fixture:check` | Compare the checked-in descriptor bytes to the builder. |
| `mise run setup:interop` | Install task-local Go 1.27.3 and download checksum-locked modules. |
| `mise run test:interop` | Run the Go-to-Go reference baseline and both cross-language directions/codecs, 18 original identity cases, the generated path, then 40 separate gzip cases. |
| `mise run setup:conformance` | Install task-local official runner 1.0.6 and protoc 36.2 with locked checksums. |
| `mise run conformance:build` | Generate descriptors from unchanged official schemas using protoc. |
| `mise run conformance:check` | Verify schema hashes and byte-for-byte descriptor regeneration. |
| `mise run test:conformance` | Run identity (94/88) and gzip (88/82) client/server profiles with strict accounting. |
| `mise run verify` | Run core checks, specs, packages, generation, Go interop, official conformance, the selected Rails lane and the HTTP/2 backend. |

Every entry point installs missing prerequisites through the same task graph.
During Ruby iteration, use focused tests and relevant static checks. Run
`check` for fast static feedback and `verify` for package/tooling changes and
final implementation handoff.
[CI](https://github.com/jimeh/connectrpc-ruby/blob/main/docs/ci.md) splits the same
canonical component tasks into nine core, three Rails and four optional HTTP/2 checks, with syntax, specs, package isolation,
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
one exact case name from `conformance/cases.json` for troubleshooting. CI runs
every configured case in both directions on both pinned Rubies; `verify` runs
both directions on the active Ruby.
The 18-case independent Go identity fixture and generated baseline stay separate.
Focused tasks are `test:interop:identity`, `test:interop:gzip`,
`test:conformance:identity` and `test:conformance:gzip`. Exact conformance selectors
automatically choose their inventory; explicit profile tasks keep runs bounded.

The profile has 92 client passes and two expected caller-cancellation
failures, plus 88 server passes. Both unsupported caller instructions still
invoke real uncanceled RPCs and return faithful results. Only the two exact
caller-cancellation cases are marked known-failing, and every assertion and
actual RPC result must match the documented limitation. Unexpected passes or
unrelated failures still fail verification.
The additional gzip profile has 86 client passes and two executed expected
caller-cancellation failures, plus 82 server passes. Its separate exact-name
manifest was approved after live causal verification; the same strict checks
reject an unexpected pass or unrelated failure. Neither discovery counts nor
unsupported instructions are passes. Actual wire coding, byte counts,
lengths and digests are recorded independently; ordinary reference error responses
may be identity even for gzip requests. Timeouts, duplicate metadata and remote
canceled errors must pass normally.
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
dependencies, and the publication guard. It runs `gem install --norc --local`
from a separate temporary working directory inside `Bundler.with_unbundled_env`,
with fresh `GEM_HOME`/`GEM_PATH` and cleared Ruby/Bundler options. It verifies
core require without Google/Rack and useful missing-optional-require errors.
It then installs the cached, locked optional artifacts, including native
`bigdecimal`, Google protobuf and its `rake` dependency, Rack, rackup and
WEBrick. The installed gem performs real typed Rack/NetHTTP calls with both
binary and JSON scalar-wrapper payloads with identity and gzip, including errors. The helper scripts are copied to the
external cwd; library code loads exclusively from the fresh installed gem.
The installed executable first proves missing Google fails helpfully with empty
stdout. The task retains its task-local compiler activation, and the check
requires `protoc --version` to report `libprotoc 36.2`. Missing executables and
failed commands report the command and failure status. After the optional
artifacts are installed, actual protoc invokes only
`GEM_HOME/bin/protoc-gen-connectrpc-ruby` outside the checkout with cleared
Ruby/Bundler source paths. The generated tree performs typed success/error,
metadata/trailers, nested imported types and presence calls in both codecs.
Google package and explicit Ruby package fixtures import ordinary WKTs, assert
exact official message-class identity and perform typed calls in both codecs.
The check does not install the development JSON gem: it exercises the Ruby
default JSON and Net::HTTP gems. All installs are offline and temporary files
are removed. Nothing is published.

Only `lib/**/*.rb`, `exe/protoc-gen-connectrpc-ruby`, `README.md`, and the two
design/development documents ship. The executable metadata and packaged execute
bits are checked; syntax and RuboCop include the extensionless executable.
The exact manifest in `scripts/check_package.rb` must be deliberately updated
when library files are added. Specs, fixtures, examples, peer code, tooling,
coverage and installed development dependencies do not ship.

### Generator provenance and tools

Ordinary Specs load unique checked-in official outputs without installing
protoc, Buf, Go or the conformance runner. `spec/fixtures/generation/proto/`
owns unique schemas; `ruby/` contains protoc 36.2 message output and this
plugin's glue. `echo/` contains official outputs for the existing shared
`echo.proto`; these run in a separate process to avoid duplicate descriptor-pool
registration with `EchoExample`. Never hand-edit generated files.

`generation:check` regenerates messages/glue with protoc 36.2 and runs real Buf
1.75.0 local plugins, including the official protoc Ruby builtin. Service glue
must be byte-identical between compilers. Their official message descriptor bytes
can differ, so each output tree is independently loaded and exercised. Compiler
version, comments and source-code info do not affect glue. Official `_pb.rb`
files alone are excluded from RuboCop; glue remains checked, with scoped naming
and named-forwarding exceptions for the specified generated API.

Mise locks Buf's Linux x64 archive SHA256
`7168a06e29bd5eb8d1530848f242264f7a86b9b8af3b2e811570fbea0c929a30`,
verified against upstream [sha256.txt](https://github.com/bufbuild/buf/releases/download/v1.75.0/sha256.txt).
The stable [release API](https://api.github.com/repos/bufbuild/buf/releases/tags/v1.75.0)
reports publication on 2026-10-22T01:02:29Z, over seven days before the
2026-11-02 audit. Existing protoc and all other pins remain unchanged. Buf is
development-only and uses ignored `vendor/buf/cache`, never global config/cache.
Package CI installs both tools; Interop consumes checked-in output and needs
neither compiler. `example:generated` generates temporary greeting output and
cleans it up after observable localhost shutdown.

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
arbitrary comma-containing values. The separate generated process adds Ruby->Go
success/error calls and reuses
the Go client's six existing success/error/limit scenarios against generated
Ruby handler registration. Both codecs execute, retaining the original 18-case
fixture result unchanged. This is not the Connect conformance suite.

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

## Optional Rails development

The self-contained [Rails adapter guide](../gems/connectrpc-rails/README.md)
owns controller setup, lifecycle/security limits and generated-code placement.
Root `Gemfile`, lock, runtime dependencies, setup and specs do not require Rails.
Discover adapter commands with `mise tasks`:

```sh
RAILS_LANE=8.0 mise run test:rails
RAILS_LANE=8.1 mise run verify:rails
MISE_RUBY_VERSION=4.0.9 RAILS_LANE=8.1 mise run verify:rails
mise run generation:check:rails
mise run build:rails
mise run check:package:rails
```

The launcher prints Ruby, exact selected Rails, Bundler and Gemfile. Specs assert
actual loaded Rails/Bundler. The tested combinations are Rails 8.0.5.1/Ruby
3.4.12 and Rails 8.1.4/Ruby 3.4.12 or 4.0.9. The default lane is 8.1.
`verify` includes the selected Rails lane, not all three combinations; CI runs
all three independently. Adapter setup depends on `setup:bundler`, not root
`setup`, and uses the root ABI-specific `vendor/bundle` cache.

Actual Appraisal 2.5.0 generates the lane Gemfiles. Its parser cannot accept a
source cooldown keyword, so the adapter development source is plain and tasks
and CI enforce `BUNDLE_COOLDOWN=7`. Root source cooldown/pins/lock stay intact.
There is no adapter base lock: select one generated checksummed lane lock for
frozen installation and actual `appraisal <lane>` execution. Do not run
`appraisal install` in CI. The launcher checks Bundler 4.0.23 is installed
before Appraisal can invoke its legacy installer fallback.
`generation:check:rails` runs the actual generator in a temporary relative-layout
copy and byte-compares both outputs without touching checked-in Gemfiles.
Changing dependencies requires the same seven-day primary registry release-age
and archive checksum audit as root dependencies, including native variants.

Adapter SimpleCov includes every adapter `lib/**/*.rb` file before requires
and writes to `vendor/rails/coverage`, separate from core. Real app tests use
matching Rails defaults, native Base/API dispatch and CSRF/session behavior,
Rack::Lint, eager load/reload and synchronized concurrent requests.
`build:rails` builds both exact archives into `vendor/rails/pkg` independently
of root build output. `check:package:rails` checks manifests, guards and
conventional runtime dependency requirements, installs offline into fresh
GEM_HOME/GEM_PATH outside the checkout, and exercises installed generated
binary/JSON clients against native Rails callbacks and structured failures.
Only the adapter README and library files ship in its archive. No dependencies
or fixtures are vendored into either package, and nothing is published.

## Optional HTTP/2 development

The [backend guide](../gems/connectrpc-async/README.md) owns its public API,
reactor affinity, TLS/mTLS lifecycle, backend normalization and resource limits.
Use `mise run async:setup`, then focused `mise run async:test -- spec/tls_spec.rb`
or `async:check` while iterating. `async:build` builds its own core and backend
archives under `vendor/async/pkg`; `async:package` installs cached normal runtime
dependencies in a fresh outside-checkout GEM_HOME and performs generated
h2c/mTLS calls with identity and gzip plus core NetHTTP TLS/JSON in both encodings.
That process activates OpenSSL 4.0.2/JSON 3.1.0, so its TLS trust fixture is passed
only to the child process.
`async:example` runs the same generated localhost scenarios from development
sources. `async:interop` uses the existing pinned Go module graph and toolchain
with a separate peer executable. `async:verify` combines backend checks, product
specs, package isolation and independent Go interoperability; root `verify`
includes it alongside the existing core and selected Rails verification.

The backend Gemfile/lock is independent; do not resolve it into root or Rails
locks. All registry/platform artifacts require seven-day primary release-age
and archive checksum verification, including generic/native variants. Keep its
root dependency as a development-only relative path. Bundle installation and
native caches use `vendor/async/bundle`; Bundler user state uses
`vendor/async/bundler`. Only the observed generated backend `.bundle/` directory
is ignored. SimpleCov starts before backend/core requires, includes every
backend library file and writes `coverage/async/<Ruby>/`. Green coverage does
not establish interoperability or hosted clean-environment success.
