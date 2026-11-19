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
active Ruby, then the root bundle into ignored `vendor/bundle/`. This project
ships gems, so no development lock is committed: the first setup resolves the
newest versions the Gemfile and gemspecs allow (subject to the cooldown below)
and writes an ignored local `Gemfile.lock`. Later setups reuse that local lock.
Root Bundler user state is scoped to `vendor/bundler/`; optional bundles set
their own user directories and ignored locks.
Re-running setup is safe. Bundler config files are ignored so user-level
settings cannot silently change the bundle location or resolution behavior.
`mise.lock` currently locks Linux x64 downloads and checksums, including both
Ruby source archives. Add and review locks for other development platforms
with `mise lock --platform <platform>` before installing there.

## Commands and evidence

`mise tasks` is the command index. Ruby operations use Rake where useful;
Mise owns setup ordering and verification composition.

| Command | Purpose |
| --- | --- |
| `mise run setup` | Install locked tools, pinned Bundler and the root bundle. |
| `mise run format` | Apply RuboCop Layout fixes. |
| `mise run format:check` | Check Layout without writing. |
| `mise run lint` | Run remaining RuboCop cops. |
| `mise run syntax` | Parse maintained library, specs, scripts, gemspec, Gemfile, Rakefile. |
| `mise run test -- spec/connectrpc_spec.rb` | Run focused RSpec examples; accepts `file:line` and RSpec options. |
| `mise run test` | Run all specs and generate coverage. |
| `mise run lint:workflows` | Run actionlint and offline zizmor. |
| `mise run check:static` | Run format, lint, and workflow checks once. |
| `mise run check` | Run static checks and Ruby syntax checks. |
| `mise run build` | Build `pkg/connectrpc-<version>.gem`. |
| `mise run check:package` | Build, install, and require the gem in isolation. |
| `mise run setup:generation` | Install task-local protoc 36.2 and development-only Buf 1.75.0. |
| `mise run generation:build` | Regenerate checked-in official messages and service glue using protoc; verify actual Buf glue parity/runtime. |
| `mise run generation:check` | Regenerate into temp trees, compare fixture bytes/glue parity, exercise both protoc and Buf output. |
| `mise run example:generated` | Compile the greeting proto and run its generated localhost client/handlers in both codecs. |
| `mise run example` | Run a typed ephemeral localhost Rack/NetHTTP example in both codecs. |
| `mise run example:server -- NAME [--mtls]` | Serve a [runnable example](../examples/README.md) server until Ctrl-C; `PORT` overrides 8080. `--mtls` (Async, Falcon) requires client certificates. |
| `mise run example:client -- NAME [RPC [ARGS...]]` | Call the running example server; `--list` prints servers, clients, RPCs and options. |
| `mise run example:verify` | Run every supported example pairing and representative refusals on ephemeral ports. |
| `mise run fixture:build` | Rebuild the shared descriptor fixture from its explicit Ruby schema builder. |
| `mise run fixture:check` | Compare the checked-in descriptor bytes to the builder. |
| `mise run setup:interop` | Install task-local Go 1.27.3 and download checksum-locked modules. |
| `mise run test:interop` | Run the Go-to-Go reference baseline and both cross-language directions/codecs, 18 original identity cases, the generated path, then 40 separate gzip cases. |
| `mise run setup:conformance` | Install task-local official runner 1.0.6 and protoc 36.2 with locked checksums. |
| `mise run conformance:build` | Generate descriptors from unchanged official schemas using protoc. |
| `mise run conformance:check` | Verify schema hashes and byte-for-byte descriptor regeneration. |
| `mise run test:conformance` | Run the declared identity and gzip client/server profiles with strict accounting; see the [conformance guide](../conformance/README.md). |
| `mise run verify` | Run core checks, specs, packages, generation, Go interop, official conformance, the selected Rails lane and the independent HTTP/2 and Falcon backends. |

Every entry point installs missing prerequisites through the same task graph.
During Ruby iteration, use focused tests and relevant static checks. Run
`check` for fast static feedback and `verify` for package/tooling changes and
final implementation handoff. Run `mise run verify` and then `mise run rack:verify`
for final handoff; moved middleware specs have their own canonical gate.
[CI](https://github.com/jimeh/connectrpc-ruby/blob/main/docs/ci.md) splits the same
canonical component tasks into nine core, three Rails, four optional HTTP/2,
two optional Rack, two optional testing, two optional RBS and four optional Falcon
checks, plus the optional OTel, metrics, health, Sentry and hyper leaves, with syntax, specs, package isolation,
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

Identity requires 96 actual client and 90 server passes, including two exact
GET names per direction. The disjoint gzip profile requires 88 client and 82
server passes, for 184/172 in total. There are no expected-failure manifests or
waivers. All four caller-cancellation cases use the original 5 ms timer armed
before the blocking call, the original 200 ms response request, and no deadline.
Their causal ledger distinguishes positively observed cancellation before
transport, during setup and during exchange using closed call/transport scope
counts, source transition and the actual library canceled/nil-cause result.
Missing dispatch alone cannot establish an early cancellation. A separately
proved `response_observed` branch allows explicit cancellation before Client's
terminal commit after a complete transport response, without claiming Client
validated/decoded it or interrupted its socket. Completed writes
retain actual byte/body/name/no-timeout checks; interrupted writes leave byte
delivery unknown. Dedicated synchronized real-wire tests prove request progress,
interruption/retirement and cleanup separately from the official timer scenario.
Lifecycle close-call telemetry leaves interruption causality unknown because
NetHTTP can independently close the socket; it still requires observed closure
and retirement for attached sockets.
Never gate/restart the official timer after wire progress.
Actual wire coding, byte counts, lengths and digests remain independently
checked. Ordinary reference error responses may use identity for gzip requests.
Timeouts, duplicate metadata and remote canceled errors must also pass normally.
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

Canonical builders share `scripts/package_build.rb`. It loads each gem's exact
manifest and metadata in its owning source directory, then builds from a private
temporary copy with executable files set to `0755` and ordinary files to `0644`.
Checkout permissions and bytes remain unchanged, and each package lane builds
its own archives in its existing output directory. `spec/package_build_spec.rb`
checks real archive headers, complete file bytes, manifests and metadata under
umasks `002` and `022` using group-writable fixture inputs.
Gemspec manifests resolve from the gemspec's own directory, so loading one does
not depend on the working directory. RubyGems still reads payload files relative
to its own working directory, so run a direct `gem build` from the owning
directory, for example `gem -C gems/connectrpc-async build connectrpc-async.gemspec`.

`check:package` asserts an explicit reviewed package manifest, zero runtime
dependencies, MIT metadata and complete license bytes, and the push host. It runs `gem install --norc --local`
from a separate temporary working directory inside `Bundler.with_unbundled_env`,
with fresh `GEM_HOME`/`GEM_PATH` and cleared Ruby/Bundler options. It verifies
core require without Google/Rack, an ordinary missing `connectrpc/rack` file
error, and useful missing Google/generator dependency errors.
It independently builds the optional Rack archive under `vendor/package/rack`,
stages it and only the required runtime archives in the external cwd, and uses
normal offline dependency resolution. It installs the cached, bundle-resolved optional
artifacts, including native
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

Core ships no `lib/connectrpc/rack.rb`. Core `sig/`, the explicitly selected
`rbs/google_protobuf_4_36/` bridge, remaining `lib/**/*.rb`,
`exe/protoc-gen-connectrpc-ruby`, `LICENSE`, `README.md`, and the
design/development/[RBS guide](rbs.md) documents ship. The executable metadata and packaged execute
bits are checked; syntax and RuboCop include the extensionless executable.
The optional Rack, Rails, Async, testing and Falcon packages carry byte-identical copies of the root
`LICENSE`; their package checks verify both the copies and archived notices.
Keep these copies in sync when updating the canonical notice.
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

### Gem dependency constraints

Users resolve our gemspec requirements, never our development locks, so the
gemspecs are the dependency contract. Each third-party runtime dependency falls
into one of three tiers, and internal gems follow the train:

1. **Floor and cap.** For dependencies whose public API is all we use, the
   floor is the release the project currently tests, for example
   `rack >= 3.2.7` or `falcon >= 0.57.0`. Pre-1.0 dependencies are capped below
   1, and 1.x+ gems below their next major.
2. **Guarded coupling.** A dependency whose internals we rely on may take a
   floor-and-cap range only when all of the following hold:
   - `require` detects the coupling at boot. A contract check runs before any
     request, fails loudly with a named error (`UpstreamContractError`) and has
     no opt-out.
   - An `upstream_contract_spec.rb` example pins each relied-on behavior and
     each worked-around behavior.
   - The daily newest-allowed CI run exercises it.

   Coupling does not qualify when a boot check cannot observe it: ordering
   under concurrency, flow-control accounting or eager-start races. Neither do
   replaced methods whose failure mode is a hang or silent misbehavior under
   load, nor anything checked only by its shape. The only member is `async`
   in connectrpc-async (`>= 2.46.0, < 3`): the
   `ConnectRPC::Async::Internal::UpstreamContract` finalization probe proves at
   require that Async still calls the per-instance `#consume` the completion
   joins depend on.
3. **Patch-pinned.** Everything else coupled to private internals keeps a
   patch-only `~> x.y.z` requirement, named with its reason in the gemspec
   comment:

   | Gem | Package | Requirement | Reason | What would unblock it |
   | --- | --- | --- | --- | --- |
   | async-http | connectrpc-async | `~> 0.105.0` | Own copies of `read_in_background` and `Output#start` publish their task before the eager first pass; an `Output` subclass calls the private `stream`/`passthrough`; the `Stream` module writes `@input`/`@output` and reads `@length`; `Server#each` is an owned loop. The require guard checks their shape, but not the eager-start ordering or ivar roles they replace, and reverting them hangs the suite. | Upstream ask U2 in [issue #68](https://github.com/jimeh/connectrpc-ruby/issues/68): publish the reader and output tasks before their first run, a stream-class hook, and a `Stream#length` reader. |
   | protocol-http2 | connectrpc-async | `~> 0.29.2` | Receive-window enforcement (`consume_local_window` and the stream `receive_data` check) and recording credit before writing WINDOW_UPDATE replace its flow-control accounting. `update_local_settings` keeps the configured connection window target, and streams take the advertised stream window before SETTINGS ACK. | Upstream ask U3 in issue #68: enforce receive flow control and expand the local window before writing WINDOW_UPDATE. |

   connectrpc-falcon has no patch pins of its own. Its `HTTP1Connection`
   subclasses async-http's `HTTP1::Server`, and Falcon's `UpstreamContract`
   checks its `Endpoint` hooks at require; connectrpc-async's async-http pin
   holds that coupling.
4. **Internal gems** keep the train's `~> M.m.0` requirement.

Raise a floor only when code needs a newer release, and prove it in the
lowest-allowed lane below. To bump a patch pin, update the gemspec requirement
and its comment, `UpstreamContract::TESTED` and the package check's expected
requirement strings (`gems/<gem>/scripts/package.rb`). A green
[pins-lifted canary](#pins-lifted-canary) run at the new release is the
evidence. Moving a dependency between tiers updates the gemspec comment and
this list.

### Fresh resolution and the lowest-allowed lane

Own-gem development locks (`Gemfile.lock`, each optional package's
`Gemfile.lock` and both Rails lane locks) are ignored. A fresh checkout and
every CI run resolve the newest versions the Gemfiles and gemspecs allow. Only
tools keep exact Gemfile pins: rake, rspec, rubocop, simplecov, appraisal,
webrick, rackup, and google-protobuf 4.36.2, which matches pinned protoc 36.2
and the `google_protobuf_4_36` RBS bridge. The tool-only `typecheck/` and
`release/` bundles keep committed, frozen, checksummed locks.

Setup reuses an existing local lock, so a stale local resolution persists
until you refresh it. Run `mise exec -- bundle update` for the root bundle, or
delete the owning ignored lock and re-run its setup task, for example:

```sh
rm gems/connectrpc-async/Gemfile.lock && mise run async:setup
rm gems/connectrpc-on-rails/gemfiles/rails_8.0.gemfile.lock && RAILS_LANE=8.0 mise run setup:rails
```

`CONNECTRPC_DEPENDENCY_FLOORS=1` selects the lowest-allowed lane. Each launcher
then writes an ignored wrapper beside the owning Gemfile (`Gemfile.floors`, or
`gemfiles/rails_8.0.floors.gemfile`). The wrapper evaluates the real Gemfile
through [`scripts/dependency_floors.rb`](../scripts/dependency_floors.rb), which
pins every third-party runtime dependency of the loaded connectrpc gemspecs to
the lowest version its requirement admits (`>= 3.3` becomes 3.3.0 and
`~> 0.105.0` becomes 0.105.0). Bundler derives a separate ignored
`*.floors.lock`, so normal locks stay untouched, and a missing floor release
fails resolution. Real Gemfiles, `Appraisals` and generated lane Gemfiles carry
no floors logic. The Ruby 3.4.12 floors tasks delete their floors lock, resolve
the bundle fresh, assert every floor resolved exactly, and run specs and package
checks:

```sh
mise run floors:async
mise run floors:falcon
mise run floors:rack
mise run floors:rails   # Rails 8.0 lane, directly under the wrapper bundle
mise run floors:otel
mise run floors:prometheus
mise run floors:yabeda
mise run floors:health
mise run floors:sentry
mise run floors:hyper   # rb_sys 0.9.130 for the source gem
mise run floors         # all ten
```

Floors do not apply to development tools or transitive-only dependencies. A
launcher can add a floors-only constraint on a transitive gem through
`DependencyFloors.gemfile(..., constraints: { ... })`; the wrapper declares it,
and real Gemfiles never do. The OTel and Sentry launchers constrain `json` to
`< 3` because their tools otherwise resolve json 3.x, which rejects the JSON
`quirks_mode` option that the Action Pack floor's ActiveSupport (Rails 8.0)
passes. The installed package checks expect the release RubyGems activates, which can be a
newer Ruby default gem (for example Ruby 3.4's openssl 3.3.x) than the floor.

A new optional gem under `gems/connectrpc-<name>/` follows the same model:

- Its `Gemfile` uses `source "https://rubygems.org", cooldown: 3`, the gemspec,
  core as a relative `path:` gem, and exact pins for tools only. The
  `.gitignore` already covers `gems/*/Gemfile.lock` and floors files.
- Its launcher sets `BUNDLE_GEMFILE` through
  `DependencyFloors.gemfile(...)`, clears `BUNDLE_LOCKFILE`, sets
  `BUNDLE_COOLDOWN=3`, and offers a `floors` subcommand, as
  `gems/connectrpc-rack/scripts/run.rb` does.
- Add a `floors:<name>` task and CI leaf when the gem has third-party runtime
  dependencies. Package checks resolve archives from the bundle it resolved,
  never from hard-coded versions.

### Pins-lifted canary

The pins-lifted canary is a non-gating daily lane for the patch pins above. It
resolves each local package's third-party `~> x.y.z` pin at its newest release
below the next major (`< 1` for 0.x), runs the Async or Falcon upstream contract
spec and suite, and reports which contract broke, before anyone bumps a pin:

```sh
mise run canary:async
mise run canary:falcon
mise run canary:hyper   # cargo update in a temporary copy; see below
mise run canary         # all three
```

`CONNECTRPC_DEPENDENCY_CANARY=1` makes the launcher write an ignored
`Gemfile.canary` beside the package Gemfile. It evaluates the real Gemfile
through [`scripts/dependency_canary.rb`](../scripts/dependency_canary.rb),
which first prepends a module onto `Gem::Specification` that widens the patch
pins of every local gemspec loaded afterwards. That includes connectrpc-async
inside the Falcon bundle and the `bundle exec` children. Bundler derives an
ignored `Gemfile.canary.lock`, and the bundle and Bundler user state (including
its index cache) live in `vendor/<pkg>/canary/`, so normal and floors bundles,
locks and caches never see the lifted requirements. Canary and floors mode
cannot be combined, and canary mode allows only the launcher's `setup`, `test`,
`exec` and `canary` commands, never `build` or `package`.

Each task deletes the canary lock, installs the bundle (a resolution failure
fails here), then runs [`scripts/canary_driver.rb`](../scripts/canary_driver.rb)
outside Bundler:

1. A report lists each lifted dependency (its pin, lifted range, resolved
   version, and whether that is within or beyond the pin) and the resolved
   socketry stack: the newest eligible releases after the three-day cooldown.
   It checks that the canary lock recorded the lifted requirements, and it
   fails when nothing was lifted, which means the lane should be retired or
   retargeted.
2. `spec/upstream_contract_spec.rb`, then the rest of the suite. Each phase
   runs in its own process group under a wall-clock deadline (5 and 30
   minutes); on expiry the driver kills the group, so a hang is still reported
   and the other phase still runs. A process the phase leaves behind holding
   its output fails the phase a few seconds after the command exits, and the
   driver kills its group.

The summary classifies each phase as passed, a timeout (hang), a contract guard
failure (the `UpstreamContractError` raised at require, with each broken
contract), a boot failure (another load error, or an error before any example
ran, such as a removed upstream file or class), or example failures with their
ids and any error raised outside examples, such as in an `after` hook. A failed
"worked around" example can mean upstream fixed that behavior; delete the
workaround it names rather than treating it as a break. In GitHub Actions the
report and summary are also appended to the job summary. CI runs the lane on
Ruby 3.4.12 only; it has no Ruby 4.0.9 leg.

### Cooldowns and updates

Bundler's source cooldown is three days (`BUNDLE_COOLDOWN=3` and Gemfile
`cooldown: 3`), so fresh resolution can pick a release three days old. Old
registries or index entries lacking timestamps may not enforce it. See
[Bundler cooldown](https://bundler.io/man/bundle-config.1.html). CI runs daily
to catch upstream releases that break the newest or lowest allowed versions.
Report such a break; fix it if it is our bug, otherwise tighten that one
gemspec requirement with a reason rather than pinning the development Gemfile.

Keep exact tool pins. Check release dates against the
[primary RubyGems API](https://guides.rubygems.org/rubygems-org-api/) or upstream
GitHub releases before updating: stable tool and action releases must be at
least seven days old. Verify Ruby requirements for both supported Rubies. For
actions, resolve the upstream release tag to its commit and retain a full SHA
with a tag comment. Mise's minimum age filters fuzzy resolution, not explicit
pins; review pinned release ages manually.

The tool bundles update their committed locks with frozen mode temporarily
disabled. Keep the generic Ruby platform and checksums, and do not add a `ruby`
Gemfile directive or `RUBY VERSION` field that excludes the other CI Ruby:

```sh
mise exec -- env BUNDLE_GEMFILE=typecheck/Gemfile BUNDLE_FROZEN=false bundle lock --update --add-platform ruby --add-checksums
mise lock --platform linux-x64
```

For an urgent security fix inside the cooldown window, obtain an explicit
maintainer exception, record the advisory/version/reason in the PR, and
override cooldown only for that resolution (for example,
`mise exec -- env BUNDLE_COOLDOWN=0 bundle update <gem>`).
Do not weaken the persistent policy. Original project work is [MIT licensed](../LICENSE).
The invalid push host remains an accidental-push guard, not a security boundary
against someone editing metadata. Publishing, releasing or deploying requires
separate, explicit authorization.

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
Releases use a Release Please PR on main, local candidate tasks and a
tag-triggered publishing workflow. See the
[release guide](https://github.com/jimeh/connectrpc-ruby/blob/main/docs/releasing.md)
for App and trusted-publisher setup, synchronized versions and recovery.
Project MIT licensing is approved. Releasing still needs separate, explicit authorization.

Guides on main describe the next release. When a PR documents a feature,
option, event or metric that the latest release lacks, mark it "Since X.Y.Z"
in the prose with the version the next release will have:

- Use the version of the open Release Please PR.
- When no release PR is open, use the next patch after the version in
  `.github/.release-please-manifest.json`. A merged release PR sets that
  version even while its tag waits to be published. Before 1.0, the release
  config (`bump-patch-for-minor-pre-major`) releases `feat:` commits as a
  patch.
- A breaking change makes the next release a minor (`bump-minor-pre-major`),
  such as 0.3.0 after 0.2.1 in the manifest. Mark it with that version, and update any markers
  on main that name the superseded patch version.

Keep markers out of headings, so anchors stay stable, and keep them after the
release, so readers on older installs can tell what they lack. Fixes and behavior changes to existing
features need no marker; breaking minors also need migration notes in
[upgrading.md](upgrading.md).

## Optional Rails development

The self-contained [Rails adapter guide](../gems/connectrpc-on-rails/README.md)
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

The launcher prints Ruby, the selected lane, Bundler and Gemfile. Specs assert
that the loaded Rails belongs to the lane and the actual Bundler. The tested
combinations are the newest Rails 8.0.x/Ruby 3.4.12 and newest Rails 8.1.x/Ruby
3.4.12 or 4.0.9, plus the Action Pack floor 8.0.5.1/Ruby 3.4.12 in
`floors:rails`. The default lane is 8.1.
`verify` includes the selected Rails lane, not all three combinations; CI runs
all three independently. Adapter setup depends on `setup:bundler`, not root
`setup`, and uses the root ABI-specific `vendor/bundle` cache.

Actual Appraisal 2.5.0 generates the lane Gemfiles from `Appraisals`, which
admit any `~> 8.0.0` or `~> 8.1.0` Rails release. Its parser cannot accept a
source cooldown keyword, so the adapter development source is plain and tasks
and CI enforce `BUNDLE_COOLDOWN=3`. There is no adapter base lock: each lane
resolves its own ignored lane lock for actual `appraisal <lane>` execution. Do
not run `appraisal install` in CI. The launcher checks Bundler 4.0.23 is
installed before Appraisal can invoke its legacy installer fallback.
`generation:check:rails` runs the actual generator in a temporary relative-layout
copy and byte-compares both outputs without touching checked-in Gemfiles.

Adapter SimpleCov includes every adapter `lib/**/*.rb` file before requires
and writes to `vendor/rails/coverage`, separate from core. Real app tests use
matching Rails defaults, native Base/API dispatch and CSRF/session behavior,
Rack::Lint, eager load/reload and synchronized concurrent requests.
`build:rails` builds core, Rack and Rails exact archives into `vendor/rails/pkg` independently
of root build output. `check:package:rails` checks all three manifests, guards and
conventional runtime dependency requirements, stages only their runtime closure, installs offline into fresh
GEM_HOME/GEM_PATH outside the checkout, and exercises installed generated
binary/JSON clients against native Rails callbacks and structured failures.
The adapter README, `LICENSE`, library files and its owned `sig/` manifest/signatures ship in its archive. No dependencies
or fixtures are vendored into either package, and nothing is published.

## Optional HTTP/2 development

The [backend guide](../gems/connectrpc-async/README.md) owns its public API,
reactor affinity, TLS/mTLS lifecycle, backend normalization and resource limits.
Use `mise run async:setup`, then focused `mise run async:test -- spec/tls_spec.rb`
or `async:check` while iterating. `async:build` builds its own core and backend
archives under `vendor/async/pkg`; `async:package` installs cached normal runtime
dependencies in a fresh outside-checkout GEM_HOME and performs generated
h2c/mTLS calls with identity and gzip plus core NetHTTP TLS/JSON in both encodings.
That process asserts it activates the OpenSSL and JSON releases the development
bundle resolved, and its TLS trust fixture is passed only to the child process.
`async:example` runs the same generated localhost scenarios from development
sources. `async:interop` uses the existing pinned Go module graph and toolchain
with a separate peer executable. `async:verify` combines backend checks, product
specs, package isolation and independent Go interoperability; root `verify`
includes it alongside the existing core and selected Rails verification.

The backend Gemfile and its ignored local lock are independent; do not resolve
it into root or Rails bundles. Keep its root dependency as a development-only
relative path. Bundle installation and
native caches use `vendor/async/bundle`; Bundler user state uses
`vendor/async/bundler`. Only the observed generated backend `.bundle/` directory
is ignored. SimpleCov starts before backend/core requires, includes every
backend library file and writes `coverage/async/<Ruby>/`. Green coverage does
not establish interoperability or hosted clean-environment success.

## Optional Rack development

The [middleware guide](../gems/connectrpc-rack/README.md) owns installation,
mounting and lifecycle limits. Run `mise run rack:setup`, focused
`mise run rack:test -- spec/rack_spec.rb`, and `mise run rack:check` while iterating.
The Gemfile reuses the root tool pins with core as a development path
dependency; Rack floats within the gemspec requirement. Bundle, Bundler user state, lint cache and build
outputs are isolated under `vendor/rack`; SimpleCov starts before requires and
covers every package source in `coverage/rack/<Ruby>/`.

`rack:build` builds its own core and middleware archives, avoiding root/Rails
output races. `rack:package` checks exact manifests/dependencies/guards, installs
core alone to prove middleware is unavailable, then resolves the Rack package
normally offline using staged runtime archives in an external fresh GEM_HOME.
It runs real Google proto/JSON identity/gzip success, errors/details and metadata
under Rack::Lint over ephemeral localhost NetHTTP, asserting installed core and
Rack source ownership. Root installed checks also retain default JSON/NetHTTP
and the actual installed generator. Rails checks all three local owners.

Final local handoff requires `mise run verify` followed by `mise run rack:verify`.
Rack verification stays separate from root `verify`. CI runs the core checks and
two Rack specs/check/package lanes on the existing Rubies; clean-environment
matrix evidence remains a hosted responsibility. No runtime savings are claimed.

See the optional [in-process testing package](../gems/connectrpc-testing/README.md) for real encoded client/server round trips without sockets.

## Optional RBS development

[The RBS guide](rbs.md) covers official message generation, opt-in service RBS,
strict Steep configuration, the explicit protobuf bridge and nominal alternatives.
`mise run rbs:setup` installs the independent frozen checksummed tool
bundle. `rbs:check` checks protoc/Buf parity, unchanged default Ruby and official
message RBS, deterministic output, real generated runtime calls, RBS validation
and positive/negative strict consumers. It also runs the strict core
implementation target defined by `typecheck/source.rb`, reviewed source inventory
and per-package API callers.
Discover the focused `rbs:source`, `rbs:api`, `rbs:inventory` and capture-only
`rbs:proof` leaves with `mise tasks`. `rbs:package` checks every installed package
and composed optional environments outside the checkout, using the existing
scoped optional setups to populate offline archives. RubyGems resolves the
installed closure from the running Ruby ABI's cached archives. Run both on each pinned Ruby.
Locally, that closure comes from the union of the scoped bundle caches, so
`rbs:package` assumes those bundles were refreshed recently: stale ignored local
locks can leave packages on different versions. CI resolves every bundle fresh.
These leaves keep their Bundler state, caches, bundle and package outputs under
`vendor/rbs/` and add no runtime or root-bundle dependencies.

## Optional Falcon development

The [Falcon guide](../gems/connectrpc-falcon/README.md) owns the listener,
worker-local TLS provider and Rack/Rails fallback contracts. Start with
`mise run falcon:setup`, then use focused `falcon:test` selectors and
`falcon:check`. `falcon:build` writes separate core/Async/Falcon archives to
`vendor/falcon/pkg`. `falcon:package` installs normal dependencies offline in an
isolated GEM_HOME and runs the actual Falcon DSL/service outside the checkout.
`falcon:interop` runs identity and gzip against the pinned independent Go peer,
including actual Falcon process workers. `falcon:verify` combines these leaves;
root `verify` includes it. CI runs separate Falcon specs/package and Go checks
across Ruby 3.4.12 and 4.0.9.

Keep its Gemfile and ignored local lock, `vendor/falcon/bundle`, Bundler user
state and `coverage/falcon/<Ruby>/` independent. Actual worker,
TLS, retirement and installed-caller tests establish different properties;
green coverage or baseline probes do not substitute for them.

## Optional OpenTelemetry development

`mise run otel:setup` installs the independent API/SDK and integration bundle
under `vendor/otel`; the SDK and instrumentations float like runtime dependencies. Use `otel:test -- spec/tracing_spec.rb` for generated
in-process traces, `spec/native_spec.rb` for real Async/Falcon boundaries, and
`spec/rack_rails_spec.rb spec/rails_spec.rb` for Rack/NetHTTP and native Rails.
`otel:check`, `otel:build`, `otel:package`, `otel:example` and `otel:verify` are
discoverable leaves. Repeat `MISE_RUBY_VERSION=4.0.9 mise run otel:verify` for the
second pinned runtime. Coverage starts before requires and includes core and all
local integration libraries under `coverage/otel/<Ruby>/`. The two OTel CI leaves
run independently of the other package jobs. Runtime bundles never gain an SDK.
Package checks install normal dependencies offline into a fresh external
GEM_HOME, then run copied generated callers with actual SDK in-memory exports.
See the [package guide](../gems/connectrpc-otel/README.md) for the precise trace
baseline and the unknown-method `rpc.method_original` limitation.

## Optional metrics development

Core's backend-neutral `connectrpc/metrics` layer is covered by the root suite
(`mise run test -- spec/metrics_spec.rb spec/message_events_spec.rb`). The sinks
have independent bundles that resolve the newest allowed versions, and
`floors:prometheus` / `floors:yabeda` repeat specs and package checks at the
gemspec floors. `mise run prometheus:setup` installs
prometheus-client with the local Rack, Async, Falcon and testing packages under
`vendor/prometheus`, and `mise run yabeda:setup` installs Yabeda with the testing
package under `vendor/yabeda`. Each has discoverable `test`, `check`, `build`,
`package`, `example` and `verify` leaves, coverage under
`coverage/{prometheus,yabeda}/<Ruby>/`, and one CI leaf per pinned Ruby. Use
`prometheus:test -- spec/hosts_spec.rb` for real Rack, Async and Falcon host
completion and `spec/direct_file_store_spec.rb` for forked workers. The Yabeda
bundle deliberately excludes yabeda-prometheus, whose `prometheus-client < 5`
is below the range connectrpc-prometheus supports. Package checks
install normal dependencies offline into a fresh external GEM_HOME and run the
copied generated example. See the
[Prometheus](../gems/connectrpc-prometheus/README.md) and
[Yabeda](../gems/connectrpc-yabeda/README.md) guides for metric semantics.

## Optional health development

`mise run health:setup` installs the health service bundle with the local Rack,
Async, Falcon and testing packages under `vendor/health`. Its discoverable
`test`, `check`, `build`, `package`, `interop` and `verify` leaves write
coverage under `coverage/health/<Ruby>/`, and `floors:health` repeats specs and
package checks at the gemspec floors. Use `health:test -- spec/registry_spec.rb`
for Watch concurrency with a controllable writer and `spec/async_host_spec.rb`
for cancellation, connection loss, retirement and drain on the real Async host.
`health:interop` builds the existing `interop/grpc` peer, whose `health.go`
drives grpc-go's standard health client, and runs it against the Async Host,
real Falcon workers and grpc-go's reference server. Core's `Context#on_cancel`
is covered by `mise run test -- spec/context_on_cancel_spec.rb`. The vendored
schema and generated files belong to `generation:build`/`generation:check`.
See the [health guide](../gems/connectrpc-health/README.md).

## Optional hyper development

`gems/connectrpc-hyper/` is a Rust extension (hyper, h2, rustls with
aws-lc-rs, tokio; bound with magnus and rb-sys). Besides the pinned Rubies it
needs Rust 1.98.0 (the rb-sys-dock image's), which `hyper:toolchain` (a
`hyper:tools` dependency) installs from `mise.lock` with rustfmt, clippy,
cargo-deny 0.20.2 and cargo-audit 0.23.0; libclang for
bindgen; a C compiler; and Go 1.27.3 for the peer. On distributions whose
libclang lacks clang's resource headers, the launcher points bindgen at the C
compiler's include directory. Tasks:

| Command | Purpose |
| --- | --- |
| `mise run hyper:setup` | Install the bundle under `vendor/hyper/bundle` and compile the extension into `lib/connectrpc/hyper/<minor>/` (ignored), one per Ruby minor. |
| `mise run hyper:peer` | Build the disposable grpc-go/connect-go/raw HTTP/2 Go peer into `vendor/hyper/peer`. |
| `mise run hyper:test -- spec/pool_spec.rb` | Run specs (paths relative to the gem); coverage goes to `coverage/hyper/<Ruby>/`. |
| `mise run hyper:check` | Ruby syntax and RuboCop, `cargo fmt --check`, `cargo clippy -D warnings`, `cargo deny check`, `cargo audit` and the third-party notices check. |
| `mise run hyper:interop` | The grpc-go/connect-go interop matrix (`--tag interop`). |
| `mise run hyper:package` | Build the source gem, fetch its locked crates into `vendor/hyper/cargo`, install it offline (`CARGO_NET_OFFLINE=true`) into a fresh GEM_HOME and run `scripts/smoke.rb`. |
| `mise run hyper:verify` | All of the above. |
| `mise run hyper:msrv` | Build the extension with Rust 1.85.0, the declared `rust-version`. |
| `mise run hyper:cross -- <platform>` | Cross-build one platform gem (or `ruby` for the source gem) into `vendor/hyper/cross/<platform>/`; `PLATFORM=<platform>` also works. `HYPER_CROSS_TWICE=1` rebuilds from empty caches at the same paths and fails unless the SHA256 match. |
| `mise run hyper:smoke:source -- vendor/hyper/cross/ruby [EVIDENCE.json]` | Install the built source gem into a fresh GEM_HOME, compiling with Rust 1.98.0 and fetching its locked crates, then smoke it. |
| `mise run hyper:cargo:update` | `cargo update` (or `-- -p <crate>`), then regenerate `THIRD_PARTY_NOTICES.md`. |
| `mise run canary:hyper` | Non-gating: the specs against `cargo update`'s newest crates. |

Repeat a lane on the other Ruby with `MISE_RUBY_VERSION=4.0.9`. The offload
specs need Ruby 4.0's fiber-scheduler blocking-operation hook and skip
on 3.4.12; the DNS shim specs need Linux and glibc, and the fork specs skip
where `fork` is unavailable.

Cargo policy: `Cargo.lock` is committed and every build passes `--locked`,
including `extconf.rb`, cross builds and the source gem's install. Update it
only with `hyper:cargo:update`, and check that each new crate release is at
least seven days old, as for tools; the daily
[canary](ci.md#pins-lifted-canary) shows whether the newest crates still pass.
`deny.toml` limits licenses and sources. `THIRD_PARTY_NOTICES.md` lists every
crate linked on any target with its license texts and ships in the gem; a
stale file fails `hyper:check` and `hyper:package`.

Cross builds use rb-sys-dock and the `rbsys/<platform>:0.9.130` images pinned
by digest in `HyperPlatforms::IMAGES` (Docker required; the image compiles, and
host Rust 1.98.0 only runs a `cargo metadata --locked` preflight). The image tag
must equal the bundled rb_sys version, so update both together. `hyper:cross`
copies the package's tracked and unignored files into
`vendor/hyper/cross-work/<platform>`, builds there for Ruby 3.4 and 4.0, fails
if the build changed `Cargo.toml`, `Cargo.lock` or the extension's
`Cargo.toml`, checks the gem and moves it to `vendor/hyper/cross/`, so several
platforms can build from one checkout and the checkout stays clean. Cross
builds keep a Cargo home and rb-sys-dock cache per platform,
`vendor/hyper/cross-cargo/<platform>` and `vendor/hyper/rb-sys-dock/<platform>`;
run one build per platform at a time. With `HYPER_CROSS_TWICE=1` it
deletes the work copy and both caches and rebuilds at the same absolute paths,
because darwin binaries embed the build's absolute object paths. The `ruby` leaf requires Ruby
3.4.12 and the commit time as `SOURCE_DATE_EPOCH`, so its archive equals the
release candidate's source gem.
`ruby gems/connectrpc-hyper/scripts/install_smoke.rb vendor/hyper/cross/<platform> [EVIDENCE.json]`
installs a built gem offline into a fresh GEM_HOME with a core gem built from
the checkout, runs the smoke calls and optionally records release evidence; CI
uses it on each platform. The source
gem needs Rust 1.85+, libclang and a C compiler on the installing host. See the
[package guide](../gems/connectrpc-hyper/README.md) and [the plan](plans/hyper.md).

## Optional Sentry development

`mise run sentry:setup` installs the independent Sentry bundle under
`vendor/sentry`: sentry-ruby, sentry-rails with Rails and in-memory SQLite
ActiveRecord, sentry-opentelemetry, the OpenTelemetry SDK and the local Rack,
Rails, Async, Falcon, testing and OTel packages. They float like runtime
dependencies; only tools are pinned. Every example initializes a real SDK with
`Sentry::DummyTransport`, no background worker threads and a `.invalid` DSN, and
closes it afterwards, so specs never send events. Use
`sentry:test -- spec/rails_spec.rb` for sentry-rails nesting and exception
reporting, `spec/native_spec.rb spec/threaded_spec.rb` for real Async, Falcon and
ThreadedTransport boundaries, and `spec/lifetime_spec.rb` for promotion and the
finish scope. `sentry:check`, `sentry:build`, `sentry:package`, `sentry:example`
and `sentry:verify` are discoverable leaves; repeat
`MISE_RUBY_VERSION=4.0.9 mise run sentry:verify` for the second pinned runtime.
Coverage lives under `coverage/sentry/<Ruby>/`. `floors:sentry` runs the same
specs and package checks with sentry-ruby 6.7.0 (sentry-rails and
sentry-opentelemetry follow it) and constrains `json` to `< 3` like the OTel
lane. Package checks install the archive offline into a fresh external
GEM_HOME, require it without Rails and run the copied generated example. See
the [package guide](../gems/connectrpc-sentry/README.md) for the lifetime model
and its limits.

## Native streaming verification

`async:test` and `falcon:test` include the incremental shape/codec/coding/security
matrix, bounded output/ingress, healthy same-connection siblings and cancellation.
`otel:test` includes actual SDK processor-task identity, mTLS peer identity,
codec/handler cleanup and separate processing/HTTP completion evidence.
Falcon H1 focused specs distinguish the unchanged absolute 100 ms / 256 KiB
discard (adapter remainder and full 64 KiB read reservations) from the cooperative
500 ms total final-close expiry anchored at actual final HTTP flush. They check
EOF before hygiene, delayed callbacks spending the same budget, no reads or
pipeline parsing during remaining grace, admission joins before host cleanup,
owner interruption and real worker shutdown. Timings may overshoot under reactor
delay; they are not hard real-time bounds. SDK specs preserve completed delivery
after later hygiene/grace failure.

`async:interop:streaming` uses the unchanged pinned connect-go graph in both
directions for all three streaming shapes, both codecs and codings over
h2c/TLS/mTLS. `falcon:interop:streaming` runs the Go client against actual Falcon
process workers and checks worker shutdown. These supplement the original unary,
GET, generated, cancellation and tail-drain leaves; they do not replace them.

`async:conformance` executes native client and Async server profiles.
`falcon:conformance` executes only Falcon server profiles. Exact inventories are
290/262 identity client/server and 268/246 gzip client/server, with zero expected
failures. Every run checks actual runner discovery, scheduled/received names,
passes and closed library/transport/server lifetimes. Source transitions and
actual canceled library results are recorded; socket interruption remains unknown
without controlled wire evidence. Original unary profiles and their 5 ms before-call,
200 ms response-delay and nil-deadline semantics remain independent and unchanged.
Both native conformance and streaming Go leaves belong to each backend's verify
aggregate and the existing two-Ruby CI jobs. No test results are cached.
`async:conformance:threaded` adds separate Connect and gRPC unary/server-stream
client profiles through `ThreadedTransport` from plain threads; see
[conformance](../conformance/README.md#threadedtransport-client-profiles).
`async:conformance:per-call` runs the same inventories through `PerCallTransport`
from plain threads and inside a reactor; see
[conformance](../conformance/README.md#percalltransport-client-profiles).
The Falcon-only `falcon:conformance:http1` leaf adds separate SS/CS server
profiles requiring 174 identity and 160 gzip passes. Root `verify` includes this
complete H1 gate, so use that pass for full-profile confirmation. Fresh exact
oversized CS selectors can be run separately for focused regression evidence;
do not weaken their original assertions or inventories.

`rbs:check` includes concrete generated streaming positives and exact negative
fixtures. `rbs:package` runs generated streaming network callers and a custom
streaming SPI under normal installed discovery outside the checkout, in both
package load orders. The [streaming guide](streaming.md) documents the fixed
one-second tail policy and its compatibility tradeoff.

## Lifecycle campaign

The long-running lifecycle campaign repeatedly drives the native clients and
servers through connection churn and faults, then checks that everything they
own retires. It is non-gating and belongs to no `test` or `verify` task:

```sh
mise run async:soak     # Async H2 Transport, ThreadedTransport, PerCallTransport and Host
mise run falcon:soak    # Falcon HTTP/2 and HTTP/1.1 (HTTP1Transport, Net::HTTP)
mise run soak           # both, Async then Falcon
```

The campaign is exercised on Linux, locally and in CI. It counts descriptors
through `/dev/fd`, but RSS and the TCP ports and states in its diagnostics come
from `/proc`; without `/proc` it reports RSS as missing and descriptor targets
without socket details. Other platforms are untested.

[`scripts/soak.rb`](../scripts/soak.rb) runs `scripts/run.rb soak` for each
named package in turn, each in its own process group. A failed package does not
stop the next one, and the script exits non-zero if any package failed.
`scripts/run.rb soak` runs RSpec over `soak/**/*_soak.rb` with `soak/.rspec`,
which loads `soak/soak_helper.rb` instead of `spec_helper`, so SimpleCov never
starts. The scenarios (churn, GOAWAY drains, stream retirement,
stuck frames, slow peers, peer failure, TLS rotation, shutdown, keepalive and
refusal) and their invariants are specified in the
[campaign plan](plans/lifecycle-campaign.md). Shared HTTP/2 scenarios live in
`gems/connectrpc-async/soak/support/scenarios/` and run against both the Async
Host and Falcon; every private-state read lives in
`gems/connectrpc-async/soak/support/sampler.rb`, the one file to update when
library internals move.

Each scenario runs warmup rounds until two consecutive post-cleanup samples of
threads, `/dev/fd` entries and unfinished Async tasks (harness, peer and
ThreadedTransport owner reactors) are identical, at most three. Every measured
round must then settle its owned objects (client and server resources retired,
owner threads joined, registries and shared cancellation listeners empty) and
return exactly to that sample. Every call without its own cancellation source
carries the campaign's shared, never-cancelled token, also when it is expected
to fail. Admission, byte, socket, server connection and stream-buffer peaks are
sampled under established load against their configured caps, and a handler
ledger proves at-most-once execution. There are no tolerance bands. RSS and GC
live slots are reported, never asserted.

| Variable | Default | Meaning |
| --- | --- | --- |
| `CONNECTRPC_SOAK_ROUNDS` | 5 | Measured rounds per scenario |
| `CONNECTRPC_SOAK_DEADLINE` | 900 | Run deadline in seconds; raise it for runs well above the default rounds (CI uses 2,700 for 50) |
| `CONNECTRPC_SOAK_CLEANUP` | 60 | Seconds after the deadline before the process group is killed |
| `CONNECTRPC_SOAK_SEED` | random | Seed for scenario choices and RSpec order; always recorded |
| `CONNECTRPC_SOAK_ONLY` | none | Ruby regular expression matched against example descriptions |

Extra task arguments go to RSpec, for example
`mise run async:soak -- soak/drains_soak.rb` or
`CONNECTRPC_SOAK_ONLY="2a age drain|7 rotation" mise run falcon:soak`.

Reports go to `vendor/<pkg>/soak/<Ruby>/`: `campaign.jsonl` gains one line per
environment record, baseline, round and scenario as they happen, and the
supervisor renders `summary.md` from it, also after a hang. At the deadline the
supervisor sends TERM; the child records thread backtraces, Async task trees and
descriptor targets (TCP sockets with their ports and state) and exits with 124.
After the cleanup budget the supervisor kills the process group. Any failure,
deadline or kill exits non-zero. Reproduce a run with its recorded seed. Keep the
first failure of an intermittent result and its report; do not replace it with a
later pass. The next run for the same package and Ruby replaces the report, so
copy `vendor/<pkg>/soak/<Ruby>/` aside before rerunning.

`spec/soak_harness_spec.rb` runs in `async:test`. It plants a leaked descriptor,
thread, Async task, cancellation registration, unretired resource (also one
that only looks superseded), open round-scoped client, replaced reused
connection, duplicate handler execution, transient socket-cap overshoot,
over-cap retained stream bytes, ingress growth after a saturated window and a
leak in every warmup in small real
campaigns, and requires each to fail at its intended check. Only the poll that
detects a planted leak is shortened. Another control requires teardown to run
every cleanup after a failing one and report the first error.

## Official browser development

`mise run browser:setup` installs development-only tools under `vendor/browser`;
ordinary core setup installs no JS runtime or browser. Linux x86_64 on Ubuntu
24.04 is the audited platform. Python 3's standard ZIP support and tar extract
checksum-verified official archives without OS dependency installation. Exact
artifact hashes and primary publication dates live in `browser/artifacts.json`.
Bun 1.4.4 manages a frozen five-package lock and bundles an ESM entry to IIFE.
Node 24.22.1 executes Playwright 1.64.1 with Chromium 156.0.8078.4. grpc-web 2.1.1
and google-protobuf 4.0.3 are development-only. All pins exceed seven days.

`browser:generation:build` and `browser:generation:check` use official protoc
36.2, protoc-gen-grpc-web 2.1.1 and protoc-gen-js 4.0.3 binaries. Generated
binary/text clients, protobuf messages and canonical Any/Status remain untouched
by hand. `browser:test` exercises actual cross-origin generated browser clients
against real Async, Falcon and native Rails fixtures. `browser:verify` combines
generation parity and the collected browser suite. The two Browser gRPC-Web CI
leaves run on Ruby 3.4.12 and 4.0.9 without replacing existing checks.

Playwright explicitly uses `chromiumSandbox: false` for its owned local process
and `ignoreHTTPSErrors: true` for fixture TLS. No host security or trust setting
changes. These browser TLS calls do not establish verified authentication; the
SDK/mTLS focused specs separately verify peer identity on native H2 and Falcon
H1. No screenshots, shared browser sessions or uploads are required.

The separate `browser/reference` Go module wraps the existing official generated
grpc-go service with improbable-eng/grpc-web 0.15.0 for independent text browser
controls. Audited compress 1.20.1/cors 1.11.1 pins keep its old default graph out
of use; core and existing Go graphs are unchanged. The same executable uses
existing connect-go 1.21.1 for independent binary unary/stream wire controls.
Caches, executables, browser output and fixture logs stay in `vendor/browser`.

`async:conformance:web`, `falcon:conformance:web` and `rack:conformance:web` run
separate exact binary inventories of 138 H2 unary/SS, 276 H1/H2 unary/SS and 37
portable plain-H1 unary cases. Each validates actual runner names, dispatch,
wire arrivals, processing, observer completion and shutdown. The official runner
has no text axis. Existing Connect/native-gRPC inventories remain independent.
