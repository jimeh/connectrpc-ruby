# CI

The workflow runs on pull requests, pushes to `main`, manual dispatch and a
daily schedule (04:37 UTC), on Ubuntu 24.04. It has 48 checks: nine core, three
optional Rails, four optional HTTP/2, two optional Rack, two optional in-process
testing, two optional RBS, four Falcon, two OpenTelemetry, two Prometheus, two
Yabeda, two health, two Sentry, two browser gRPC-Web, nine lowest-allowed
dependency checks and one runnable-examples check, with no serial cache primer
or aggregate job:

| Check | Ruby | Canonical commands |
| --- | --- | --- |
| Static | 3.4.12 | `mise run check:static` |
| Specs | 3.4.12, 4.0.9 | `mise run syntax`, then a separate `mise run test` step |
| Package | 3.4.12, 4.0.9 | `mise run generation:check`, then `mise run check:package` |
| Go interoperability | 3.4.12, 4.0.9 | `mise run fixture:check`, then `mise run test:interop` |
| Official conformance | 3.4.12, 4.0.9 | `mise run test:conformance` |
| Rails 8.0 | 3.4.12 | `mise run test:rails`, `generation:check:rails`, `check:package:rails` with `RAILS_LANE=8.0` |
| Rails 8.1 | 3.4.12, 4.0.9 | Same Rails commands with `RAILS_LANE=8.1` |
| Lowest allowed async, falcon, rack, rails, otel, prometheus, yabeda, health, sentry | 3.4.12 | `mise run floors:<leaf>` |
| Async specs/package | 3.4.12, 4.0.9 | `mise run async:test`, `mise run async:package`, `mise run async:conformance`, `async:conformance:grpc`, `async:conformance:http1`, `async:conformance:threaded` |
| Async Go interoperability | 3.4.12, 4.0.9 | `mise run async:interop`, `mise run async:interop:streaming`, `async:interop:http1`, `async:interop:grpc`, `async:interop:keepalive` |
| Rack specs/check/package | 3.4.12, 4.0.9 | `mise run rack:test`, `mise run rack:check`, `mise run rack:package` |
| In-process testing | 3.4.12, 4.0.9 | `mise run testing:verify` |
| RBS source/API/package | 3.4.12, 4.0.9 | `mise run rbs:check`, then `mise run rbs:package` |
| Falcon specs/package | 3.4.12, 4.0.9 | `mise run falcon:test`, `mise run falcon:package`, `mise run falcon:conformance`, `falcon:conformance:grpc`, `falcon:conformance:http1` |
| Falcon Go interoperability | 3.4.12, 4.0.9 | `mise run falcon:interop`, `mise run falcon:interop:streaming`, `falcon:interop:grpc` |
| OpenTelemetry integration/package | 3.4.12, 4.0.9 | `mise run otel:verify` |
| Prometheus metrics | 3.4.12, 4.0.9 | `mise run prometheus:verify` |
| Yabeda metrics | 3.4.12, 4.0.9 | `mise run yabeda:verify` |
| Health service | 3.4.12, 4.0.9 | `mise run health:verify` |
| Sentry tracing | 3.4.12, 4.0.9 | `mise run sentry:verify` |
| Browser gRPC-Web | 3.4.12, 4.0.9 | `mise run browser:verify`, then `async:conformance:web`, `falcon:conformance:web` and `rack:conformance:web` |
| Runnable examples | 4.0.9 | `mise run example:verify` |

Each matrix has `fail-fast: false`. Every check runs its commands on cache
hits too. Package isolation still installs cached `.gem` artifacts offline and
exercises default JSON and Net::HTTP. Interop still verifies modules and builds
with readonly dependencies before its 18 original identity cases, separate generated
Ruby path and 40 gzip cases. Conformance regenerates descriptors and separately
executes the identity profile (96 client/90 server actual passes, including GET)
and gzip profile (88 client/82 server actual passes). All 184 client and 172 server
cases must pass with zero expected failures; cancellation uses actual source,
request-write and socket cleanup evidence. Existing canonical commands
cover both portions without new jobs or matrix axes. Product, generator and harness specs remain
part of Specs; CI guard specs cover task and runtime boundaries.

## Fresh dependency resolution

Own-gem development locks are not committed, so every job resolves the newest
versions its Gemfile and gemspecs allow, within the three-day Bundler cooldown
(`cooldown: 3` in the Gemfiles and `BUNDLE_COOLDOWN=3` for Appraisal lanes and
adapter jobs). No job sets `BUNDLE_FROZEN`; the runtime guard rejects
`BUNDLE_FROZEN`, `BUNDLE_DEPLOYMENT` and `BUNDLE_LOCKFILE`. Generated locks are
ignored, so the workspace-clean steps still pass. The daily schedule catches
upstream releases that break either the newest or the lowest allowed versions.
The concurrency group includes the event name, so a scheduled run and a push
to `main` do not cancel each other. GitHub disables scheduled workflows in a
public repository after 60 days without repository activity; if that happens,
re-enable the workflow in the Actions tab.

The lowest-allowed matrix runs `floors:async`, `floors:falcon`, `floors:rack`,
`floors:rails` (Rails 8.0 lane), `floors:otel`, `floors:prometheus`,
`floors:yabeda`, `floors:health` and `floors:sentry` on Ruby 3.4.12. Each task sets
`CONNECTRPC_DEPENDENCY_FLOORS=1`, generates its ignored wrapper Gemfile,
installs it, asserts every runtime dependency resolved exactly at its declared
floor (the OTel and Sentry leaves also constrain `json` to `< 3`; see development), then runs
that package's specs and installed-package check. Because the
wrapper does not exist before the task runs, these jobs disable the setup-ruby
bundle cache and install from the registry each time. See
[development](development.md#fresh-resolution-and-the-lowest-allowed-lane).

## Runtime boundary

CI pins `ruby/setup-ruby` v1.335.0 and `actions/setup-go` v7.0.0 by full commit,
with exact Ruby 3.4.12/4.0.9, Bundler 4.0.23 and Go 1.27.3. These official
actions use prebuilt runtimes instead of compiling Ruby in each job. Go is
installed in interoperability and browser reference jobs. The action runtime
downloads trust upstream HTTPS/release assets. Observed archive digests are audit evidence,
not enforced runtime checksum pins in these actions. This differs from local
Mise source/archive installs, which retain their reviewed locks/checksums.

`MISE_ENV=ci` loads `mise.ci.toml`, disabling Ruby/Go resolution and nested
`mise exec` auto-installation. Local `setup`, `check`, `test` and aggregate
`verify` keep their source-build behavior. No `system` tool override or unlocked
installation is used. `setup:tools` validates exact Ruby/Bundler and the real
Ruby path under `RUNNER_TOOL_CACHE`. `setup:interop` additionally validates Go,
GOROOT, working directory, module mode and actual cache paths before downloading
modules. The guard prints runtime paths, actual bundle path, CPU count,
OS/architecture and runner image to support timing analysis.

A command-bearing Mise override replaces the entire task. The CI interop
setup therefore repeats the local task's `tools.go`, environment and directory;
keep those fields aligned when changing the local task. Its only behavioral
change is replacing the runtime installer with validation. The Mise action
adds neither shims nor tool/environment PATH exports, so it cannot hide the
action runtimes. Canonical tasks still activate their task-local static,
generation and conformance tools. Each job rejects tracked changes and unexpected untracked
files, including accidental `mise.ci.lock` creation. Ignored bundle, package,
coverage and harness outputs are allowed.

## Native caches and invalidation

RBS uses an explicit dependency-bundle cache described below, and Runnable
examples adds one for its optional bundles. Other lanes use their native
setup-action caches. No lane caches test results.
Cache misses, eviction or denied saves leave installation and verification
required. Shared-key save reservation collisions are safe: saves happen only
after complete installs/builds, with no partial primer job.

- Outside RBS and the lowest-allowed lanes, Ruby caches `vendor/bundle`. Absolute workflow `BUNDLE_PATH` points there
  before the action runs, with ignore-config/shared-gem settings matching
  Mise. This matters because `BUNDLE_IGNORE_CONFIG` ignores the action's local
  Bundler config. Without a committed lock the action runs `bundle lock` first,
  without deployment mode, and keys on that fresh lock. The native key includes
  full Ruby/engine, OS version, architecture, working directory, dependency
  groups and lockfile hash, so a new upstream release produces a new key. Explicit
  `cache-version: bundler-4.0.23-v1` also invalidates on an independently changed
  Bundler input or schema. `BUNDLED WITH` is already part of the lock hash.
  Same-Ruby jobs share complete bundles; native extensions cannot cross Ruby
  or OS/architecture keys. The action always runs `bundle install` before saving.
  Offline package checks require cached `.gem` files as well as installed gems.
- Go caches the actual `go env GOMODCACHE` and `GOCACHE`. Job environment sets
  these to workspace `vendor/interop/mod` and `vendor/interop/build` before
  setup-go, matching the task environment. Its key includes OS, Linux image
  family (`ImageOS`), architecture, exact Go and `interop/go.sum` hash. Both Ruby jobs may share it;
  Go's content-addressed build cache handles source changes. The post action
  saves after a successful job; module verification and readonly builds always
  run. Unused global Go caches are not archived.
- Mise caches its data directory, including its binary and installed tools.
  Static installs only `actionlint zizmor`; conformance installs only the exact
  runner/protoc backends. Package installs the exact protoc 36.2/Buf 1.75.0
  set for regeneration parity/runtime and installed-plugin checks. Specs and
  Interop disable Mise installation and caching. Buf cache is project-local
  under ignored `vendor/buf/cache`.
  `install: true` runs even after restores and saves only after all requested
  installs complete. The default key with prefix `connectrpc-ci-tools-v1`
  includes platform/runner image family (`ImageOS`), Mise version, `MISE_ENV`,
  normalized tool set and config/lock hashes, including `mise.ci.toml`. Its default 30-day period
  bounds cache reuse even without configuration changes. Distinct tool sets
  prevent an incomplete static subset winning the conformance cache key.
  Locked checksums govern fresh downloads; restored extracted tools are trusted
  cache contents, not freshly checksum-verified archives.

`ImageOS` identifies the image family, such as `ubuntu24`. A new weekly
`ImageVersion` within that family does not itself invalidate these Go or Mise
cache keys.

These behaviors are grounded in the pinned action sources:
[ruby bundle install/key](https://github.com/ruby/setup-ruby/blob/7f797815d8adceb1c412e7abefdd1ab4abc47a5f/bundler.js),
[Go cache paths](https://github.com/actions/setup-go/blob/b7ad1dad31e06c5925ef5d2fc7ad053ef454303e/src/package-managers.ts),
[Go key](https://github.com/actions/setup-go/blob/b7ad1dad31e06c5925ef5d2fc7ad053ef454303e/src/cache-restore.ts),
[Mise install/save/key](https://github.com/jdx/mise-action/blob/82a612c5ab8bcda7cc5db6b7e81a8e7584c3dc6e/src/index.ts),
and [Mise config hashing](https://github.com/jdx/mise-action/blob/82a612c5ab8bcda7cc5db6b7e81a8e7584c3dc6e/src/config-hash.ts).

## Timing evidence

Measure the first hosted run for new keys, then one rerun on the same head for
warm evidence. Record run/attempt, actual checkout SHA/base, queue latency,
job/setup/check/cache-save times, runtime paths, cache hits/misses, runner image
and CPU count. The first run's setup timings count even if a later check fails:
Ruby or Mise may already have saved valid caches. A later attempt is not cold
for those keys. PR caches are merge-ref scoped; a future first `main` run can
be cold, and other PRs reuse main caches rather than this PR's caches directly.

Historical private-repository jobs took 459s/303s, including 401s/264s setup.
The repository is now public, so runner hardware allocation can differ.
Compare observations with that qualification; do not attribute the entire
historical difference to the workflow. Local probes establish task/runtime
contracts, not hosted action success, cache hits or a guaranteed speedup.
Original project work is MIT licensed. The invalid gem push guard stays in place;
publishing, releasing or deploying requires separate, explicit authorization.

## Optional Rails lanes

Rails uses the same official pinned Ruby/Bundler action and root ABI-specific
`vendor/bundle` cache. The setup-ruby step selects its generated Appraisal
Gemfile explicitly, so each lane key includes the lane lock the action resolves.
Mise then propagates `RAILS_LANE` deliberately through the launcher, asserts
actual Rails in the specs, and preserves action-provided runtimes with the
existing CI guard. Rails setup depends on `setup:bundler`, avoiding root
development-bundle installation in those lanes. `BUNDLE_COOLDOWN=3` is effective
in setup-ruby and every adapter task. CI resolves fresh lane locks and executes
actual `appraisal <lane>` commands, never `appraisal install`. The nine core
checks retain their commands and evidence scope; no aggregate verify job is
added. Local Appraisal generation and package checks do not prove hosted cache
behavior or clean exact-head CI success.

## Optional HTTP/2 lanes

Two independent Ruby matrix jobs add four checks, preserving the nine core and
three Rails lanes explicitly. Backend specs/package and independent connect-go
interop run on both exact Rubies. They reuse the pinned official Ruby/Bundler
and Go actions, with lock-keyed native backend caches under
`vendor/async/bundle` and the existing pinned module/build caches. Their selected
Gemfile is `gems/connectrpc-async/Gemfile`; Bundler user state is separate under
`vendor/async/bundler`, and the three-day cooldown applies.
Core setup/spec jobs install no backend dependencies. Backend jobs never reach
root `setup`: `async:interop:grpc`, `async:interop:keepalive` and
`falcon:interop:grpc` run the official gRPC peer generation check through their own backend bundle instead of root
`grpc:generation:check`, so the job's `BUNDLE_GEMFILE` is never installed into
the root `vendor/bundle`.

`async:tools` has a complete CI override preserving its task environment and
working directory while replacing local runtime preparation with exact
runtime/PATH/config validation. It reuses the unchanged root runtime guard in a
separate process and checks the actual backend bundle/Gemfile itself. Task-local
Go activation stays on `async:interop`; its command runs directly without a
nested Mise execution dropping Go. CI still disables exec auto-install and
rejects tracked/unexpected untracked changes after every lane. The sole observed
backend `.bundle/` output from local Bundler configuration is ignored, matching
the setup-ruby behavior without hiding Gemfiles or locks. Local guards and
product/Go/package passes do not prove hosted cache behavior or exact-head CI.

## Optional Rack lanes

The Rack matrix adds two independent checks while preserving the original 16.
It selects `gems/connectrpc-rack/Gemfile`, `vendor/rack/bundle` and
`vendor/rack/bundler` before setup-ruby, with the existing exact Ruby/Bundler,
ignore-config settings and three-day cooldown. No Go or compiler tool is needed.
`rack:tools` validates actual middleware Gemfile, bundle path, user home and
working directory, then reuses the unchanged root runtime/PATH guard. Its CI
replacement retains the whole local task context. Specs, lint and normal
offline installed-package calls run on both Rubies, including source-owner
assertions. Rack verification stays separate from root `verify`; local handoff also runs
`mise run rack:verify` for the moved middleware coverage.


## Optional in-process testing lanes

The [testing package](../gems/connectrpc-testing/README.md) has a separate
bundle and native cache under `vendor/testing/bundle`, with Bundler
user state under `vendor/testing/bundler`. Both Ruby lanes run package syntax,
lint, behavior specs, the generated example and isolated offline installation.
The complete `testing:tools` CI override validates the action runtime and actual
testing Gemfile/cache directly, without assuming the root bundle path. Testing verification stays separate from root
`verify`. These calls do not provide network or
independent-peer interoperability evidence.

## Optional RBS lanes

The independent RBS matrix adds two checks, one per exact Ruby. Each runs real
protoc/Buf service RBS generation, unchanged official message RBS, strict
positive and per-file asserted negative Steep consumers, and installed
gem/executable discovery outside the checkout. Root runtime-generation fixtures
remain under the existing Package job. The RBS tools use a separate frozen
checksummed `typecheck/Gemfile.lock` and worktree-local `vendor/rbs/` bundle,
Bundler state, caches and per-Ruby package outputs. No additional runtime
dependency enters core, Rails or Async bundles.

The pinned Ruby/Bundler action prepares the runtime with its bundle cache disabled
only in this lane. One explicit `actions/cache` v6.1.0 step, pinned to
`55cc8345863c7cc4c66a329aec7e433d2d1c52a9`, owns `vendor/bundle` and the independent
`vendor/{rbs,rack,testing,async,falcon,otel,prometheus,yabeda,health,sentry}/bundle` directories. Rails Appraisal
needs the root bundle directory. There are no overlapping bundle-cache owners or
bundle-specific setup-action environment overrides. The action release was
published on 2026-06-26, satisfying the seven-day age policy.

Its key includes OS, architecture, exact Ruby, Bundler 4.0.23, actual runner
`ImageOS`/`ImageVersion`, every affected Gemfile, committed tool lock and
gemspec, and a versioned cache schema. Own-gem locks are not committed, so a
restored bundle can predate newer upstream releases; it only seeds the install.
There are no fallback restore keys or cross-OS restores. Weekly
image changes invalidate native extensions too. Restored extracted gems are
trusted cache contents; bundle installs still run unconditionally and resolve
fresh.
Misses and eviction require normal setup, and a successful job saves a complete
bundle union. Generated files, diagnostic results, Bundler configuration and the
fresh external GEM_HOME are never cached.

The existing pinned Mise action also caches the exact protoc/Buf tool set used
by Package, under the same platform/tool/config/lock invalidation rules above.
Each source and installed negative set shares one fresh single-worker Steep
invocation while requiring the same exact diagnostic for every separate file.
All positive, strict source, inventory, RBS validation, Falcon-only, normal
installed discovery and runtime checks still execute on cache hits. The
[workflow](../.github/workflows/ci.yml) defines the job matrix. These checks remain
independent of aggregate local `verify`. See the [RBS guide](rbs.md) for parser
guards and proof boundaries.

The cache behavior is grounded in the pinned action's
[restore implementation](https://github.com/actions/cache/blob/55cc8345863c7cc4c66a329aec7e433d2d1c52a9/src/restoreImpl.ts),
[save implementation](https://github.com/actions/cache/blob/55cc8345863c7cc4c66a329aec7e433d2d1c52a9/src/saveImpl.ts)
and [successful-job post condition](https://github.com/actions/cache/blob/55cc8345863c7cc4c66a329aec7e433d2d1c52a9/action.yml).

## Optional Falcon lanes

The two Ruby lanes run separate Falcon specs/package and Go checks. They
select `gems/connectrpc-falcon/Gemfile`, `vendor/falcon/bundle` and
`vendor/falcon/bundler` before setup-ruby. The pinned Ruby/Bundler actions
and three-day cooldown apply independently of core, Rack,
Rails and Async bundles. `falcon:tools` retains its complete task environment
and directory in CI while validating the action runtime and actual bundle.

Specs/package checks exercise native requests, TLS/provider lifecycle, real
process workers, Rack/Rails fallback and isolated installed service hosting.
Separate Go jobs use the existing pinned toolchain/module caches and run the
independent peer against actual Falcon workers. Every lane checks workspace
cleanliness. Local verification does not establish hosted exact-head success.

## Optional metrics lanes

Prometheus and Yabeda each run one `<name>:verify` leaf per pinned Ruby. Like
OTel, each selects its own `gems/connectrpc-<name>/Gemfile`, `vendor/<name>/bundle`
and `vendor/<name>/bundler` before setup-ruby, resolves fresh within the gemspec
constraints under the three-day cooldown, and validates the action runtime
through `<name>:tools`. `floors:prometheus` and `floors:yabeda` cover their
lowest allowed versions.
The Prometheus lane includes real Rack, Async and Falcon hosts and a forked
`DirectFileStore` check; the Yabeda lane records through `yabeda/testing`. Both
install their archives offline into a fresh external GEM_HOME and check
workspace cleanliness.

## Optional Sentry lane

The `sentry` job runs `sentry:verify` once per pinned Ruby with its own
`gems/connectrpc-sentry/Gemfile`, `vendor/sentry/bundle` and
`vendor/sentry/bundler`, validated through `sentry:tools` like the OTel and
metrics lanes. It runs the real SDK with an in-memory transport against
sentry-rails, Rack, native Async/Falcon h2c and ThreadedTransport, installs the
archive offline into a fresh external GEM_HOME and checks workspace
cleanliness. `floors:sentry` covers sentry-ruby 6.7.0.

## Public API typing gates

The two RBS jobs run strict implementation checks for the core files and private
state listed in `typecheck/source.rb`. They validate that source list, enforce
the reviewed source/signature inventory, and check application callers for all
eleven packages. Generated positive, exact per-file negative and nominal/bridge
checks remain.
Installed checks use normal RBS discovery in a fresh external GEM_HOME, validate
each optional dependency closure and both composition orders, and exercise API-only
OTel without loading an SDK. Scoped optional setups supply offline archives,
and RubyGems resolves the installed closure from the running Ruby ABI's caches;
no tool pin, runtime dependency or job/matrix axis is added. The
[workflow](../.github/workflows/ci.yml) defines the complete job matrix. Local
`rbs:proof` verifies representative source and drift failures in an independent
capture. A passing consumer alone does not establish strict source checking or
backend interoperability. See the [RBS guide](rbs.md).

## Native streaming leaves

The existing Async and Falcon specs/package jobs additionally run exact named
native official profiles on each pinned Ruby. Async owns the native client once
and its server profiles; Falcon runs separate server profiles. Their Mise action
always installs the already pinned runner/protoc pair and caches only tools under
the existing platform/image/config/lock key scheme. Existing check names, matrix
axes, bundle setup and clean-workspace assertions remain unchanged.

Both Falcon specs/package lanes also run `falcon:conformance:http1` after the
existing conformance tasks. The required H1 server identity/gzip profiles retain
their strict inventories: 174 identity and 160 gzip cases must all pass,
including the original oversized CS assertions. There is no allowed-failure
setting. Production local runs pass both profiles on both pinned Rubies using
the cooperative total 500 ms final-close policy, with unchanged 100 ms / 256 KiB
discard caps. Local results do not establish clean-environment or exact-head CI
success. Historical Web failures were diagnosed as an early-head/upload race
and addressed by the separate baseline repair #42. The integrated branch must
still pass both strict hosted Web lanes, with the Web publication gate excluded
from Connect response-first retirement.
See the [H1 conformance status](../conformance/README.md#connect-http11-server-profiles).

The existing backend Go jobs additionally run `async:interop:streaming` and
`falcon:interop:streaming`. The latter exercises real process workers. Original
unary identity/gzip, GET, generated, native cancellation and typed tail-drain
commands remain. OTel jobs collect the SDK streaming tests through their existing
`otel:verify`; RBS jobs retain strict sources, batched exact per-file negatives,
ABI/platform/image cache keys, unconditional installs and fresh external consumers.
No result cache or known-failure selection is introduced. See
[development](development.md#native-streaming-verification) for exact evidence
ownership and the [streaming guide](streaming.md) for completion policies.

The two Browser gRPC-Web leaves also receive Go 1.27.3 from the pinned
`setup-go` action. Their reference task validates the action-owned runtime,
`browser/reference/go.mod`, and `vendor/browser/go/{mod,build}` cache paths
before downloading, verifying and building the independent reference. The local
`browser:reference:setup` task explicitly installs locked Mise Go instead.
Both task definitions retain `GOTOOLCHAIN=local`, `GOWORK=off` and
`GOFLAGS=-mod=readonly`; CI does not install Go through Mise.

## Runnable examples

One check on Ruby 4.0.9 runs `example:verify`. It starts each
[runnable example](../examples/README.md) server on an ephemeral plaintext port,
runs every supported client, RPC and option combination against it plus
representative refusals, and stops it with INT or TERM. The Async and Falcon
servers then run again with `--mtls` and generated throwaway certificates for a
representative set of calls, and renew those certificates to check that each
server presents the new one without a restart. Every client and
server process has a bounded deadline with TERM then KILL escalation. The
setup-ruby cache covers the root `vendor/bundle`; the selected Rails 8.1 lane
installs its remaining gems into that path on each run. An explicit cache keyed
by the Ruby ABI/platform/image (`rbs:cache-platform`) and the Rack, Async and
Falcon Gemfiles and gemspecs restores `vendor/{rack,async,falcon}/bundle`; the
example setups still install fresh resolutions. Existing example
tasks keep their current jobs.

## Release workflows

The separate `release-please.yml` workflow runs on pushes to main and manual dispatch.
It opens or updates the release PR using an existing GitHub App from the
`release-please` environment. After a release PR merges, it tags the commit and
creates a draft GitHub release. The tag starts `release.yml`, which publishes
from the `release` environment.
The 48 CI checks above retain their mapping. `check:static` also runs the offline
`release:check` invariant for all eleven versions, release configuration and
installation guides. See [release preparation](releasing.md) for the candidate procedure and
remaining App/account setup.

## Optional health lane

The health service runs one `health:verify` leaf per pinned Ruby. It selects
`gems/connectrpc-health/Gemfile`, `vendor/health/bundle` and
`vendor/health/bundler` before setup-ruby and validates the action runtime
through `health:tools`. The pinned setup-go action provides Go 1.27.3 for
`health:interop`, whose task keeps the grpc-go module and build caches under
`vendor/grpc`. The leaf runs registry and real Rack, Async and Falcon host specs,
the offline installed-package check and grpc-go's standard health client
against the Async Host and real Falcon workers over h2c, TLS and mTLS, plus the
reference-server control. `floors:health` covers google-protobuf at its floor.
