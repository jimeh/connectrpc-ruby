# CI

The workflow runs on pull requests and pushes to `main`, on Ubuntu 24.04.
It has nine independent checks, with no serial cache primer or aggregate job:

| Check | Ruby | Canonical commands |
| --- | --- | --- |
| Static | 3.4.12 | `mise run check:static` |
| Specs | 3.4.12, 4.0.9 | `mise run syntax`, then a separate `mise run test` step |
| Package | 3.4.12, 4.0.9 | `mise run generation:check`, then `mise run check:package` |
| Go interoperability | 3.4.12, 4.0.9 | `mise run fixture:check`, then `mise run test:interop` |
| Official conformance | 3.4.12, 4.0.9 | `mise run test:conformance` |

Each matrix has `fail-fast: false`. Every check runs its commands on cache
hits too. Package isolation still installs cached `.gem` artifacts offline and
exercises default JSON and Net::HTTP. Interop still verifies modules and builds
with readonly dependencies before its 18 original cases and separate generated
Ruby path. Conformance still regenerates
descriptors and executes 94 client cases (92 passes, two exact expected
failures) and 88 server cases. Product, generator and harness specs remain
part of Specs; CI guard specs cover task and runtime boundaries.

## Runtime boundary

CI pins `ruby/setup-ruby` v1.335.0 and `actions/setup-go` v7.0.0 by full commit,
with exact Ruby 3.4.12/4.0.9, Bundler 4.0.23 and Go 1.27.3. These official
actions use prebuilt runtimes instead of compiling Ruby in each job. Go is
installed only in interoperability jobs. The action runtime downloads trust
upstream HTTPS/release assets. Observed archive digests are audit evidence,
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

There is no separate `actions/cache` step and no cached test result.
Cache misses, eviction or denied saves leave installation and verification
required. Shared-key save reservation collisions are safe: saves happen only
after complete installs/builds, with no partial primer job.

- Ruby caches `vendor/bundle`. Absolute workflow `BUNDLE_PATH` points there
  before the action runs, with frozen/ignore-config/shared-gem settings matching
  Mise. This matters because `BUNDLE_IGNORE_CONFIG` ignores the action's local
  Bundler config. The native key includes full Ruby/engine, OS version,
  architecture, working directory, dependency groups and lockfile hash. Explicit
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
Licensing remains undecided and the invalid gem push guard stays in place.
