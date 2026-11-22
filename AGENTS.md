# Project map

This is a public, unofficial typed Connect unary Ruby gem. `lib/connectrpc.rb`
is the host-neutral core entry point; Google protobuf and Rack are explicit
optional integration requires. `gems/connectrpc-rack/` exclusively owns the
`connectrpc/rack` entry point and middleware specs, independent bundle and
`rack:` tasks. Core has no middleware source shim. `lib/connectrpc/` holds the
core, bindings,
transports and explicit optional generator. `exe/protoc-gen-connectrpc-ruby` is
the standard protoc/Buf plugin. `examples/` owns ephemeral localhost hosting
and the two-terminal runnable servers/clients, and `interop/` the pinned independent Go peer.
`connectrpc.gemspec` owns core package metadata and files. The optional
`gems/connectrpc-on-rails/` package owns native Rails controller execution and its
Appraisal lanes. The optional `gems/connectrpc-async/` package owns the standalone HTTP/2 host,
transport, independent bundle and `async:` tasks. The optional `gems/connectrpc-falcon/` package composes the shared Async native
application, TLS manager and server session with real Falcon workers and a local
HTTP/1 resource. It owns its independent `falcon:` tasks and bundle. Core
setup/test remain independent. Use `RAILS_LANE=8.0` or
`8.1` with `mise run test:rails` / `verify:rails`; every Rails bundle process
uses `BUNDLE_COOLDOWN=3` and the root `vendor/bundle` cache.
Core specs live in `spec/`; `Rakefile` and `scripts/` implement tooling behind
`mise.toml`.

Use `mise tasks` to discover commands. Start with `mise run setup`.
For a Ruby change, run focused specs with `mise run test -- spec/connectrpc_spec.rb`
(or a `file:line` selector), then `mise run check`. For generator changes, also
run `mise run generation:check` (pinned protoc/Buf).
Keep official `_pb.rb` and generated `_connect.rb` fixtures reproducible; regenerate
with `mise run generation:build`, never hand-edit them. Use `mise run format` for
Layout fixes. Run `mise run verify` followed by `mise run rack:verify` for
package/tooling changes and final implementation handoff. These include tests,
build, and isolated gem installation.
[Development](docs/development.md) owns setup, verification tiers, coverage,
updates, hooks, and PR guidance. Docs-only changes need link/command review;
tests and workflow checks apply only if the edit affects those commands or examples.

Before protocol, adapter, message-binding, or public-API work, read
[design](docs/design.md). Keep shared protocol code independent of HTTP hosting
and concrete protobuf classes. Keep future streaming and production
host work outside unary maintenance. Rails execution belongs in the optional gem;
keep generated files outside Zeitwerk-managed paths and all hooks/handlers
private. HTTP/2 ownership, TLS snapshots and cancellation wrappers stay in the
optional Async package; read its README before changing those source-coupled paths. Service generation composes existing unary metadata/client/handler APIs. Reject unsupported transport capabilities explicitly.

Use RSpec for observable behavior. Choose tests for concrete success, failure,
boundary, and regression risks; do not treat a green smoke suite or coverage
percentage as interoperability evidence. Start SimpleCov before requiring the
library and keep all `lib/**/*.rb` in scope. For asynchronous work, synchronize
on observable progress and bounded completion; do not use fixed sleeps to
assume a race has started or an operation has finished.

Keep runtime dependencies deliberate and package checks isolated. Own-gem
development locks are ignored: bundles resolve the newest allowed versions, and
`CONNECTRPC_DEPENDENCY_FLOORS=1` (`floors:*` tasks) proves gemspec floors. Follow
the constraint policy in [development](docs/development.md) before changing a
gemspec requirement; only tool Gemfile pins stay exact. The non-gating
`canary:*` tasks (daily `canary.yml`) run the Async/Falcon suites with the
remaining patch pins lifted; a green run is the evidence for bumping a pin.
Original project work is MIT licensed. Gems pin `allowed_push_host` to
`https://rubygems.org` and require MFA; only the tag-triggered
`.github/workflows/release.yml` publishes (see [releasing](docs/releasing.md)).
Do not merge release PRs, push `v*` tags, run or dispatch the release workflow,
or otherwise publish, release, or deploy without separate, explicit
authorization. A successful build or verify command does not authorize
publication. `release/` holds the release job's frozen Sigstore bundle.

Optional HTTP/2 development uses `mise run async:setup`, then `async:test`,
`async:check` and `async:verify`. It has a separate Gemfile and ignored lock,
`vendor/async` bundles/package outputs and `coverage/async` reports. Do not add
backend dependencies to core or Rails bundles. Both Ruby lanes must run actual
h2c/TLS/mTLS and independent Go checks. Cancellation tests must establish real
wire progress; a stuck frame deliberately fails its connection and siblings.
Backend package checks build their own core archive to avoid root/Rails races.

Optional Falcon development uses `mise run falcon:setup`, then `falcon:test`,
`falcon:check` and `falcon:verify`. Its separate lock, caches/package outputs and
coverage live under `vendor/falcon` and `coverage/falcon`. Keep RPC outside
Falcon encoding/cache middleware; only explicit Rack fallback receives it.
Worker providers compile after prepare and before accepts. Keep production
lifecycle hooks independent of test-only event/control plumbing.

Optional tracing lives in `gems/connectrpc-otel/`; read its README before changing
span conventions, propagation or host completion. Core owns the fixed neutral
lifecycle and has no OTel dependency. The separate `otel:` bundle owns real
SDK-backed cross-package specs and packages, using `vendor/otel` and
`coverage/otel`. Preserve independent admission, processing and delivery facts;
never infer HTTP/2 completion from `send_response` return or Rack body close.

Optional metrics: core's `lib/connectrpc/metrics/` (loaded only by
`require "connectrpc/metrics"`) owns the metric set, labels and completion
classification; `gems/connectrpc-prometheus/` and `gems/connectrpc-yabeda/` are
thin sinks with independent `prometheus:` / `yabeda:` bundles under
`vendor/<name>` and `coverage/<name>`. Read
[the plan](docs/plans/metrics.md) and the Prometheus README before changing names,
labels, buckets or message-callback byte semantics. Keep yabeda-prometheus out of
every bundle (its prometheus-client pin splits the shared version map).

Optional health: `gems/connectrpc-health/` owns `grpc.health.v1` (registry,
Check/List/Watch) with its `health:` bundle under `vendor/health`. Its vendored
grpc-proto schema, generated `lib/grpc` and opt-in `rbs/grpc_health_v1` signature
bridge come only from `generation:build`; read [the plan](docs/plans/health.md)
and its README before changing Watch concurrency, shutdown or the grpc-go
`health:interop` script.

Optional native Sentry tracing lives in `gems/connectrpc-sentry/`; read its README
and [the plan](docs/plans/sentry.md) before changing the nest-or-own lifetime,
promotion, scope handling, propagation or exception capture. Inside
`ConnectRPC::Sentry` always write `::Sentry`. The separate `sentry:` bundle uses
`vendor/sentry` and `coverage/sentry`; specs run a real SDK with
`Sentry::DummyTransport` and a `.invalid` DSN, never a real DSN or network sends.
Fiber hosts require `hub_isolation_level = :fiber`; keep the fail-closed check at
start and every attach.

Optional reactor-free client transport: `gems/connectrpc-hyper/` (Rust extension
on hyper/h2/rustls/tokio via magnus and rb-sys). Read its README and
[the plan](docs/plans/hyper.md) before changing waits, interrupts, fork,
pooling, retries, TLS or proxies; keep unsafe FFI small with its invariants
commented. `hyper:*` tasks use `vendor/hyper` and `coverage/hyper` with Rust
1.98.0 (MSRV 1.85, `hyper:msrv`), libclang and a C compiler; specs need the Go
peer (`hyper:peer`, Go 1.27.3). Commit `Cargo.lock` and build `--locked`; crate
updates use `hyper:cargo:update` and the 7-day age rule, and regenerate
`THIRD_PARTY_NOTICES.md` with `scripts/notices.rb`. GOAWAY retries depend on
hyper and h2 internals `wire.rs` documents; `retry_spec.rb`'s stream-id and
waiting-HEADERS specs fail when an update breaks them. `hyper:cross` builds one
platform gem with rb-sys-dock in a disposable copy (digest-pinned images,
`HYPER_CROSS_TWICE=1` for the reproducibility check).
`.github/workflows/hyper-gems.yml` builds and smokes all nine archives for CI
and the release; `ReleaseTrain::PLATFORMS` makes the candidate check them.

Public API changes must update the owning gem's signatures, the supported/SPI
inventory and focused caller evidence. Read [RBS guidance](docs/rbs.md) before
changing those surfaces; run `rbs:check` for consumers, inventory and strict core
implementation checking, and `rbs:package` for installed discovery. The
`rbs:inventory` snapshot (`typecheck/surface.json`) also fails on added, removed
or re-signatured private lib methods, so lib refactors need `mise run rbs:check`
too. Framework compatibility declarations outside `sig/` remain explicit
opt-ins.
