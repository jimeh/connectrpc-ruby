# Project map

This is a public, unofficial typed Connect unary Ruby gem. `lib/connectrpc.rb`
is the host-neutral core entry point; Google protobuf and Rack are explicit
optional integration requires. `gems/connectrpc-rack/` exclusively owns the
`connectrpc/rack` entry point and middleware specs, independent bundle and
`rack:` tasks. Core has no middleware source shim. `lib/connectrpc/` holds the
core, bindings,
transports and explicit optional generator. `exe/protoc-gen-connectrpc-ruby` is
the standard protoc/Buf plugin. `examples/` owns ephemeral localhost hosting
and `interop/` the pinned independent Go peer.
`connectrpc.gemspec` owns core package metadata and files. The optional
`gems/connectrpc-rails/` package owns native Rails controller execution and its
Appraisal lanes. The optional `gems/connectrpc-async/` package owns the standalone HTTP/2 host,
transport, independent bundle and `async:` tasks. The optional `gems/connectrpc-falcon/` package composes the shared Async native
application, TLS manager and server session with real Falcon workers and a local
HTTP/1 resource. It owns its independent `falcon:` tasks and bundle. Core
setup/test remain independent. Use `RAILS_LANE=8.0` or
`8.1` with `mise run test:rails` / `verify:rails`; every Rails bundle process
uses `BUNDLE_COOLDOWN=7` and the root `vendor/bundle` cache.
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

Keep runtime dependencies deliberate and package checks isolated. Original
project work is MIT licensed; `allowed_push_host` intentionally points to
`https://gems.invalid`.
Do not remove that guard or publish, release, or deploy without separate,
explicit authorization. A successful build or verify command does not authorize
publication.

Optional HTTP/2 development uses `mise run async:setup`, then `async:test`,
`async:check` and `async:verify`. It has a separate checksummed Gemfile/lock,
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

Public API changes must update the owning gem's signatures, the supported/SPI
inventory and focused caller evidence. Read [RBS guidance](docs/rbs.md) before
changing those surfaces; run `rbs:check` for consumers, inventory and strict core
implementation checking, and `rbs:package` for installed discovery. Framework
compatibility declarations outside `sig/` remain explicit opt-ins.
