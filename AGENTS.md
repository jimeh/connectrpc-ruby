# Project map

This is a public, unofficial typed Connect unary Ruby gem. `lib/connectrpc.rb`
is the host-neutral core entry point; Google protobuf and Rack are explicit
optional integration requires. `lib/connectrpc/` holds the core, bindings,
transports and explicit optional generator. `exe/protoc-gen-connectrpc-ruby` is
the standard protoc/Buf plugin. `examples/` owns ephemeral localhost hosting
and `interop/` the pinned independent Go peer.
`connectrpc.gemspec` owns core package metadata and files. The optional
`gems/connectrpc-rails/` package owns native Rails controller execution and its
Appraisal lanes. The optional `gems/connectrpc-async/` package owns the standalone HTTP/2 host,
transport, independent bundle and `async:` tasks. Core setup/test remain independent. Use `RAILS_LANE=8.0` or
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
Layout fixes. Run `mise run verify` for package/tooling changes and final
implementation handoff; it includes tests, build, and isolated gem installation.
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

Keep runtime dependencies deliberate and package checks isolated. Licensing is
undecided, and `allowed_push_host` intentionally points to `https://gems.invalid`.
Do not remove that guard or publish, release, or deploy without separate,
explicit authorization and a licensing/publication decision. A successful build
or verify command does not authorize publication.

Optional HTTP/2 development uses `mise run async:setup`, then `async:test`,
`async:check` and `async:verify`. It has a separate checksummed Gemfile/lock,
`vendor/async` bundles/package outputs and `coverage/async` reports. Do not add
backend dependencies to core or Rails bundles. Both Ruby lanes must run actual
h2c/TLS/mTLS and independent Go checks. Cancellation tests must establish real
wire progress; a stuck frame deliberately fails its connection and siblings.
Backend package checks build their own core archive to avoid root/Rails races.
