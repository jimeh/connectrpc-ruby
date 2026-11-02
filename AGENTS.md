# Project map

This is a private, unofficial Ruby gem bootstrap. `lib/connectrpc.rb` is the
public require entry point; `lib/connectrpc/version.rb` is the only API.
`connectrpc.gemspec` owns package metadata and files. Specs live in `spec/`;
`Rakefile` and `scripts/` implement tooling behind `mise.toml`.

Use `mise tasks` to discover commands. Start with `mise run setup`.
For a Ruby change, run focused specs with `mise run test -- spec/connectrpc_spec.rb`
(or a `file:line` selector), then `mise run check`. Use `mise run format` for
Layout fixes. Run `mise run verify` for package/tooling changes and final
implementation handoff; it includes tests, build, and isolated gem installation.
[Development](docs/development.md) owns setup, verification tiers, coverage,
updates, hooks, and PR guidance. Docs-only changes need link/command review;
tests and workflow checks apply only if the edit affects those commands or examples.

Before protocol, adapter, message-binding, or public-API work, read
[design](docs/design.md). Keep shared protocol code independent of HTTP hosting
and concrete protobuf classes. Do not implement the proposed milestone as part
of bootstrap maintenance. Reject unsupported transport capabilities explicitly.

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
