# Official unary conformance tooling

This directory is test infrastructure and is excluded from the gem. The adapters
exercise the real `ConnectRPC::Client`/NetHTTP and `Server`/Rack implementations
on ephemeral loopback HTTP/1.1. `profile.yaml` explicitly selects Connect unary
POST, identity, proto and JSON, and message receive limits. It disables TLS,
client certificates, H2C, HTTP trailers, GET and half-duplex. This plaintext
host profile does not qualify the library's TLS behavior. Connect unary
`trailer-` metadata is still tested.

```sh
mise run setup:conformance
mise run conformance:check
mise run test:conformance
mise run test:conformance -- server
```

For troubleshooting, append one exact name from [cases.json](cases.json), quoted
as a single argument, after `client` or `server`. Full verification and CI supply
no selector. No skips, flakes or wildcards are enabled. The two exact
caller-cancellation cases use [known-failing-client.txt](known-failing-client.txt).

## Pins and provenance

Schemas are unchanged copies from
[connectrpc/conformance v1.0.6](https://github.com/connectrpc/conformance/tree/eaa97669854d500691a393033d15950895d6aff7/proto/connectrpc/conformance/v1)
(commit `eaa97669854d500691a393033d15950895d6aff7`). Their upstream Apache-2.0
[license](LICENSE) applies to those files, separately from the undecided gem
license. [schema-sha256.json](schema-sha256.json) records every copied schema.

Mise uses explicit task-local `github:connectrpc/conformance@1.0.6` and
`aqua:protocolbuffers/protobuf/protoc@36.2` backends. The Linux x64 archives have
these SHA256 digests in [mise.lock](../mise.lock):

- Runner: `803a9ed1778583f9b98f44b4b77ab06897d1027183ef2064de32a0fb7d688ab0`.
- protoc: `121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278`.

The runner release was published on 2026-10-14 and protoc on 2026-09-17; both
were over seven days old when pinned on 2026-11-02. No Ruby gems or Go modules
are added for this runner. Ordinary setup and focused Ruby specs do not install
these task-local tools. Locked setup works with `MISE_LOCKED=1`.

`mise run conformance:build` invokes official protoc with `--include_imports`,
`--proto_path=conformance/proto`, and ordered inputs `config.proto`,
`service.proto`, `client_compat.proto`, `server_compat.proto` under
`connectrpc/conformance/v1/`. protoc uses its bundled standard includes.
`conformance:check` checks copied schema hashes and byte-for-byte regeneration
of [descriptor.bin](descriptor.bin). The initial descriptor SHA256 is
`0da3f361ceee99592fbc82a3115c42538af113ee688d37b19f49af5e3bb6ab96`.
The loader registers each official FileDescriptorProto in dependency order in
Google's generated pool, after requiring standard Any, Empty, Struct and
descriptor types. It preserves optional presence and native nested Any JSON.

## Execution and accounting

The pinned runner discovers 94 configured client cases and 88 server cases.
[cases.json](cases.json) freezes the complete observed names, including timeout,
duplicate metadata and remote `canceled` error cases. These are configured and
executed counts, not totals for unsupported protocol/configuration axes. The
independent [Go fixture](../interop/) remains 18 cases and is separate evidence.

Each run preserves `runner.log`, `telemetry.jsonl` and `summary.json` beneath
ignored `vendor/conformance/results/`, with Ruby version and direction in the
run directory name. Raw `--vv` logs retain complete assertion blocks. The
version-specific ledger requires exact scheduled, response, actual dispatch
and completion sets, and checks totals, duplicates, missing outcomes, setup
errors, could-not-run cases, unknown output and runner exit status. The client
records receipt, actual NetHTTP exchange, completion and emitted framed result.
The server records actual Rack requests, completed HTTP responses, readiness,
stdin EOF and main-thread shutdown. The child command uses `RbConfig.ruby` and
the inherited Bundler environment; Mise output never enters protobuf framing.

The server stays alive after configuration EOF and exits on SIGTERM. A self-pipe
wakes the main loop, which closes the localhost host with bounded cleanup.
Framing uses four-byte big-endian sizes, exact reads, flushes, a 16 MiB maximum,
and explicit malformed/truncated errors. Protocol stdout contains frames only.

## Explicit caller-cancellation limitation

There is no public caller-cancellation API. The two expanded client
`Client Cancellation/.../unary/cancel-after-close-send` instructions request
cancellation after 5 ms against a response delayed 200 ms. The adapter records
that the instruction is unsupported, executes the real uncanceled RPC normally,
and returns its successful payload and metadata. It never synthesizes a canceled
result or substitutes a deadline.

Both cases must fail with these exact assertions:

```text
expecting an error but received none
expecting 0 response messages but instead got 1
```

The observed full run is 92 passed plus two expected failures for the client,
and 88 passed with zero failures for the server. The two unsupported caller instructions are still
dispatched and executed; they are not passes. The ledger verifies their actual
success, echoed request, absence of timeout, payload, headers/trailers and exact
failure reason. The exact-name `--known-failing` manifest requires these cases
to fail, and the ledger independently rejects any different assertion or result.
An unexpected pass, missing case, setup error or unrelated failure fails
verification. Every timeout/deadline case remains a
must-pass. This profile does not establish full Connect conformance.
