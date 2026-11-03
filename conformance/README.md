# Official unary conformance tooling

This directory is test infrastructure and is excluded from the gem. The adapters
exercise the real `ConnectRPC::Client`/NetHTTP and `Server`/Rack implementations
on ephemeral loopback HTTP/1.1. `profile.yaml` explicitly selects Connect unary
POST and safe unary GET, identity, proto and JSON, and message receive limits. It disables TLS,
client certificates, H2C, HTTP trailers and half-duplex.
`profile-gzip.yaml` adds a separately executed gzip-only profile with the same axes. This plaintext
host profile does not qualify the library's TLS behavior. Connect unary
`trailer-` metadata is still tested.

```sh
mise run setup:conformance
mise run conformance:check
mise run test:conformance
mise run test:conformance -- server
mise run test:conformance:identity
mise run test:conformance:gzip
```

For troubleshooting, append one exact name from [cases.json](cases.json), quoted
or [cases-gzip.json](cases-gzip.json), as a single argument after `client` or
`server`. The exact inventory selects the profile automatically. Full verification and CI supply
no selector. No skips, flakes, wildcards, expected failures or known-failing
manifests are enabled.

## Pins and provenance

Schemas are unchanged copies from
[connectrpc/conformance v1.0.6](https://github.com/connectrpc/conformance/tree/eaa97669854d500691a393033d15950895d6aff7/proto/connectrpc/conformance/v1)
(commit `eaa97669854d500691a393033d15950895d6aff7`). Their upstream Apache-2.0
[license](LICENSE) applies to those files, separately from the project MIT
license. [schema-sha256.json](schema-sha256.json) records every copied schema.

Mise uses explicit task-local `github:connectrpc/conformance@1.0.6` and
`aqua:protocolbuffers/protobuf/protoc@36.2` backends. The Linux x64 archives have
these SHA256 digests in [mise.lock](../mise.lock):

- Runner: `803a9ed1778583f9b98f44b4b77ab06897d1027183ef2064de32a0fb7d688ab0`.
- protoc: `121f6c7afe1d4d0e3ea6aab9632038599250134cbf4474cb1167d2c7decd4278`.

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

The pinned runner discovers 96 configured client cases and 90 server cases.
[cases.json](cases.json) freezes the original 94/88 observed names; the ledger adds the two exact GET names per direction, including timeout,
duplicate metadata and remote `canceled` error cases. These are configured and
executed counts, not totals for unsupported protocol/configuration axes. The
independent [Go fixture](../interop/) retains its 18 identity cases and adds
40 separate gzip cases; neither is official conformance evidence. The exact
gzip inventory has 88 client and 82 server cases, disjoint from identity. Their
expanded union requires 184 actual client and 172 server passes. The dedicated
compressed-error client case has no compression/codec suffix, so membership is
never inferred from substrings.

Each run preserves `runner.log`, `telemetry.jsonl` and `summary.json` beneath
ignored `vendor/conformance/results/`, with Ruby version and direction in the
run directory name, together with profile identity. Raw `--vv` logs retain complete assertion blocks. The
version-specific ledger requires exact scheduled, response, actual dispatch
and completion sets, and checks totals, duplicates, missing outcomes, setup
errors, could-not-run cases, unknown output and runner exit status. The client
records receipt, actual NetHTTP exchange, completion and emitted framed result.
Wire observations retain coding, encoded byte counts, Content-Length when
present, gzip magic and SHA256. Failed transport reads are distinguished from
complete responses. Matching default wire caps are retained for runner receive
limits. The server records actual Rack requests, completed HTTP responses, readiness,
stdin EOF and main-thread shutdown. The child command uses `RbConfig.ruby` and
the inherited Bundler environment; Mise output never enters protobuf framing.

The server stays alive after configuration EOF and exits on SIGTERM. A self-pipe
wakes the main loop, which closes the localhost host with bounded cleanup.
Framing uses four-byte big-endian sizes, exact reads, flushes, a 16 MiB maximum,
and explicit malformed/truncated errors. Protocol stdout contains frames only.

## Explicit caller cancellation

All four `Client Cancellation/.../unary/cancel-after-close-send` cases must pass
unwaived. The adapter arms the original 5 ms timer before invoking the blocking
Client call, passes a real `CancellationToken`, keeps `timeout: nil`, and leaves
the original 200 ms response definition unchanged. The reference implementation
uses the same arrangement in
[referenceclient/impl.go](https://github.com/connectrpc/conformance/blob/eaa97669854d500691a393033d15950895d6aff7/internal/app/referenceclient/impl.go).
The local [client instruction schema](proto/connectrpc/conformance/v1/client_compat.proto)
also specifies this blocking-client arrangement.

Fixture-only BufferedIO instrumentation captures successfully completed socket
writes. After the exchange, it records the actual request bytes/digest/coding,
decoded request, absence of wire timeout, source transition, owned socket
interruption and retirement, and the real library canceled error. Prepared-body
or pre-dispatch telemetry alone cannot pass this ledger. Serialization of this
evidence stays outside the timed write path. A timer firing before dispatch is
valid cancellation behavior but insufficient official wire-cancellation evidence;
verification reports it instead of delaying or restarting the timer.

Identity runs 96 client/90 server cases and gzip runs 88/82, with all 184 client
and 172 server cases required to pass. The original four cancellation names and
payloads remain unchanged. Missing/unexecuted/duplicate/setup/unknown outcomes,
waiver output and a fabricated canceled result without wire progress fail the
ledger's focused negative probes. Ordinary errors may use identity even for gzip
requests; Basic success separately requires actual gzip responses in both codecs
and directions.

The identity inventory retains its original 94/88 baseline; `Ledger::GET_CASES`
adds exactly two official safe GET successes per direction. Actual GET method,
controls and decoded query payload bytes/hash are recorded separately from the
measured empty HTTP body. The fixture's request-local query echo populates
`ConnectGetInfo`; production RPC observation exposes no query or payload.
Focused Ruby and Go probes cover gzip GET and malformed-query/boundary behavior.
