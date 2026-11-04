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
version-specific ledger requires exact scheduled, response and completion sets,
and branch-specific actual dispatch sets. It checks totals, duplicates, missing outcomes, setup
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

A call-owned observer covers the real Client invocation, transport entry,
connection setup and exchange. Each entered scope must exit exactly once, and
the closed call's entry/exit counts must match its emitted events in causal
order. Cancellation before transport or during setup therefore needs positive
lifetime evidence, the real source transition, and the actual library canceled
error with no cause. Missing dispatch alone is never evidence of early
cancellation. The original request and actual nil timeout argument are checked
independently of whether transport starts. The timer's arming, expiry and source
request times are recorded without restarting or waiting for wire readiness.

Fixture-only BufferedIO instrumentation counts attempted writes and captures
successfully completed writes through exchange completion. A closed observer
with zero attempts proves no request write started. An attempted write that did
not return leaves partial byte delivery unknown. Successful write prefixes are
compared with the prepared body; complete requests additionally require the
original decoded body, codec/compression, name, length and no wire timeout.
A completed header write still requires the correct name and Content-Length
against the prepared body when a later body write is interrupted. Unknown
remaining delivery never exempts known completed-write facts.
Raw I/O failures and the transport's normalized canceled result are recorded
separately. Socket lifecycle attachment, close calls and retirement are observed
at their actual boundaries. NetHTTP can independently close the same socket
while a lifecycle close runs, so a close call reports `interrupted: null`.
A scope with no observed close calls reports `interrupted: false`; the fixture
never claims proven interruption from before/after socket state. Final closure
is unknown when no raw socket was attached, rather than reported as an observed
open or closed socket. Dedicated controlled-peer tests establish real wire
interruption separately from this concurrent observer.

Summaries retain scheduled/completed cases, actual dispatches, cancellation
stages and cancellations before transport separately. The stages are
`before_transport`, `before_setup`, `during_setup`, `before_write`,
`write_incomplete`, `request_written`, `write_overlapped_signal` and
`response_observed`. The `write_overlapped_signal` stage means a completed-write
timestamp followed the source request timestamp; it does not establish whether
each byte preceded cancellation. Every normal
case still requires its complete dispatch and wire-terminal observations.

`response_observed` requires a complete transport response, complete actual
request writes, successful balanced nested scopes, the source's first transition
and the real Client canceled/nil-cause outcome before its terminal commit. It
means the transport observed a response, without claiming that Client validated
or decoded it successfully. Resource retirement and closure are checked
separately; this branch does not require or imply socket interruption or server
cancellation. Both a response and a failure, missing scope exits, a fabricated
adapter result, unrelated errors or an already committed Client success fail.

Dedicated synchronized tests in
[conformance_cancellation_spec.rb](../spec/conformance_cancellation_spec.rb)
prove actual full-request cancellation in both codecs and compression modes,
with a real peer observing request progress before cancellation. They check the
library canceled/nil-cause outcome, actual body bytes, owned socket cleanup,
no retry/late send and registration cleanup. These deliberately controlled
cancellation tests are separate from the unmodified official 5 ms cases.
[NetHTTP cancellation tests](../spec/net_http_cancellation_spec.rb) and the
[native HTTP/2 cancellation tests](../gems/connectrpc-async/spec/cancellation_spec.rb)
retain their real I/O and handler-progress coverage. Official case passes do not
claim that every timer necessarily fired after a request write.

Identity runs 96 client/90 server cases and gzip runs 88/82, with all 184 client
and 172 server cases required to pass. The original four cancellation names and
payloads remain unchanged. Missing/unexecuted/duplicate/setup/unknown outcomes,
waiver output and a canceled adapter result without a complete causal observer
fail the ledger's focused negative probes. Ordinary errors may use identity even for gzip
requests; Basic success separately requires actual gzip responses in both codecs
and directions.

The identity inventory retains its original 94/88 baseline; `Ledger::GET_CASES`
adds exactly two official safe GET successes per direction. Actual GET method,
controls and decoded query payload bytes/hash are recorded separately from the
measured empty HTTP body. The fixture's request-local query echo populates
`ConnectGetInfo`; production RPC observation exposes no query or payload.
Focused Ruby and Go probes cover gzip GET and malformed-query/boundary behavior.

## Connect HTTP/1.1 server profiles

`mise run falcon:conformance:http1` runs separate official Connect SS/CS server
profiles for HTTP_VERSION_1, protobuf/JSON and identity/gzip. Half-duplex bidi
support is false. The independent `http1_ledger.rb` and `cases-http1-*.json`
retain exact discovered inventories and strict accounting. Existing H2 and
client profiles are unchanged. Both Falcon specs/package CI lanes run this
required task after the existing Connect and gRPC conformance tasks. This does
not implement a Ruby H1 client.

Production-source runs on 2026-11-04 passed 174/174 identity and 160/160 gzip
cases on each of Ruby 3.4.12 and 4.0.9, without a RUBYOPT overlay. Fresh exact
first-request-size-limit CS cases also passed over plain TCP and TLS on both
lanes. The original assertions, inventories and strict ledgers are unchanged;
no case is waived, excluded or counted as an expected failure.

Retiring delivered Connect H1 responses use a cooperative 500 ms total budget
from the actual final HTTP flush, with raw TCP write-side half-close. Response
EOF precedes the unchanged absolute 100 ms / 256 KiB discard; no further reads
occur during the remaining wait. Scheduler overshoot and admission/socket
retention are accepted. This is best effort reset avoidance, not a guarantee
under arbitrary network/peer failure. Go is a comparison: connect-go may consume
the full declared oversized message, while net/http may drain before flush or
reuse. Ruby remains response-first and bounded. The official runner stays
connectconformance 1.0.6 (embedded connect-go 1.21.0, Go 1.27.2); the separate
repository peer remains connect-go 1.21.1.

The earlier hosted Web H1/TLS oversized SS failure at `d928ce6` (275/276) remains
unexplained despite later passes. This Connect-only policy does not establish a
fix for it. Clean-environment CI and exact-head review remain separate from
these local results. See [streaming](../docs/streaming.md#falcon-http11-server)
for the precise policy. Ruby H1 client work remains pending; standalone Async
Host stays H2-only. This delivery is one server-only PR, with no client stack.
