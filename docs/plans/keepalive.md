# Plan: HTTP/2 keepalive, enforcement and connection age (#69)

Base: origin/main 8c2dd4b. Branch: feat/h2-keepalive. One PR.
Status: implemented in [#76](https://github.com/jimeh/connectrpc-ruby/pull/76).
Accepted by Jim on 2026-11-16 and revised after a Codex plan review; the plan
below is kept as approved.

## Objective

Long-lived Async clients (`Transport`, `ThreadedTransport`) detect half-open
HTTP/2 connections with PINGs and retire them like any failed connection.
The Async `Host` and the Falcon native H2 path can PING dead clients, enforce
a minimum client ping interval as grpc-go does, and bound connection idle time
and age with graceful GOAWAY, so clients behind L4 load balancers rebalance.

## Constraints

- Pinned protocol-http2 0.29.2 has `send_ping`, and its `receive_ping` writes
  and flushes the ACK synchronously on the reader. It ignores inbound ACKs.
  `send_goaway` calls `close!`, which ends our reader loop. No newer
  protocol-http2 is needed.
- HTTP/2 ownership, cancellation and retirement stay in `connectrpc-async`.
  Core gets nothing.
- Public API changes need RBS signatures, inventory entries and installed
  typed callers (`docs/rbs.md`).
- Tests use an injected clock; Go-side minimums (grpc-go client `Time` >= 10 s)
  are an external boundary where wall time is unavoidable.
- grpc-go v1.85.0 reference behavior:
  - Server `handlePing`: queue ACK; if strikes were reset by a server
    HEADERS/DATA write, zero them and skip; else a ping within 2 h (no active
    streams, `!PermitWithoutStream`) or within `MinTime` strikes; more than 2
    strikes sends GOAWAY(ENHANCE_YOUR_CALM, "too_many_pings") and closes.
    The first ping never strikes.
  - Client: on too_many_pings, sets `Time = max(current, 2 * offending
    transport's Time)` for later transports.
  - Server: `Time` 2 h, `Timeout` 20 s. `MaxConnectionAge` gets +/-10% jitter.
    Drain sends GOAWAY(MaxInt32) + PING, then GOAWAY(last id) on ACK or after
    a fixed 5 s. Age grace starts at age expiry. Idle is measured from the
    moment active streams reach zero (or connection start); PINGs do not
    reset it.

## Client

- `ConnectRPC::Async::Keepalive.new(time:, timeout: 20,
  permit_without_calls: false)`: frozen value object, equality like `Backoff`.
- `ConnectionPolicy.new(keepalive: nil)`: nil disables. `ThreadedTransport`
  already forwards `connection_policy:` to its inner `Transport`.
- **Maintenance tasks.** A new resource ownership class for long-lived
  maintenance tasks: excluded from `Resource#empty?` (so graceful drain and
  idle detection still see an empty connection) but included in
  `retirement_roots`, descendant capture, cancellation and `owns_task?`.
  Graceful close and drain stop maintenance before waiting for emptiness.
- **Monitoring condition.** Monitor whenever the connection has open
  streams, registered writers, guard waiters or unfinished stream output
  (the #72 reset-stream output), or always with `permit_without_calls: true`.
  Otherwise dormant until one of those appears; a new stream-open
  notification (mirroring `Connection#delete`'s) wakes it.
- **Probe.** After `time` with no frames read:
  1. Record a probe id (8-byte counter) and an absolute deadline
     `now + timeout` before attempting the write-guard acquisition.
  2. A child writer task acquires the guard and writes the PING. The
     supervisor waits outside the guard.
  3. Success needs the matching ACK before the deadline, observed on the
     reader even if it arrives before the writer task returns. Other frames
     do not count (a peer that writes but never reads must not pass). Reads
     only defer the next probe.
  4. Expiry or write failure: record the terminal reason on the resource,
     then abort raw-first, which unblocks any stuck writer.
- Frame reads are timestamped in `read_frame`'s pre-application block hook,
  so a synchronously blocked ACK write cannot hide receipt.
- In-flight calls on a retired connection fail `unavailable` and are not
  resent. The refused-retry rule is unchanged.
- Draining (GOAWAY-received) connections keep monitoring while work remains.
- `ClientConnection#receive_goaway` records ENHANCE_YOUR_CALM +
  `too_many_pings`; the manager sets its effective time to
  `max(current, 2 * that connection's time)` for later connections.

## Observability

- New `ConnectionEvent` type `:connection_closed`, `outcome` = reason:
  `:keepalive_timeout`, `:drain_timeout`, `:goaway`, `:closed`.
- The reason is stored on the resource before any yield, first writer wins
  (precedence by time of decision). The event is emitted once, from the
  accounting transition where the connection leaves the manager, not during
  transport `close`. It survives later lease release.
- No struct change. Metrics `ConnectionObserver#call` ignores unknown types
  (`lib/connectrpc/metrics/connection_observer.rb:42`), so Prometheus/Yabeda
  labels hold. One spec asserts it records nothing. RBS event-type and
  outcome unions and the README observer contract are extended.

## Server

- `ConnectRPC::Async::ServerKeepalive.new(time: 7200, timeout: 20,
  min_time: 5, permit_without_calls: true, max_connection_age: nil,
  max_connection_age_grace: nil)`. `time: nil` disables server PINGs,
  `min_time: nil` disables enforcement. The defaults accept every valid grpc-go
  client configuration (grpc-go clients ping at most every 10 s; 5 s leaves room for
  network jitter on idle connections, as grpc-go's keepalive example server does) and still stop floods.
- `keepalive:` on `Host`, `ServerSession` and `ConnectRPC::Falcon::Server`
  (default `ServerKeepalive.new`, nil turns all off). H2 resources only;
  Falcon HTTP/1 resources keep today's idle abort.
- **Server PINGs** use the client's probe code, regardless of streams.
- **Enforcement.** `ServerConnection#receive_ping` evaluates strikes first,
  without yielding, then hands the ACK (and, on violation, the GOAWAY) to a
  supervised control writer bounded by its own 100 ms write bound, so a held
  outbound frame cannot block the reader. Strike reset on server HEADERS/DATA
  writes. On violation: GOAWAY(ENHANCE_YOUR_CALM, "too_many_pings", cutoff),
  half-close, bounded discard to EOF, raw-first abort.
- **Idle.** `idle_timeout` (existing, default 60) becomes graceful for H2,
  like `MaxConnectionIdle`. Idle time comes from a timestamp set when active
  streams reach zero or at start, not from `activity_at`, so PING traffic
  does not extend it.
- **GOAWAY-first drain** for age and idle, as an explicit state machine:
  1. *Heads-up:* write GOAWAY(2^31-1, NO_ERROR) + drain PING without
     `close!`. Requests keep dispatching normally (no `draining` flag, no 503).
  2. *Cutoff:* on the drain PING ACK or 5 s, freeze the cutoff (separate
     from the parser's highest stream id), install it without yielding, then
     write GOAWAY(NO_ERROR, cutoff). Streams above the cutoff go through
     normal protocol bookkeeping (HPACK, fragmented HEADERS, DATA, RST,
     connection flow control) and are reset with REFUSED_STREAM rather than
     dispatched, so siblings never see a connection error.
  3. *Quiescent:* when accepted streams and handlers finish, half-close and
     read to EOF, bounded independently of age grace.
  4. *Retiring:* raw-first abort. Age grace (nil = unlimited) starts at age
     expiry and aborts on expiry.
  Concurrent session `close` or `drain_tls` supersede it: they keep the
  existing cutoff (never raise it) and their own grace bounds the rest.
  Control writes in every step are supervised and bounded.

## Rails and Rack

They run behind a third-party server (Puma is HTTP/1.1; H2 is terminated by a
proxy). The gem does not own the H2 connection, so keepalive, enforcement and
age limits belong to that server or proxy (for example Envoy
`max_connection_duration`, nginx `keepalive_timeout`). Documented only.

## Interop (Go)

New `interop/grpc/keepalive.go` with commands dispatched by one added line in
`main.go`. `golang.org/x/net/http2` becomes a direct require for a raw-framer
peer. Kept additive because the Health thread also edits `interop/grpc`.

## Non-goals

TCP keepalive, HTTP/1.1 keepalive or age limits, automatic resend of
dispatched calls, new metrics instruments, per-call keepalive, keepalive for
Rails/Rack hosts.

## Verification

| Concern | Evidence |
| --- | --- |
| Validation | `Keepalive`, `ServerKeepalive`, `ConnectionPolicy(keepalive:)`: invalid values raise, frozen, equality |
| Clock | Extend `FakeClock`: one `advance` updates `now` once and releases all due sleepers; keepalive, age, idle and drain use the injected clock. Same-boundary timeout vs inbound activity ordering |
| Client probe timing | Reads defer probes; dormant without work when not permitted; permitted idle pings; matching ACK only; unrelated ACK, pre-flush read, post-flush DATA, immediate ACK, write failure |
| Black hole | Peer suppresses ACKs with a gated in-flight call: retired by `time + timeout`, `:connection_closed`/`:keepalive_timeout` once, call `unavailable`, one server invocation, next call on a new connection |
| Siblings | `connection_limit: 2`, one connection black-holed, the other keeps serving |
| #72 held frame | `permit_without_calls: false`, zero registered streams, frame gate unreleased, later caller waiting behind the guard: connection retired within bound, caller fails, next call succeeds |
| Maintenance ownership | Graceful close while keepalive is dormant and while its writer is blocked; abort started by the keepalive supervisor itself |
| Event ordering | Exactly-once, reason kept through later lease release, observer interruption, simultaneous EOF/timeout precedence |
| ThreadedTransport | Keepalive keeps running after the initiating thread's call returns; close joins it |
| too_many_pings | Peer GOAWAY(ENHANCE_YOUR_CALM, too_many_pings): next connection's interval doubled from that connection's interval |
| Wire fixtures | Extend `RecordingServer` to record PING payloads and GOAWAY code/debug/cutoff |
| Enforcement | Raw H2 client: strike counting, reset after server DATA, 2 h no-stream rule, permit flag, GOAWAY code/debug/cutoff, ACK while an outbound frame is held |
| Server PINGs | A client that never ACKs is aborted, handler cancelled |
| Drain states | Heads-up/cutoff GOAWAY pair, request between them dispatched, stream above cutoff refused while a sibling keeps receiving DATA, fragmented HEADERS + HPACK refs + DATA/RST after refusal, missing drain ACK (5 s), withheld EOF, stalled GOAWAY write, age grace from expiry, close/drain_tls superseding |
| Idle | Graceful GOAWAY; PING/ACK traffic does not extend idle |
| Falcon | Real native worker: age drain, keepalive timeout, enforcement; HTTP/1 fallback idle regression |
| Metrics | Core observer spec ignores `:connection_closed` |
| Ruby client vs grpc-go server | Go server with scaled `MinTime` 1 s: compliant Ruby client (time 1 s) on a quiet bidi past the strike threshold, same connection, then echoes; over-eager client (0.2 s) gets too_many_pings and doubles |
| grpc-go client vs Ruby Host | Documented example settings (10 s / 1 s / permit) against the chosen defaults, quiet past the strike threshold (>= 4 pings), server records one connection; over-eager raw-framer peer gets too_many_pings; BDP ping evidence asserted from server wire records, not inferred from payload size |
| Age drain with Go | Ruby Host age 1 s; server-side wire records prove GOAWAY sent on connection A, the in-flight Go bidi completes on A after it, the next RPC uses connection B |
| Modes | h2c, TLS and mTLS, run concurrently to keep wall time near 45 s, in a new `async:interop:keepalive` task included in `async:verify`; Falcon cases in `falcon:interop:grpc` |
| Regression | `async:verify`, `falcon:verify`, `otel:verify`, `prometheus:verify`, `yabeda:verify`, `rbs:check`, `rbs:package`, `verify`, `rack:verify` on both Ruby lanes |

## Decisions

Jim accepted these on 2026-11-16, with the condition that grpc-go client
defaults stay compatible:

| Question | Decision |
| --- | --- |
| Client placement | Nested `Keepalive` on `ConnectionPolicy` (`keepalive: nil` off) |
| Client default | Off; the README recommends `time: 300`, safe against grpc-go default enforcement |
| Server defaults | Tolerant: `min_time: 5` (changed from 10 during implementation for jitter margin), `permit_without_calls: true`, server PINGs 2 h / 20 s, no age limit |
| `idle_timeout` | Becomes a graceful GOAWAY drain for H2 |
| Observability | `:connection_closed` with a reason for every retirement |
| Delivery | One PR |
