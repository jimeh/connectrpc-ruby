# Plan: long-running lifecycle campaign (#34)

Base: origin/main 5c5e094. Branch: test/lifecycle-campaign. One PR.
Status: accepted by Jim on 2026-11-16 with every recommended option. Revised
after a Codex plan review.

## Accepted decisions

- Harness: RSpec scenario files in `gems/<pkg>/soak/` with their own options
  file (no `spec_helper`, no SimpleCov), run by a root `scripts/soak.rb`
  supervisor with a run deadline, a cleanup budget and a final kill.
- Tasks: `async:soak`, `falcon:soak` and the umbrella `soak`.
- Stable baseline: exact owned-object retirement plus exact global counts
  against a warmup-stabilized baseline; peaks checked against configured caps
  under load; RSS and heap reported only.
- Bounds and CI: local default 5 rounds per scenario; non-gating nightly and
  dispatchable `soak.yml`, {async, falcon} x {3.4.12, 4.0.9}, 50 rounds,
  60-minute timeout, reports uploaded on failure.
- Rack and Rails: out of scope (see below); core NetHTTP client churn is covered
  against Falcon HTTP/1.
- Real Falcon worker processes: out of scope.
- Harness negative controls run in `async:test`.

Rack and Rails do not own connections: Puma or the fronting proxy does. Our
code there owns response-body completion and `rack.response_finished`, which
the Rack specs already cover per request; Rails execution is owned by
`connectrpc-on-rails`.

## Objective

A bounded, discoverable campaign that repeatedly drives the supported native
clients and servers through churn and fault scenarios, observes owned resources
and queues, and fails with diagnostics unless: owned resources retire and global
counts return exactly to a stabilized baseline after cleanup, admission and
queues stay inside configured bounds at their peaks, siblings make the progress
each scenario promises, and no logical call is executed or resent beyond the
documented rules.

## Constraints

- No fixed sleeps: poll asserted conditions under bounded deadlines; report the
  last observation on timeout.
- Dynamic localhost ports; disposable in-process peers only.
- Do not edit `gems/connectrpc-async/lib`, `gems/connectrpc-falcon/lib` (#26
  closeout) or `gems/connectrpc-on-rails/` (#86).
- Not part of `*:test` or any `verify`; no SimpleCov.
- Real timers with small values on purpose (fast specs already prove the state
  machines with fake clocks).

## Shape

```
scripts/soak.rb                    # supervisor: runs one package campaign as a child process
gems/connectrpc-async/soak/
  .rspec                           # --require ./soak/soak_helper (custom options file; skips spec_helper/SimpleCov)
  soak_helper.rb                   # library + selected spec/support helpers + harness
  support/{sampler,baseline,ledger,fault_relay,report,topology}.rb
  *_soak.rb
gems/connectrpc-falcon/soak/       # same layout; requires async soak support
gems/connectrpc-async/spec/soak_harness_spec.rb   # harness negative controls, in async:test
.github/workflows/soak.yml
```

- `scripts/run.rb soak` branch in both packages: `rspec --options soak/.rspec --default-path soak --pattern '**/*_soak.rb'`.
- Supervisor (`scripts/soak.rb`): monotonic run deadline, then a separate
  cleanup budget. On deadline it signals the child, whose trap writes thread
  backtraces, Async task trees and fd readlinks to the report; after the cleanup
  budget it kills the child. Exit non-zero on either.
- Reports: `vendor/<pkg>/soak/<ruby>/` (already ignored), resolved from the repo
  root. JSONL appended per round/scenario so partial reports survive hangs and
  kills; a Markdown summary at the end.
- Harness owns its own reactor (not `AsyncHelpers#reactor`, whose 10 s timeout
  and child cancellation would mask leaks). Peers that callers block on run on a
  separate reactor thread (`ThreadedHelpers::PeerThread` pattern). Relay
  sockets, listeners and threads are registered for cleanup before setup.

## Observation

Two layers, both exact:

1. Owned objects (primary). Every resource the scenario creates is tracked and
   must retire: client connection resources reach `retirement_completed?`
   (not just `finished?`); server resources leave the session; ThreadedTransport
   owner threads are joined; raw sockets are closed; H2 reader, writer, output
   and maintenance tasks are finished; TLS refresh attempts/timers finished.
2. Global counts (secondary, catches untracked leaks): threads, fds
   (`/dev/fd`), Async tasks of the harness reactor and of every
   ThreadedTransport owner reactor. Baseline is taken only after warmup rounds
   that exercise every path in the scenario, repeated until two consecutive
   post-cleanup samples are identical (at most 3 warmups, else the scenario
   fails as unstable with both samples). After cleanup: equal to baseline.

Live registries (empty after cleanup, compared at equivalent phases for
persistent clients):

| Subject | Registries |
|---|---|
| ConnectionManager | `connections`, `@draining`, `@reserved`, `@waiters`, `@drain_timers`, `@attempt`, `@timer` |
| Transport | `@calls`, `@acquiring`; per connection writers/output/maintenance tasks |
| ThreadedTransport | engine `@calls`, admission count and bytes (read under its lock) |
| HTTP1Transport | `@calls`, `@leases`, `@idle`, retained resources |
| ServerSession | `@admitted`, `@h1_wire`, `@identities`, live resources |
| Falcon H1 resource | active state, tracked tasks, retirement members |
| Cancellation | a long-lived, never-cancelled shared source given to every non-cancelled call: its `@registrations` returns to empty after each round (proves unregister, not cancel-clearing) |

No separate deadline registry exists (deadlines go through `CallCancellation`).

Peaks and bounds, sampled during established saturation, not after quiescence:
admission count vs `max_requests`; reserved Connect unary bytes vs
`max_pending_bytes`; raw client sockets via `SocketMeter` vs
`connection_limit + max_draining`; server connections vs `max_connections`;
ingress retained bytes and output queue maxima via the existing
`streaming_bounds` observations. RSS and GC live slots are reported, never
asserted.

All private reads live in `sampler.rb`.

## Correctness invariants

- Execution at most once: every logical call carries `x-soak-id`; the handler
  ledger counts entries per id; each id runs at most once and every success ran
  exactly once. Checked after retirement, so a late resend is caught.
- Wire attempts: a raw H2 peer counts HEADERS per id for the refusal cases:
  Connect unary first refusal resends once; second refusal fails `unavailable`
  with no third attempt; native gRPC unary and streams are never resent;
  1xx HEADERS then refusal is never resent; unsent reroutes are distinguished
  from resends.
- Outcome allowlists per scenario; counts in the report.
- Siblings, placed per scenario:
  - stream consumer stops consuming, connection reader still runs: same-connection siblings must succeed;
  - TCP peer that reads nothing: siblings on another connection must succeed;
  - RST, keepalive timeout, forced shutdown, stuck-frame fallback: affected
    calls fail with the asserted code; unaffected connection/host keeps
    progressing; next calls recover.

## Workload matrix

| Client | Server | Shapes |
|---|---|---|
| Transport | Async Host, Falcon H2 (h2c, TLS, mTLS) | Connect unary POST/GET, server/client/bidi stream; gRPC unary, server stream |
| ThreadedTransport | Async Host, Falcon H2 | Connect unary POST/GET, server stream; gRPC unary, server stream |
| HTTP1Transport | Falcon H1 (TCP, TLS) | Connect unary/GET, server/client stream |
| core NetHTTP | Falcon H1 | Connect unary |

Concurrency, payload sizes and codecs are fixed per scenario and recorded.

## Scenarios (each repeated N rounds)

1. Churn and reuse: persistent client (reused connection, identical quiescent
   samples across rounds) plus clients created and closed every round.
2. GOAWAY: age drain (`ServerKeepalive max_connection_age` small, grace) under
   load, synchronized on observed GOAWAY/`connection_closed` events, not
   elapsed time; idle drain of a persistent client; `drain_tls`.
3. Ordinary stream retirement: per-call `CancellationSource`s for a random
   subset, deadlines, ThreadedTransport `Timeout`/`Thread#raise`/`Thread#kill`,
   and flow-control credit saturation. Server sees cancellation; same-connection
   siblings finish; connection count unchanged.
4. Stuck-frame fallback: `FrameGate` holds a partially written DATA frame, then
   cancel: documented connection-wide fallback; siblings on that connection fail
   `unavailable`; next calls reconnect. Plus the peer-reset exception (lease
   released while output is blocked; keepalive retires the connection).
5. Slow and non-reading peers: non-consuming stream reader (scenario 3
   placement); fully non-reading TCP client against the server (other-connection
   siblings; host close returns false after grace; all released); client-stream
   handler that never reads the upload (client deadline ends it). Falcon H1:
   client that withholds request input (input retirement limits) and client
   that never reads the response (idle/grace bound), kept separate.
6. Peer failure: relay RSTs every connection mid-traffic; Host and Falcon
   `close(grace: 0)` with calls in flight.
7. mTLS/provider rotation: provider version changes per round; `refresh_tls` +
   `drain_tls` under mTLS load; assert generation increments, new connections
   present the new leaf (observed client side) and handlers see the expected
   client identity; old-generation connections retire; client identity rotation
   by replacing the transport; a blocked provider refresh interrupted by close.
8. Shutdown: graceful (`close` true) and forced (`close` false) for Host, Falcon
   server, Transport, ThreadedTransport (owner thread joined), HTTP1Transport.
9. Keepalive (#76): client `Keepalive` + relay blackhole -> `:keepalive_timeout`
   and reconnect; server keepalive with a raw client that never ACKs -> abort;
   `too_many_pings` -> ENHANCE_YOUR_CALM and doubled interval on later
   connections.
10. Refusal semantics (wire-attempt cases above) repeated under churn.

## Bounds, tasks, CI

- `CONNECTRPC_SOAK_ROUNDS` (local default 5), `CONNECTRPC_SOAK_DEADLINE`
  (run, default 900 s), `CONNECTRPC_SOAK_CLEANUP` (default 60 s),
  `CONNECTRPC_SOAK_SEED`, `CONNECTRPC_SOAK_ONLY` (scenario filter).
- Tasks: `async:soak`, `falcon:soak`, umbrella `soak`; each depends on its own
  package setup; not in `verify`.
- `soak.yml`: nightly + `workflow_dispatch`, non-gating, matrix {async, falcon}
  x {3.4.12, 4.0.9}, `CI_EXPECTED_RUBY_VERSION` per lane, rounds 50 with a
  matching deadline, 60-minute job timeout, `if: always()` artifact upload of
  the report and job summary.
- Report records revision, Ruby/OpenSSL/platform, resolved gem versions, seed,
  rounds completed, concurrency, payload sizes, elapsed time and every failure
  with its diagnostics.
- Root `rake syntax` gains the two `soak/` globs; RuboCop already covers them
  through each package's `check`.
- Docs: a `docs/development.md` section and a `docs/ci.md` section; Async and
  Falcon READMEs get one line each pointing at the tasks.

## Out of scope

- Real Falcon worker processes (in-process server runs the same session and
  resource code; `process_workers_spec` covers worker stop narrowly).
- Arbitrary native or non-yielding application interruption.
- Memory growth assertions.
- Rack/Rails hosts (see accepted decisions).

## Verification

- `soak_harness_spec.rb` (fast, in `async:test`): leaked fd, thread, task,
  registration, unretired resource, transient cap overshoot and retained stream
  bytes are each detected; one perturbation run of a real scenario confirms a
  failure is reported at the intended assertion.
- `mise run soak` locally on 3.4.12 and 4.0.9; one larger local run; reports
  summarized in the PR.
- `async:check`, `falcon:check`, `syntax`, `lint:workflows`.
- `soak.yml` dispatched on the PR branch.
- First failure of any intermittent result is kept with its artifacts and
  investigated, not replaced by a later pass. Defects: recorded with evidence;
  fixed here only if small and outside #26-owned files, else reported to the
  coordinator.
