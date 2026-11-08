# Async Host accept recovery

Status: frozen after independent Claude plan review and the coordinator's
explicit design approval on 2026-11-08. The exclusive local execution slot is
released to this repair for setup, RED/GREEN and focused/owning gates.
Base: `805bb8099bac63b9bff75b1cac7a637c9752217c`.
Scope: one repair PR referencing #26/M4.

## Outcome and fixed policy

Keep the standalone Host usable through bounded transient accept failures, then
close the failed host and expose the original exception to application supervision.
Jim selected this recovery direction. The coordinator approved the concrete
policy below before implementation.

- Retry only `Errno::EMFILE`, `Errno::ENFILE`, `Errno::ENOBUFS`, `Errno::ENOMEM`,
  `Errno::ECONNABORTED`, and `Errno::EINTR` raised by the socket accept operation.
  These select descriptor/memory pressure, an aborted pending connection and an
  interrupted accept. This is a deliberate recovery policy, not a claim that
  every platform surfaces each errno or that recovery is guaranteed.
- Each listener has five retries after its initial failed accept, with cooperative,
  cancellable delays of 0.05, 0.1, 0.2, 0.4 and 0.5 seconds. The sixth consecutive
  selected failure terminates the host. The total requested delay is 1.25 seconds;
  scheduler or native stalls are outside that timing bound. No jitter or public
  policy knobs are needed for this local operation.
- A successful raw accept resets that listener's consecutive-failure streak,
  including a peer subsequently rejected by admission or TLS. Another listener's
  success does not reset the failing listener. During retry, other listeners and
  admitted RPCs remain live. Existing deadlines, identity snapshots and caps hold.
- Every other accept `IOError`/`SystemCallError` is terminal, including `EBADF`,
  `EINVAL`, `ENOTSOCK`, and unsupported network errors. Unexpected `StandardError`
  escaping the listener is also supervised as terminal, never retried. Errors
  after raw accept do not spend the accept retry budget.

Five retries give brief pressure a chance to clear while bounding each failure
streak. Intermittent failures separated by successful accepts can continue
indefinitely, with no new retry notification API. A longer consecutive outage
belongs to the caller's supervisor. No RPC replay or client reconnection is
introduced. The finite budget also applies to aborted-connection/interruption
storms. A peer may contribute to ECONNABORTED exhaustion on platforms that expose
it. The reviewer recommended unbudgeted immediate retries for ECONNABORTED/EINTR;
that conflicts with the requested finite, no-spin policy. The coordinator
explicitly accepted the finite-budget error-storm tradeoff for all six errnos.

## Application supervision and shutdown

Add only `Host#wait`, with no arguments. On terminal listener failure it raises
the identical original exception after session abort and Host listener joins.
On ordinary shutdown it returns nil once accepting has stopped and the Host
listener tasks have finished; session cleanup has begun but may still be running.
`close` remains the API for awaiting the session's drain outcome. This distinction
is necessary for a Host-only repair: the session's only Host callback runs at the
start of owner-cancellation teardown, with no completion callback. The coordinator
approved this distinction. No shared lifecycle expansion is authorized.

Repeated/concurrent waits see the same outcome. Cancelling a waiter does not
close the host. Wait explicitly checks existing reactor/thread affinity before
touching a promise. Before start, wait raises
`LifecycleError`, unless the host was already closed; close-before-start remains
valid and wait then returns nil.

The first recorded terminal listener cause wins. Terminal handling closes all
listeners and uses the existing session's forced abort: no drain window,
raw-first close followed by cancel/join of peer resources, then explicit Host
listener joins. Contrary to the original draft, server abort has no 100 ms timer.
Its cancel/join is cooperative and has no hard bound for cancellation-resistant
or nonyielding application code. The fixed policy bounds retries and grants zero
drain time; it does not create a new server cleanup deadline. The coordinator
explicitly accepted this limit and withdrew any requirement for a new deadline.

Capture the application owner in Host construction. The cleanup supervisor is
an independent child of that owner, never a session-lifetime child or a child of
a short-lived start caller. After startup, recording the first terminal cause
starts this supervisor in the same turn, before a concurrent close can initiate
a drain window. The listener itself never aborts/closes the session. Settlement
must preserve the original recorded cause if owner cancellation interrupts
terminal cleanup; documentation and tests must distinguish completed retirement
from an interrupted cleanup, and must not claim every task finished solely from
promise settlement. Ordinary owner cancellation must still cancel real backoff
and blocked accept, retire owned resources, and resolve ordinary waiters.

`close(grace:)` retains its existing boolean drain outcome and validation, and
never raises a stored accept failure from an ensure. Repeated/concurrent close
shares the first close outcome. If ordinary close has begun, accept errors caused
by teardown are normal shutdown. If a terminal cause was recorded first, a
concurrent close cannot erase it. Unrelated exceptions escaping caller code must
survive ordinary ensure-close unchanged. `on_tls_error` retains its TLS meaning.

Start still binds before returning and is idempotent only while running. A
selected first-accept failure can enter retry before start returns. A terminal
failure already observed before start returns must complete cleanup and raise
that same error from start; failures observed later are raised by wait. After
terminal failure or ordinary close, start raises `LifecycleError`, consistent
with the existing nonrestartable session. Bind/initial setup errors continue to
raise synchronously from start; after their abort, wait returns nil, because
wait reports listener failure, not a second channel for already-raised setup
errors. A failure on one listener terminates
the whole Host after its budget is spent; during retries healthy listeners stay
usable. Initialize the listener task collection before scheduling and append
tasks individually. During start, listeners only record terminal causes; start
finishes collecting tasks, then tears down and raises the recorded cause in
preference to any secondary scheduling error. Stop scheduling once a failure
is recorded. Cancellation skips the current task via Internal.cancel, never
self-stops in a teardown callback. Once shutdown begins, listener exceptions
are shutdown outcomes even when they are not IO errors.

Partial startup after successful endpoint binding closes every bound listener
and owned task. Partial failure inside upstream multi-address binding is a
pre-existing io-endpoint boundary and outside this repair; do not claim it is
fixed. A private bind seam may supply two actual loopback listeners for stable
multi-listener tests without relying on dual-stack localhost DNS or shared ports.

## Implementation ownership

The fresh implementer owns `gems/connectrpc-async/lib/connectrpc/async/host.rb`,
focused real Host specs and test support under the owning gem, its README and
RBS, `typecheck/surface.json` entries for every new public and private method,
API callers and installed runtime callers needed for `wait` (close then wait,
without blocking installed checks). Root owns the M4 support-document adoption
disposition and the stale Still open entry; this PR must report the required
coordinator update rather than edit that policy unasked. Keep retry timing
private and testable without public tunables or a
generic state/supervision framework. Retain the diagnosis seam: wrap the actual
listener object's accept boundary while using real Host, ServerSession, socket,
transport and generated Echo calls. A private backoff method may provide a
deterministic timing boundary, but at least close-during-backoff exercises the
real cooperative timer.

Prefer Host-only lifecycle implementation using existing ServerSession APIs.
Any directly necessary shared private lifecycle edit requires an explicit
rationale and affected Falcon evidence before expansion. Core, transport,
Falcon source, observer #48 and other #26 repairs are outside scope. Do not
alter pins, inventories, assertions or limits to obtain a passing result.

## Verification and delivery

1. Independent Claude review and the coordinator checkpoint are complete.
   No UI exists; UX review does not apply. Use a fresh implementation worker.
2. The coordinator released the exclusive runtime slot for setup, implementation
   RED/GREEN and focused/owning gates. Use existing Mise tasks and pinned bundles.
   Preserve all original diagnosis and unrelated failure logs. Request coordination
   again before final heavy aggregate graphs; no aggregate retry is authorized.
3. Before product changes, run a focused regression that fails at its intended
   fresh generated RPC/progress/terminal-observation assertion. Prove one-shot
   recovery preserves both a held in-flight RPC and a later RPC on that admitted
   connection, plus a fresh typed client. Exercise h2c and verified TLS/mTLS
   composition as warranted, using existing helpers and both pinned Ruby lanes.
4. Prove the exact delay sequence, six-failure exhaustion, original exception
   identity, reset after successful accept, per-listener independence and no
   extra attempts across controlled backoff boundaries. Cover all six selected
   errnos and representative permanent/unsupported errors at the actual seam.
   Prove all listeners/resources/tasks retire on normal terminal cleanup before
   wait raises, even with an admitted stalled peer. Cover owner interruption
   explicitly and preserve the cause without overstating completed cleanup.
   Use events or controlled timing, never fixed sleeps
   to infer progress. Include a multiple-listener case with real listening sockets.
5. Cover first-accept transient and permanent errors, partial startup, close
   during blocked real accept and real backoff, owner cancellation, repeated
   close/wait, concurrent close/wait, waiter cancellation, invalid wait affinity,
   close-before-start, start-after-failure and caller exception preservation.
   Add focused tests where each distinct failure mode needs them; avoid duplicate
   permutations. Runner output must confirm each new example was collected.
6. Run both pinned lanes' focused owning specs, `async:check`, required RBS/API
   inventory and installed caller checks. Broaden to owning Async tests and
   package evidence where justified. Repository policy's final `verify` and
   `rack:verify` are qualification gates, but known aggregate failures and slot
   policy require coordinator agreement before launching those heavy graphs.
   Preserve every initial failure and stop for disposition; never retry a graph
   or CI until green. Exact-head CI supplies only the evidence it actually runs.
7. Use ship-feature-pr: fresh implementation, inspect and commit, one draft PR,
   independent exact-head Codex/Claude review, then authorized CodeRabbit after
   internal acceptance. Apply convergence rules to correction batches. Never
   merge. Readiness requires current base, required checks/reviews on head,
   and no valid blocking feedback. Report open threads and qualification gaps
   to the coordinator, who owns public issues and support adoption disposition.

The main CI Falcon H1 Ruby 4 identity failure (173/174) and observer's historical
Ruby 3.4 async:interop SSL_connect ECONNRESET remain independent evidence. This
repair establishes no common cause or causal fix for either.

## Unresolved questions

No product questions remain. If actual cancellation or startup behavior conflicts
with the frozen guarantees, stop with evidence rather than expanding shared code.
Final heavy aggregate execution requires coordination. Known main CI Falcon H1
and observer local reset failures remain qualification blockers, with no waiver
or repair authority here.
