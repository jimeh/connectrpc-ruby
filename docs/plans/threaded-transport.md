# Threaded transport and resilient Async connections

Status: design for review. No implementation is approved by this document.
Base: `95ecf9f` (Async Host accept recovery, #50).
Issues: [#28](https://github.com/jimeh/connectrpc-ruby/issues/28) (Async
connection management) and [#29](https://github.com/jimeh/connectrpc-ruby/issues/29)
(persistent client for ordinary threads), following the findings in
[#24](https://github.com/jimeh/connectrpc-ruby/issues/24).
Delivery: four implementation PRs, each needing separate approval.

## Objective

A Rails app on Puma, or any other threaded Ruby program, should be able to
create one client at boot and share it across request threads. The client keeps
one HTTP/2 connection, multiplexes concurrent calls over it, and reconnects on
its own after the connection fails or the server comes back from downtime. This
is the experience of grpc-go's `ClientConn`.

Today a threaded caller has two options, and neither fits:

- The core NetHTTP transport opens and closes a TCP/TLS connection for every
  call (`lib/connectrpc/transports/net_http.rb:126-173`). It sends
  `Connection: close`, starts a watchdog thread per call that has a deadline,
  and supports only Connect unary over HTTP/1.1.
- The Async `Transport` already pools and multiplexes HTTP/2 with
  `connection_limit: 1`, but it requires an application-owned reactor. Its
  constructor, calls and close must run on that reactor's thread
  (`gems/connectrpc-async/lib/connectrpc/async/internal.rb:86-102`).

The Async transport also lacks the connection policy that a long-lived client
needs (#28). Setup runs under whichever RPC arrived first, failures have no
shared backoff, callers cannot wait for readiness, and a draining connection
keeps the only pool slot after GOAWAY.

This design adds both pieces: a private connection manager inside the Async
H2 transport, used by every H2 `Transport`, and a `ThreadedTransport` that
drives that transport from one owner thread on behalf of any number of caller
threads.

## Decisions

Jim accepted these choices on 2026-11-15:

| Question | Decision |
| --- | --- |
| Package and name | `ConnectRPC::Async::ThreadedTransport` in `connectrpc-async`. It uses `Client.new(transport:)` like the other transports. |
| Defaults | Go-like: one connection, fail-fast, backoff of 1 s growing by 1.6x with 20% jitter and a hard 30 s cap, one extra draining connection, 16 MiB of admitted request bytes, refused unary calls retried once. |
| Tuning | Every policy value is configurable, and each feature can be turned off: `backoff: nil`, `max_pending_bytes: nil`, `retry_refused: false`, `max_draining: 0`. `wait_for_ready` requires a backoff. |
| Effect on `Async::Transport` | The connection policy is on by default for every H2 `Transport`, not only `ThreadedTransport`. There is no legacy mode. Jim chose this on 2026-11-15, reversing an earlier opt-in choice, because nothing has been released and nobody depends on today's behavior yet. It overrides #28's request to preserve existing behavior. |
| Delivery | Four PRs: the #28 manager core, threaded Connect unary, GOAWAY replacement with refused-stream retry, then the stream proxy. Client and bidi streaming from threads come later. |

The direction set before this design also stands:

- The threaded path is an explicit object that owns one background thread
  running an Async reactor. The existing Async transport keeps its promise of
  no hidden reactor thread.
- Reconnecting never replays an RPC that may have reached the server. Only a
  call that provably never ran is sent again.
- NetHTTP stays the dependency-free fallback. The measurements below show the
  threaded transport is not far off, so keep-alive for NetHTTP stays out of
  scope.

## Feasibility evidence

Throwaway spikes, kept out of this PR, prototyped the threaded transport as
about 150 lines. They used `Thread::Queue` handoff to an owner thread that runs
`Async { Transport.new(...) }`, against an Async Host in a separate process.
They ran on Ruby 3.4.12 and 4.0.9 with the frozen Async bundle, on ephemeral
localhost ports, on one Linux x86_64 machine. The numbers are indicative, not
baselines; #35 owns performance baselines.

**Cross-thread handoff works.** A caller thread pushing to a `Thread::Queue`
wakes a reactor task blocked in `pop` through Async's fiber scheduler. Generated
clients worked without changes.

| Ruby 3.4.12, 64-byte echo | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| 1 fiber in the caller's reactor | 1189 req/s | 0.67 ms | 2.51 ms |
| 1 thread through the prototype | 1179 req/s | 0.77 ms | 1.62 ms |
| 8 fibers in the caller's reactor | 1983 req/s | 4.07 ms | 6.44 ms |
| 8 threads through the prototype | 1824 req/s | 4.57 ms | 6.73 ms |

Ruby 4.0.9 and 64 KiB messages showed the same pattern. The single-process
Ruby server is probably the limit at 8-way concurrency.

**Fork works with a per-process engine.** The prototype keyed its engine to
`Process.pid`. After the parent had made a call and held a live connection, a
forked child made a call. The child started its own owner thread and a new TCP
connection. After the child exited normally, the parent kept using its original
connection. This held for h2c and verified TLS on both Rubies.

**Deadlines and interruption cross the boundary.** Over h2c and TLS on both
Rubies:

- A 200 ms deadline against a 2 s handler returned `deadline_exceeded` after
  201 ms.
- `Timeout.timeout(0.2)` around a call without a deadline unblocked the caller
  after 200 ms, and so did `Thread#raise` while the caller was blocked.
- In each case the server handler observed cancellation, a concurrent sibling
  call on the same connection completed, and later calls reused that connection.

The `Thread#raise` check used a short sleep to let the victim block. It shows
the mechanism works, not that every interleaving is safe; the implementation
tests must synchronize on observable progress.

**GVL contention costs less than with NetHTTP.** MRI hands the GVL to a thread
waking from blocking I/O only after the running thread's quantum, 100 ms by
default. Each blocking wakeup on the reactor thread can therefore wait that long
when other threads run Ruby code. The spike measured serial call latency while
four other threads ran Ruby code. The NetHTTP rows use a minimal Ruby HTTP/1.1
echo peer, because the Async Host is H2-only. Both peers are separate processes,
so the comparison mainly reflects client-side blocking points.

| Serial call p50 | No load | 4 threads at 50% CPU | 4 CPU-bound threads |
| --- | --- | --- | --- |
| Prototype, H2 (3.4.12 / 4.0.9) | 0.9 / 0.8 ms | 46 / 46 ms | 1209 / 1210 ms |
| NetHTTP, a new connection per call (3.4.12 / 4.0.9) | 0.5 / 0.4 ms | 87 / 71 ms | 2417 / 2016 ms |
| Prototype with `RUBY_THREAD_TIMESLICE=10` | 0.8 / 0.8 ms | 30 / 30 ms | 121 / 121 ms |
| NetHTTP with `RUBY_THREAD_TIMESLICE=10` | 0.5 / 0.5 ms | 60 / 50 ms | 242 / 201 ms |

About 12 GVL waits per call add up to 1.2 s under full contention. Fewer
blocking wakeups per call reduce this directly, so the implementation should
count and minimize them. Without CPU load, NetHTTP's plaintext localhost
connection is cheaper than one cross-thread round trip. With TLS, a remote
peer, or any CPU load, connection reuse should outweigh that cost; no TLS
comparison was measured.

**Bidi from plain threads needs a core change.** Core binds each stream session
to the thread and scheduler that opened it
(`lib/connectrpc/streaming/session.rb:113-117`). A plain-thread caller cannot
read and write the same stream from two threads. This is why client and bidi
streaming are a later phase.

## Part 1: connection manager for the Async H2 transport (#28)

### Shape

Replace the H2 transport's `Async::Pool::Controller` with a private,
reactor-owned connection manager. The HTTP/1.1 transport set the precedent with
its private lease manager. The pool cannot provide what #28 needs, because its
constructor runs connection setup inside the first caller's task. That is why
setup currently runs under the initiating RPC's lifecycle
(`gems/connectrpc-async/lib/connectrpc/async/transport.rb:242-268`).

The manager owns:

- the active connections, each with its peer's concurrent-stream limit;
- the draining connections after GOAWAY;
- at most one connection attempt at a time, run as its own task under the
  transport lifetime, outside any RPC's lifecycle context;
- the backoff state and its timer;
- the readiness waiters, with admission counts and admitted request bytes; and
- the published connection state and events.

The core seam does not change. `call`, `open_stream` and `open_grpc_stream`
still take `TransportRequest` and `StreamRequest`, and core still owns codecs,
deadlines and cancellation arbitration.

### Public surface

```ruby
policy = ConnectRPC::Async::ConnectionPolicy.new(
  backoff: ConnectRPC::Async::Backoff.new(base: 1.0, multiplier: 1.6, jitter: 0.2, max: 30.0),
  wait_for_ready: false,
  max_pending_bytes: 16 * 1024 * 1024,
  retry_refused: true,
  max_draining: 1, drain_timeout: nil
)
transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy,
                                             connection_observer: ->(event) { ... })
transport.connection_state # => :idle, :connecting, :ready, :transient_failure, :closing or :closed
```

- `ConnectionPolicy` and `Backoff` are frozen value objects with the defaults
  shown. New knobs can be added later without widening constructors.
- Every feature can be tuned or turned off:

  | Setting | Default | Valid values | Turned off by |
  | --- | --- | --- | --- |
  | `backoff` | `Backoff.new` | a `Backoff` or `nil` | `nil` |
  | `Backoff#base` | 1.0 s | positive seconds | not applicable |
  | `Backoff#multiplier` | 1.6 | at least 1; 1 gives a constant delay | not applicable |
  | `Backoff#jitter` | 0.2 | `0...1` | `0` |
  | `Backoff#max` | 30.0 s | at least `base` | not applicable |
  | `wait_for_ready` | `false` | boolean; `true` requires a `backoff` | `false` |
  | `max_pending_bytes` | 16 MiB | positive integer or `nil` | `nil`; `max_requests` still applies |
  | `retry_refused` | `true` | boolean | `false`; refused streams fail with `unavailable` |
  | `max_draining` | 1 | nonnegative integer | `0`; a draining connection keeps its active slot |
  | `drain_timeout` | `nil` | `nil` or positive seconds | `nil` |

- `wait_for_ready: true` with `backoff: nil` raises `ArgumentError`. Waiting
  through failures with no delay would retry connections as fast as the peer
  refuses them, and a refusal is nearly instant on a LAN.
- `Transport.new` and `ThreadedTransport.new` both default to
  `connection_policy: ConnectionPolicy.new`. There is no `nil` legacy mode, so
  the manager has one code path and one set of tests.
- `connection_observer:` is optional. It receives frozen `ConnectionEvent`
  values on the owner reactor and must return quickly. Exceptions it raises are
  rescued and ignored, like cancellation listeners.
- `connection_state` reads a published snapshot. It is an observation, not a
  promise that the next call succeeds.

Per-call wait-for-ready is left out. It would need a new core call option, new
`TransportRequest` fields and generated caller changes.

### States

`connection_state` reports the manager's state. The names follow gRPC so Go
users recognize them.

| State | Meaning | Leaves when |
| --- | --- | --- |
| `idle` | No usable connection and no attempt. The constructor starts here; no socket is opened before demand. | A call arrives: `connecting`. |
| `connecting` | One attempt is in progress. | Readiness: `ready`. Failure: `transient_failure`, or `idle` when `backoff` is `nil`. |
| `ready` | At least one active connection accepts new streams. | The last active connection fails or retires: `idle`, or `connecting` if calls are waiting. |
| `transient_failure` | The last attempt failed and the backoff timer runs. | The timer fires: `connecting` if wait-for-ready calls are waiting, otherwise `idle`. |
| `closing`, `closed` | `close` was called or the owner exited. | Terminal. |

Readiness means TCP connect, TLS with hostname and ALPN `h2` verification, the
client preface, and receipt of the server's first SETTINGS frame. TCP connect
alone does not count. Readiness resets the backoff. Today's setup already
reaches this point: the pinned protocol-http2 client preface reads the server's
first frame and requires it to be SETTINGS (`client.rb`,
`send_connection_preface`).

### Call admission and waiting

1. **Admission.** Each call reserves one admission slot from `max_requests`.
   A unary call also reserves its encoded request bytes
   from `max_pending_bytes`. For POST these are the body bytes. For GET they
   are the request-target bytes, because core moves the encoded message into
   the query and sends an empty body (`lib/connectrpc/client.rb:174-179`). If
   either budget is exhausted, the call fails immediately with `unavailable`.
   The reservation lasts until the call's terminal outcome. Streams reserve a
   slot but no bytes, because their output is already bounded to one queued and
   one in-flight fragment.
2. **Acquisition.** If an active connection has free stream capacity, the call
   takes it. Otherwise the outcome depends on the state:
   - `idle`, or every active connection at its stream limit with spare
     `connection_limit`: start the shared attempt, or join the one in progress,
     and wait for it.
   - `connecting`: wait for the current attempt.
   - `transient_failure`: fail fast with `unavailable`, unless wait-for-ready
     is set.
   - all connections at their stream limit with no spare capacity: wait for
     stream capacity, as the pool does today.
3. **Waiting.** A waiter blocks on a condition until its own deadline or
   cancellation, then leaves. Fail-fast callers wait for at most one attempt; if
   it fails, they fail. Wait-for-ready callers stay through backoff and later
   attempts until their deadline. With a nil deadline they wait until
   cancellation or `close`.
4. **Demand disappearing.** Leaving never cancels the shared attempt. An attempt
   already in progress runs to completion within `connect_timeout`, even after
   its last waiter leaves. A connection that becomes ready then stays as an idle
   active connection for the next call. No new attempt starts without a waiting
   call. A backoff timer keeps running after the last waiter leaves, so the
   state stays `transient_failure` and fail-fast calls keep failing until it
   fires. When it fires without waiters, it starts nothing and the state becomes
   `idle`. Only `close` cancels an in-progress attempt or a pending timer.
5. **Dispatch guard.** Under the connection's write guard, the call rechecks its
   deadline and cancellation and checks that the connection is still reusable.
   If the connection died before any request bytes were written, the call
   returns to acquisition. This is not a replay, because nothing was sent. Once
   the request headers start to be written, the outcome may be ambiguous and the
   call is never resent, apart from the refused case in Part 3. Rerouting
   changes today's behavior, which an existing spec asserts: a connection that
   closes at the dispatch guard currently fails the call, and only a later call
   reconnects (`gems/connectrpc-async/spec/stream_cleanup_spec.rb:136-175`).
   PR A rewrites that spec so the same call succeeds on a new connection.

### Multiple connections

`connection_limit` allows more than one active connection, in `Transport` and
`ThreadedTransport` alike. The manager opens another connection only when every
active connection has reached its peer's concurrent-stream limit and fewer than
`connection_limit` exist. Otherwise a call reuses the first connection with
spare stream capacity. Today's pool grows the same way: `async-pool` 0.12.0
reuses the first resource with spare capacity and creates one only when none
has any (`async-pool-0.12.0/lib/async/pool/controller.rb`,
`acquire_or_create_resource`). Attempts stay coalesced, one at a time.

Extra connections therefore add stream capacity, not load spreading. Spreading
calls across connections, for example to reach several backends behind an L4
load balancer, is a balancing policy and stays a non-goal. In
`ThreadedTransport`, every connection lives on the one owner thread, so TLS,
HPACK and framing for all of them share that thread and the GVL. More
connections add no CPU parallelism; worker processes remain the axis for that.
The socket bound is `connection_limit + max_draining`.

### Backoff

After a failed attempt, the delay is:

```text
delay = min(base * multiplier ** (failures - 1), max) * (1 - uniform(0, jitter))
```

Jitter only shortens the delay, so `max` is a hard maximum: the default
never waits longer than 30 s, and capped delays still spread between 24 s and
30 s. This differs from the gRPC spec, which applies symmetric jitter after the
cap and can exceed it by the jitter fraction. Clamping symmetric jitter at the
cap would instead make every capped delay identical and remove the spread
across processes.

`failures` counts consecutive failed attempts. The manager schedules the delay
on the monotonic clock. A burst of calls does not reset it. When the timer fires
with no waiters, the state returns to `idle`, but `failures` is kept, so the
next attempt that fails backs off from where it left off. Readiness resets
`failures` to zero. GOAWAY is not a failure and does not count. The base,
multiplier and jitter fraction follow the
[gRPC connection backoff spec][grpc-backoff]; the cap is 30 s rather than 120 s.
With fail-fast callers, the cap bounds how long calls keep failing after the
server returns, before the next attempt.

With `backoff: nil` there is no timer and no `transient_failure` state. A failed
attempt fails only the calls waiting on it and returns the state to `idle`, so
the next call starts a new attempt at once. This matches today's pool. The
`:attempt_finished` event still reports each failure.

### Observations

`ConnectionEvent` has a fixed set of fields:

- `type`: `:state_changed`, `:attempt_finished` or `:admission_rejected`;
- `state` and `previous_state`;
- `outcome` for attempts: `:ready`, `:refused`, `:timeout`, `:tls`,
  `:protocol`, `:closed` or `:other`;
- `duration` in monotonic seconds; and
- counts of `active`, `draining`, `pending_calls` and `pending_bytes`.

Events carry no addresses, certificates, payloads, URLs or exception text.
Shared attempts run outside any RPC's lifecycle, so tracing never parents a
connection attempt to whichever call triggered it.

### Close

`close(grace:)` keeps its current contract and adds four steps: it stops
admission, wakes every readiness waiter with `unavailable`, cancels the backoff
timer and any attempt, then drains active and draining connections within the
grace. Repeated or concurrent close calls share the first outcome, as today.

## Part 2: `ThreadedTransport` (#29)

### Caller experience

```ruby
require "connectrpc/async"

# Boot, for example in a Rails initializer. No thread or socket starts here.
RPC = ConnectRPC::Async::ThreadedTransport.new(origin: "https://rpc.internal:8443")
ECHO = Example::V1::EchoService::Client.new(
  client: ConnectRPC::Client.new(base_url: "https://rpc.internal:8443", binding: binding, transport: RPC)
)

# Any request thread.
ECHO.echo(message, timeout: 2)

# Shutdown, for example in Puma's on_worker_shutdown.
RPC.close(grace: 5)
```

The constructor takes `origin:, ca_file: nil, certificate_chain: nil,
private_key: nil, connection_limit: 1, max_requests: 128, connect_timeout: 10,
connection_policy: ConnectionPolicy.new, connection_observer: nil`, the same
connection options as `Transport`.

The constructor validates every argument and compiles the TLS snapshot on the
calling thread, so a configuration error raises at boot. It starts no thread
and opens no socket. `Internal::TLS.client` is plain OpenSSL and needs no
reactor (`gems/connectrpc-async/lib/connectrpc/async/tls.rb:82-100`).

Public methods are `call`, `streaming_capabilities` with `open_stream`,
`grpc_capabilities` with `open_grpc_stream`, `close(grace: 5)` returning a
boolean, `connection_state` and `inspect`. All of them can be called from any
thread.

### Ownership

```text
caller threads (Puma)                    owner thread "connectrpc-async"
--------------------------               ---------------------------------
core Client: deadline, encode            Async reactor
ThreadedTransport#call                     Transport (manager, H2, TLS)
  admission (count, bytes)   -- inbox -->  one task per command
  wait on reply mailbox      <-- reply --  TransportResponse or error
core Client: decode, commit
```

- **Engine.** The engine pairs the owner thread with its reactor and `Transport`.
  The first call in a process starts it. Each later call compares the engine's
  recorded pid with `Process.pid` before using it.
- **Inbox.** A `Thread::Queue` carries commands to the reactor. Its length is
  bounded by admission: each admitted unary call has at most one command
  outstanding, each stream has at most one per direction, and `close` adds one.
- **Reply mailbox.** Each call has its own mailbox. The reactor posts exactly one
  terminal result, a `TransportResponse` or an error, to it. The mailbox has
  two owners, the reactor and the caller, and a small mutex-guarded state
  records which of them has finished. A caller that abandons the call marks the
  mailbox abandoned, and the reactor then drops its result instead of posting
  it.
- **Encoding and decoding.** Core already encodes the request before calling
  the transport and decodes after it returns, so both stay on the caller thread.
  Only opaque bytes and frozen header maps cross threads.
- **Reentrant use.** A call from the owner thread itself, such as from a
  `connection_observer`, would deadlock. It raises `LifecycleError` immediately.

### Deadline and cancellation

Core creates the deadline before encoding, so one budget covers encoding,
admission, the inbox wait, acquisition, connection setup, wire I/O and decoding.

- **Reactor side.** The command carries the core `CallCancellation`, composed
  with a per-call interrupt signal. The existing transport enforces the
  deadline and cancellation through its cross-thread Promise path, which resets
  the stream and keeps siblings alive
  (`gems/connectrpc-async/lib/connectrpc/async/transport.rb:165-207`).
- **Caller side.** With a finite deadline, the caller waits for the reply for
  the remaining time plus the transport's 100 ms cleanup allowance. If no
  reply arrives by then, the reactor is stalled, for example by blocking native
  code. The caller fires the interrupt signal and raises `deadline_exceeded`.
  With a nil deadline, the caller waits until the reply, its cancellation token,
  or an interruption.
- **Interruption.** Any exception that reaches the waiting caller fires the
  interrupt signal and is re-raised unchanged. This includes `Timeout::Error`,
  Rack::Timeout's exceptions, `Interrupt`, and `Thread#kill` through `ensure`.
  The caller marks its mailbox abandoned and does not wait for the reactor to
  confirm. The stalled-reactor `deadline_exceeded` path abandons the mailbox
  the same way.
- **Exception masking.** `Thread.handle_interrupt(Object => :never)` protects
  admission, inbox push and signal firing. Asynchronous exceptions are allowed
  only during the blocking wait, so a `Thread#raise` cannot leak an admission
  slot or skip the cancel.
- **Releasing admission.** The admission slot and bytes are released only when
  both owners are done: the reactor has reported the terminal outcome, and the
  caller has taken the result or abandoned the call. Whichever side finishes
  second releases. Releasing when the caller leaves would let interrupted
  callers exceed `max_requests` while their streams are still being cleaned up.
  Releasing when the reactor finishes would let paused callers hold completed
  responses outside the bound. Completed responses waiting in mailboxes
  therefore stay within `max_requests` responses of at most `response_limit`
  bytes each.
- **Instrumentation.** The reactor sees the RPC lifecycle through a detachable
  proxy. After the caller leaves, the reactor's `record_failure` and `activate`
  calls become no-ops. Core's `Instrumentation::Handle` is not built for
  concurrent mutation, and the caller may already have finished processing.

### Fork safety

Puma cluster mode, Unicorn, Pitchfork and Spring fork after boot. Only the
forking thread survives in the child.

- **Per-process engine.** The engine records the pid that started it. When a
  call sees a different pid, it builds a fresh engine for the child, with new
  queues, a new lock, new admission counters and a new owner thread. The pid
  check uses a single instance variable read, so no lock is taken that the
  parent's dead threads might hold.
- **No inherited I/O.** The child never closes, drains or writes to inherited
  engine resources. It sends no GOAWAY and no TLS `close_notify`, and it does
  not abort the parent's reactor. It drops the references. Garbage collection or
  process exit closes the child's copies of the descriptors, which leaves the
  parent's connection open, as the spike showed.
- **Shared configuration.** The compiled TLS snapshot is immutable and is
  reused in the child. Fork tests over TLS and mTLS must prove that reuse.
- **Close across fork.** `close` in a child affects only the child's engine. A
  transport closed before the fork stays closed in every process.

Applications do not need an `after_fork` hook. Constructing the transport in an
initializer and first using it in a worker is the intended pattern.

### Lifecycle

- **Close.** `close(grace: 5)` can run on any thread except the owner thread,
  where it raises `LifecycleError` like any other reentrant use. It stops
  admission on the caller side immediately, so new calls fail with
  `unavailable`. The grace starts when `close` is called. Close posts a `close`
  command whose deadline is that start plus the grace. The engine runs the
  existing graceful `Transport#close` with whatever grace remains when it
  dequeues the command: drain, GOAWAY when empty, forced teardown when the grace
  expires. Then the owner thread exits and is joined. Close returns the drain
  outcome, and concurrent callers share it. A transport that never started
  returns `true`. Closed is terminal.
- **Stalled owner during close.** The closing caller waits for the outcome until
  the grace plus the 100 ms cleanup allowance has passed. If the owner has not
  finished by then, because native code or a non-yielding operation blocks the
  reactor, close fires every pending call's interrupt signal, records the
  outcome `false`, and returns without joining the thread. The owner aborts with
  no grace left when it resumes, then exits. Close cannot interrupt the blocking
  native code itself; this is the same cooperative limit the Async README
  documents.
- **Interrupted close waiter.** An exception that reaches a waiting closer is
  re-raised unchanged. Shutdown continues on the owner thread, and other closers
  still receive the shared outcome.
- **Process exit.** The transport adds no `at_exit` hook. Ruby kills the owner
  thread at exit, and the reactor's unwinding aborts owned sockets raw-first.
  Callers that want a graceful drain call `close` from their server's shutdown
  hook.
- **Owner thread failure.** If the owner thread dies unexpectedly, its
  top-level `ensure` posts `unavailable` to every pending mailbox, the front
  discards the engine, and an observer event reports it. The next call starts a
  new engine.

### Streams over the boundary (PR D)

The user's stream block keeps running on the caller thread. Core's stream
session therefore keeps its thread affinity, and no application code moves to
a hidden thread.

The reactor runs `transport.open_stream(request) { |exchange| ... }`, or
`open_grpc_stream` for native gRPC, inside a task that serves one stream
mailbox. The caller receives an exchange proxy. The Connect proxy forwards
`response_head`, `read`, `write`, `close_send`, `finish` and `abort`. The gRPC
proxy also forwards `response_trailers`, which core needs to validate gRPC
completion and recover remote errors (`lib/connectrpc/streaming/strategy.rb:67-90`).
The proxy keeps the engine's response-EOF gate on trailers
(`gems/connectrpc-async/lib/connectrpc/async/stream_exchange.rb:305-309`), with
repeated fields intact and trailers kept separate from the initial headers.
Each proxy method posts one command and waits for its result.

- **Bounds.** Each stream has at most one command in flight. A `read(n)`
  returns at most `n` bytes, so retention stays within the existing ingress and
  output bounds. Admission follows the same two-owner rule as unary calls.
- **Out-of-band cancellation.** The mailbox serves commands one at a time, so a
  queued `abort` cannot reach a `read` or `write` that is blocked on the
  reactor. On caller interruption or a proxy operation's stalled-reactor expiry,
  the caller therefore fires the stream's interrupt signal first. Through the
  existing cancellation Promise, that wakes the blocked operation, resets the
  stream and keeps siblings alive
  (`gems/connectrpc-async/lib/connectrpc/async/transport.rb:95-110`). Only after
  firing the signal does the caller post `abort`, which coordinates the
  reactor's exit from the stream block. `StreamExchange#abort` alone only closes
  the output body (`stream_exchange.rb:272-274`).
- **Timeouts.** Timeouts passed to proxy operations get the same allowance and
  stalled-reactor fallback as unary calls.
- **Normal block exit.** The caller posts the terminal command, then waits for
  the reactor's `open_stream` or `open_grpc_stream` to return its actual
  outcome. The wait is bounded only as unary waits are: by the deadline plus the
  allowance, with no extra bound for a nil deadline. A cleanup failure the
  engine raises after the block returns must reach the caller. Native gRPC can
  do that (`transport.rb:74-85`). If the bounded wait expires first, the caller
  fails with `deadline_exceeded`. It never reports success without the
  engine's outcome.
- **Exceptional block exit.** The caller fires the interrupt signal, posts
  `abort`, abandons the mailbox and re-raises its original exception unchanged,
  without waiting for cleanup. Admission stays held until the engine finishes
  retiring the stream. The 100 ms cleanup allowance triggers destructive
  cleanup and does not mean retirement is complete, so the caller never treats
  it as a completion signal.
- **Capabilities.** PR D advertises Connect `[:server_stream]` and gRPC
  `[:unary, :server_stream]`. Native gRPC unary uses the gRPC stream exchange,
  so gRPC clients can use `ThreadedTransport` only from PR D on. Core already
  rejects unadvertised shapes before encoding or I/O.

**Later phase, client and bidi streaming.** These need one reader and one writer
operating concurrently. That requires core to allow a stream session to be used
from two caller threads, or a different blocking API, such as a request
enumerator that a reactor writer consumes. Either needs its own design and the
full-duplex gates in #24: early headers, blocked writes with concurrent reads,
half-close, bounded finish, and sibling isolation.

## Part 3: GOAWAY replacement and refused-stream retry (#28)

### GOAWAY

- **Graceful GOAWAY** (`NO_ERROR`). The connection stops taking new streams.
  Streams at or below the last stream id continue. If fewer than
  `max_draining` connections are draining, this one moves out of the active cap,
  so a replacement attempt can start while it drains. Replacing a connection is
  not a failure and does not grow the backoff. If the draining set is full, the
  connection keeps its active slot, which is today's behavior.
- **Socket bound.** The number of sockets never exceeds
  `connection_limit + max_draining`.
- **Drain timeout.** `drain_timeout`, nil by default, force-closes a draining
  connection after the given time. Its remaining streams fail with
  `unavailable` and are never resent. The count bound is the hard limit; the
  time limit is optional, because long-lived server streams can legitimately
  outlast any default.
- **Error GOAWAY.** The connection fails. Streams at or below the last stream id
  fail with `unavailable`, because they may have run.

### Refused streams

The pinned protocol-http2 0.29.2 closes two kinds of streams with
`Protocol::HTTP::RefusedError`: streams above a GOAWAY's last stream id
(`protocol-http2-0.29.2/lib/protocol/http2/connection.rb:278-292`) and streams
reset with `REFUSED_STREAM` (`stream.rb:253`). RFC 9113 defines both as never
processed ([§6.8][rfc-goaway], [§8.7][rfc-refused]).

With `retry_refused: true`, the default, a unary call whose stream is refused
before response headers is sent again once, within the same deadline. Unary request bodies are
complete strings, so resending them is exact. The retry goes back through
ordinary acquisition with the call's existing admission. It picks no particular
connection:

- **Standalone `REFUSED_STREAM`.** The connection stays healthy and reusable
  (`protocol-http2-0.29.2/lib/protocol/http2/stream.rb:248-259`), so the retry
  may use the same connection. A refusal never retires a connection and never
  aborts its accepted siblings.
- **Above GOAWAY's last stream id.** The connection is draining and accepts no
  new streams, so acquisition uses another active connection or the replacement
  attempt the GOAWAY rules allow.
- **No capacity.** If no connection can take the retry, the usual waiting rules
  apply: fail-fast calls fail with `unavailable` when the state is
  `transient_failure`, and all calls wait for stream capacity up to their
  deadline.

A second refusal fails the call with `unavailable`. Streams are never resent,
because the caller already holds the exchange and may have written messages.
This deliberately changes today's behavior, which an existing spec asserts:
refused and GOAWAY-refused unary streams currently fail with `unavailable`
(`gems/connectrpc-async/spec/stream_cleanup_spec.rb:95-133`). PR C rewrites
that spec to expect exactly one more transmission and one server invocation.

## Non-goals

- RPC retry or replay policies, hedging, service config, load balancing
  (including spreading calls across several connections), xDS,
  periodic DNS re-resolution, keepalive PINGs and background warm-up. Each needs
  its own decision; #24 lists the reasons.
- Per-call wait-for-ready, which needs core API and generated caller changes.
- An HTTP/1.1 engine for `ThreadedTransport`. The engine boundary is internal,
  so `HTTP1Transport` could be added later without changing the public shape.
- NetHTTP keep-alive or pooling.
- Client and bidi streaming from plain threads in this phase.
- Ractors, JRuby, TruffleRuby, Windows and macOS qualification.
- Changes to core codecs, cancellation arbitration or the transport SPI.

## Failure paths and risks

| Risk | Handling |
| --- | --- |
| Reactor blocked by native code, such as DNS or a codec | Caller deadline plus allowance fires; the caller fails and the interrupt signal cancels when the reactor resumes. Nil-deadline calls wait, as with today's Transport. Documented as a cooperative limit. |
| `Thread#raise` lands during bookkeeping | Interrupts masked except during the blocking wait. |
| Interrupted caller while its stream is still cleaning up | Admission held until both the reactor's outcome and the caller's consumption or abandonment. |
| Paused caller holding a completed response | Same two-owner rule: completed replies stay within `max_requests` responses of at most `response_limit` bytes. |
| Blocked stream read or write when the caller is interrupted | Interrupt signal fired out of band before `abort` is queued. |
| Reactor stalled during `close` | Close returns `false` after grace plus allowance, fires pending interrupts and leaves the owner to abort when it resumes. |
| Late lifecycle mutation after the caller left | Detachable lifecycle proxy. |
| Fork while calls are in flight or while the engine starts | Child ignores inherited state; tests fork in each of these states. |
| Server down at boot or during a deploy | Fail-fast callers get `unavailable` quickly; demand-driven attempts with backoff recover; wait-for-ready callers wait until their deadline. |
| Half-open connection with no FIN | First call after the failure gets `unavailable` and is not resent, because the write may have reached the server. Later calls reconnect. Keepalive PINGs remain a non-goal. |
| Long-lived stream on a connection that received GOAWAY | Draining slot plus optional `drain_timeout`; never more than `connection_limit + max_draining` sockets. |
| GVL starvation under CPU-heavy threads | Encoding stays on callers; implementation minimizes reactor wakeups per call; documentation explains `RUBY_THREAD_TIMESLICE`. |
| Owner thread crash | Pending callers get `unavailable`; next call restarts the engine. |
| TLS identity change | Unchanged: replace the transport. Reconnects reuse the compiled snapshot and never weaken verification. |

## Delivery and verification

Each PR runs focused specs while it is being built. Before handoff it runs
`async:check`, `async:verify`, `rbs:check` and `rbs:package` on both pinned
Rubies, plus `verify` and `rack:verify`. Each PR updates the owning signatures
and the supported/SPI inventory, and the Async README, `docs/support.md` and
`docs/design.md` for the capability it delivers. Tests synchronize on accepted
connections, frames, handler entry and completion signals, never fixed sleeps.

### PR A: connection manager core

Scope: the private manager, `ConnectionPolicy`, `ConnectionEvent`,
`connection_policy:` and `connection_observer:` on `Transport`, and
`connection_state`. Shared attempts, backoff, wait-for-ready, the byte budget
and the dispatch guard reroute, all on by default. The Async README's
`Transport` section changes with it, because it says a failed call never
reconnects and only a later call opens a fresh connection.

| Concern | Evidence |
| --- | --- |
| Backoff growth, hard cap including jitter, jitter range and reset only at SETTINGS readiness | Deterministic specs with an injected clock and random source |
| Turning features off | `backoff: nil` starts a new attempt on the next call after a failure, with no timer or `transient_failure`; `max_pending_bytes: nil` admits by count only; `wait_for_ready: true` with `backoff: nil` raises `ArgumentError`; every invalid setting is rejected at construction |
| No reset from bursts; no new attempt without a waiting call; an in-progress attempt finishes within `connect_timeout` after its last waiter leaves; a timer that fires without waiters starts nothing; `close` cancels both | Same deterministic specs |
| Shared attempt survives a short-deadline or cancelled initiator | Real TLS peer that stalls the handshake under a gate; second waiter completes |
| Recovery after refused connections | Peer refuses, then listens. Concurrent wait-for-ready waiters share paced attempts. A frame-observing peer shows that expired and cancelled waiters sent no request HEADERS or DATA, which handler counts alone cannot prove. Survivors dispatch once. Fail-fast callers fail during `transient_failure`. |
| Growth beyond one connection | A peer advertising `SETTINGS_MAX_CONCURRENT_STREAMS` of 2 with `connection_limit: 2`: the third concurrent call opens a second connection, the fifth waits for capacity, and no third socket opens |
| Count and byte limits during an outage | Admission rejects predictably for POST bodies and GET request targets; events report the rejection |
| Dead-before-dispatch reroute | Peer closes an idle connection; the next call uses a new one with one server invocation |
| No replay | POST and GET probes observe request HEADERS and DATA progress, then inject an ambiguous failure: a reset after headers, EOF after a partial or complete body, and connection loss after the handler ran. The failed call is not resent, judged by both transmitted frames and invocations; only a separately issued call reconnects. |
| Existing behavior | Existing pool and lifecycle specs pass under the default policy. The pre-dispatch closure spec is rewritten for rerouting; every other no-replay assertion is kept. |
| Attempts outside RPC context | OTel bundle spec: no attempt span is parented to the first RPC |
| Close | Wakes waiters, stops timers and attempts, keeps existing outcomes |

### PR B: threaded Connect unary

Scope: `ThreadedTransport` with Connect unary, including GET; fork safety;
close; interruption; owner-thread failure.

| Concern | Evidence |
| --- | --- |
| Multiplexing from many threads | Generated client on 8 threads over h2c, TLS and mTLS; the server observes one connection. With a low peer stream limit and `connection_limit: 2`, both connections serve calls from the same owner thread. |
| Deadline covers the inbox wait and a stalled reactor | Test hook holds the reactor; caller returns `deadline_exceeded` within the allowance and admission is released after the reactor resumes |
| Admission covers completed replies | A paused caller holds a completed reply; a replacement call cannot take its slot until the caller consumes it; an abandoned reply is dropped and its slot released |
| Interruption | `Timeout.timeout` and `Thread#raise`: the server observes `RST_STREAM(CANCEL)`, the sibling completes and the connection is reused |
| Fork | Fork while idle, with calls in flight on other threads, and during engine start, over TLS and mTLS. The child gets a new connection; the parent's connection keeps working after the child exits. |
| Outage recovery end to end | Server stops and restarts; fail-fast and wait-for-ready behavior as designed |
| Close and lifecycle | Graceful and forced drain, concurrent close, calls after close, close in a child, reentrant `call` and `close` from the owner thread |
| Close with a stalled owner | Test hook holds the reactor; close returns `false` after grace plus allowance and fires pending interrupts; the owner aborts and exits when released. Interrupting one of two concurrent closers re-raises in that closer only. |
| Owner thread failure | Injected failure; pending callers get `unavailable` promptly; the next call restarts |
| Interoperability and packaging | `async:interop` unary cases through `ThreadedTransport`; installed generated callers and RBS on both Rubies |

### PR C: GOAWAY replacement and refused retry

Scope: draining connections, replacement attempts, `drain_timeout` and the
single refused-unary retry, all on by default. The Async README's "No retries"
statement changes to name the refused exception.

| Concern | Evidence |
| --- | --- |
| Replacement while draining | Peer sends graceful GOAWAY with a live long stream and several new calls; a replacement opens; the socket count stays within `connection_limit + max_draining` |
| Full draining set | A second GOAWAY while the draining set is full keeps today's slot behavior |
| Drain timeout | Remaining streams fail with `unavailable` and are not resent |
| Refused unary retry | `REFUSED_STREAM` and above-last-id streams, for POST and GET, are sent exactly once more; server invocation count is one |
| Refusal capacity | Standalone `REFUSED_STREAM` with a live sibling on the default single connection: the retry reuses that connection and the sibling completes. Above-last-id refusal with the draining set full: the retry waits for capacity or fails per the waiting rules, and no accepted sibling is aborted. A second refusal fails with `unavailable`. |
| No stream retry | Refused streams fail with `unavailable` |
| `retry_refused: false` | Refused unary calls fail with `unavailable` after one transmission |
| Error GOAWAY | At or below the last id: fail, no retry |
| Existing behavior | The refused/GOAWAY no-replay spec is rewritten for the single retry; no-replay assertions for ambiguous failures are kept |

### PR D: stream proxy

| Concern | Evidence |
| --- | --- |
| Connect server streaming and gRPC unary and server streaming from threads | Generated clients over h2c, TLS and mTLS, both codecs where applicable |
| Early headers, bounded reads and finish | Proxy operations against controlled peers |
| Caller interruption mid-stream | Interruption during a gated nil-deadline `read` and during a blocked `write`: the stream is reset, a sibling keeps progressing, the caller's original exception is re-raised and admission is released after retirement |
| gRPC trailers | `response_trailers` through the proxy for ordinary and trailers-only responses, success and error, with repeated fields and EOF gating |
| Completion outcome | Delayed retirement and a late gRPC cleanup failure after the block returns reach the caller; the caller never reports success without the engine's outcome |
| Stalled consumer | Another thread's stream on the same connection keeps progressing |
| Conformance | Official native client conformance for Connect U/SS and gRPC U/SS run through `ThreadedTransport`, as a separate inventory |

## Documentation in this PR

This PR adds only this design. `docs/support.md` stays accurate: it says there
is no ordinary-thread persistent facade, and none is implemented yet. The
README and Async README still point to #29 for ordinary-thread persistence,
which also stays accurate. Each implementation PR updates those documents for
what it delivers.

## Open questions

None block the design. Review changed one detail of an accepted default: Jim
chose ±20% jitter with a 30 s cap, and this design makes the jitter shorten
delays only, so 30 s is a hard maximum and capped delays still spread out (see
[Backoff](#backoff)). Symmetric jitter would exceed the cap by up to 6 s.
Revert to symmetric jitter if matching gRPC exactly matters more.

This is worth checking during implementation:

- Whether the owner thread should be named per origin, such as
  `connectrpc-async rpc.internal:8443`, for easier debugging. The current
  recommendation is yes, without credentials or paths.

[grpc-backoff]: https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md
[rfc-goaway]: https://www.rfc-editor.org/rfc/rfc9113.html#section-6.8
[rfc-refused]: https://www.rfc-editor.org/rfc/rfc9113.html#section-8.7
