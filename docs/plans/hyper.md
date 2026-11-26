# Hyper: connectrpc-hyper (reactor-free Rust client transport)

Status: approved; implementation in progress on `feat/connectrpc-hyper`
(issue [#171](https://github.com/jimeh/connectrpc-ruby/issues/171)). The gem,
CI, cross builds, typecheck/package wiring, the versioned-but-unpublished
release train entry (D15), the proxy (D18) and the documentation are in
place. The contract below is kept as approved. Base: `d521b60`, rebased
onto `ccffd92` (#159 merged). PR title
`feat: add connectrpc-hyper, a reactor-free Rust client transport`.
The design follows the #171 spike (hyper 1.x/h2, rustls, tokio, magnus and
rb-sys). Jim approved building it and every recommendation below. A
cross-engine plan review (Codex) found 13 issues; all were checked against the
code and folded in (D3, D4, D5, D6, D7, D7a, D8, D9a, D10, D10a, D12, D13, D15,
D16, D17 and the verification map).

## Objective

Any Ruby thread can make Connect and native gRPC calls, all four call shapes,
over HTTP/2 (and Connect over HTTP/1.1) without an Async reactor. One shared
HTTP/2 connection per origin serves every thread, stays alive while idle and
recovers from server restarts, as a grpc-go channel does. `Thread#raise`,
`Thread#kill`, `Timeout`, deadlines and cancellation tokens stop a call at any
stage and reset its stream.

## Verified facts

- The spike prototype (`lib.rs`, `conn.rs`, `gvl.rs`, `runtime.rs`, Ruby
  `hyper.rb`) passed 120/120 interrupt cases, 224/224 interop checks per Ruby,
  streaming limits, sibling cancel, liveness (server PINGs, GOAWAY), fork and
  CONNECT proxying, on Ruby 3.4.12 and 4.0.9.
- Core's client SPI: `Transport#call(request)` returns a
  `TransportResponse`; streaming uses `streaming_capabilities` and
  `open_stream`, native gRPC uses `grpc_capabilities` and `open_grpc_stream`,
  each yielding an exchange with `response_head(timeout:)`, `read`, `write`,
  `close_send`, `finish(timeout:)` and `abort`; Connect heads are
  `StreamHead`, gRPC heads are `GRPCHead` (with the initial HEADERS
  `END_STREAM` bit) and gRPC exchanges add `response_trailers`. The unary
  `TransportResponse` has no trailers (`sig/connectrpc.rbs`). Core sends gRPC
  unary through `open_grpc_stream` with a `StreamRequest` that carries no
  cardinality.
- hyper's default `max_header_list_size` is 16 KiB, below core's 64 KiB gRPC
  field limit (`GRPCProtocol::FIELD_LIMIT`).
- hyper keepalive (`hyper-1.13.0/src/proto/h2/ping.rs`): the idle clock resets
  on response HEADERS, on DATA and trailers as the body is polled, and on PING
  ACKs. Server PINGs, SETTINGS, WINDOW_UPDATE and buffered but unpolled DATA do
  not reset it. Only the ACK proves liveness; `keep_alive_while_idle` gates
  pinging without open streams (strong count of the shared ping state). BDP
  and keepalive share one outstanding PING. hyper's BDP window cap is 16 MiB,
  as grpc-go's `bdpLimit`.
- aws-lc-rs (rustls 0.23.46, aws-lc-rs 1.19.0, aws-lc-sys 0.47.0) cross-builds
  in the unmodified `rbsys/*:0.9.130` images for all six target platforms plus
  `x64-mingw-ucrt` and `aarch64-mingw-ucrt`, without cmake or nasm. The four
  Linux gems passed the spike smoke run on their arch and libc. A Go 1.27
  default TLS server negotiated `X25519MLKEM768`. Gems grow to 4.1-4.8 MB; the
  glibc floor stays 2.29.
- rustls rejects a self-signed CA certificate used as the server certificate
  (`CaUsedAsEndEntity`), as well as CN-only and X.509 v1 certificates and
  single-label wildcards.
- `RB_NOGVL_OFFLOAD_SAFE` (Ruby >= 3.4 header) with async 2.46.0 and io-event
  1.22.3's opt-in worker pool (`ASYNC_SCHEDULER_WORKER_POOL=true` or
  `Async::Scheduler.new(worker_pool: true)`) runs the wait on a worker thread on
  Ruby 4.0.9: a sibling task kept ticking (29 ticks in 300 ms against 1), and
  `Task#stop`, a `CancellationSource`, the deadline, `Thread#raise` and
  `Thread#kill` landed in under 0.4 ms with the peer seeing the cancel.
  io-event cancels through the UBF and drains the work before re-raising, so
  stack data stays valid, but the re-raise unwinds through `rb_nogvl`: it must
  run under `rb_protect`. io-event builds the worker pool only with
  `rb_fiber_scheduler_blocking_operation_extract`, which Ruby 3.4 lacks.
  Ruby 3.4.12's `rb_nogvl` still calls a scheduler's `blocking_operation_wait`
  for an offload-safe body, handing it a plain proc
  (`rb_fiber_scheduler_blocking_operation_wait` in scheduler.c), and nothing
  there waits for the body before a cancellation unwinds the caller, so the
  flag is passed only on Ruby 4.0 and later (D4, correction round 5).
- PR #159 (merged as `ccffd92`) added NetHTTP `ca_file:`
  (appended to the default store), `certificate_chain:` (Array of PEM strings,
  leaf first), `private_key:` (PEM string), TLS 1.2 minimum, no session
  resumption, `proxy: :environment` by default through `ConnectRPC::Proxy`, and
  a default User-Agent. Its `ClientTLS` validator is a private constant.
- Release Please bumps each optional gem's `version.rb` through
  `.github/release-please-config.json` extra files; `ReleaseTrain::NAMES`
  (`scripts/release_train.rb`) is the published set.

## Decisions

- D1 Package: `gems/connectrpc-hyper/`, `require "connectrpc/hyper"`,
  `ConnectRPC::Hyper::Transport`. Runtime dependency `connectrpc` at the train
  requirement; the source gem adds `rb_sys` (`>= 0.9.130, < 1`, stripped from
  platform gems).
- D2 Crypto: rustls with the aws-lc-rs provider and `prefer-post-quantum`. No
  FIPS build.
- D3 Runtime: one lazily created, PID-checked multi-thread tokio runtime per
  process (two workers, no public setting), stored in an `AtomicPtr` slot with
  its owner PID, never a lock a fork can inherit. A cold caller builds the
  runtime on a short-lived `hyper-rt-start` thread and waits for it, GVL
  held, for at most 5 s; concurrent starts elect one runtime (compare-and-swap,
  losers shut theirs down). A start that fails or outlasts the bound raises
  `fatal` in its caller, which publishes nothing; a later use starts a fresh
  attempt, unless a start that finished late already published its runtime
  (see "Runtime start failure (#181)" under Failure paths and risks). Every
  process-wide native state, including the native-root cache and any initialisation in progress, is
  PID-scoped the same way. A forked child leaks inherited runtime state through
  `ForkGuard`, never shuts down inherited sockets, and closes (not shuts down)
  the inherited connection descriptors it can identify from a fork-safe
  registry, so a long-lived child does not keep the parent's connections open
  after the parent closes them. If that cannot be made fork-safe, document that
  inherited sockets stay open until the child exits. No current-thread mode.
- D4 Waits: `rb_nogvl(RB_NOGVL_INTR_FAIL | RB_NOGVL_OFFLOAD_SAFE)` under
  `rb_protect`, a UBF that only fires a signal, and a protected
  `rb_thread_check_ints` afterwards. `RB_NOGVL_OFFLOAD_SAFE` is passed only
  when the running Ruby is 4.0 or later, read once when the extension loads:
  4.0's blocking-operation API lets io-event's worker pool finish a running
  body before it re-raises a cancellation, while on 3.4 a scheduler's hook
  could unwind the waiting frame and leave the body's stack data dangling.
  Rust never calls Ruby from a worker or inside the nogvl body; data crossing
  the offload boundary is `Send`. A wait woken without a pending interrupt
  resumes the same operation state and never repeats a side effect (a
  `write` enqueues its bytes once, then waits for hyper to pull them). A
  `write` that finds the request body dropped (the stream ended) or the
  request stranded raises (#193): on HTTP/2, or once stranded, it waits,
  inside the same bounded wait, for the response task to publish its outcome
  (HTTP/1.1 does not wait: hyper also drops a body its own framing ended, a
  declared Content-Length or an HTTP/1.0 connection, while the response is
  pending), then raises the head's failure, or a body failure it can read
  without waiting and without taking the receive half from a reader, and
  otherwise forced teardown's phrase once `shutdown` fired, or a generic
  failure (`stream closed` on HTTP/2, `connection closed` on HTTP/1.1), which
  core treats as a send failure and recovers the server's status from. A
  write rejects a single chunk of 2 GiB or more, which h2 refuses outright,
  dropping the body while the response waits. The write in flight when the
  stream ends returns, as grpc-go's `SendMsg` does once it queued a message.
  hyper never tells the request body a reset's code, so a gRPC reset behind
  unread response DATA, or behind a reader holding the receive half, gets the
  generic failure. The
  read and write directions of one stream progress independently (core allows
  one reader and one writer concurrently, which offload makes real when the
  pool has a worker for each: async builds io-event's pool with one worker by
  default, which runs parked waits from different fibers one at a time), and
  `abort` signals and wins over busy operations instead of skipping when a
  lock is held. The close wait (D13) never offers offload; Ruby moves it to a
  helper thread instead.
- D5 Connections: `connections: :pooled` (default) keeps one *admitting* HTTP/2
  connection per origin per transport; a connection that received GOAWAY stops
  admitting, drains its existing streams and retires on its own while a new one
  admits. Pooled HTTP/2 dials belong to the pool, as in a grpc-go channel
  (Jim's decision in correction round 3): an origin has at most one shared
  dial, a task the driver owns, and every caller that finds no usable
  connection waits for it under its own deadline, cancellation and
  interrupts. Only the dial and `close` decide its outcome, and every waiter
  gets it: the pooled connection; `http/1.1` from ALPN (the clean connection
  is parked idle; each waiter uses a usable pooled or idle connection if one
  exists and otherwise dials its own, never another pool dial in that
  acquire, and a streaming waiter fails with the ALPN phrase; correction
  round 4 narrowed this from "one waiter takes it", since reusing a clean
  idle connection is ordinary pooling); or the failure, which is not cached.
  A waiter that leaves does not change the dial, so a call whose short
  deadline ends fails `deadline_exceeded` while the dial pools its connection
  for later calls. A pooled connection that ends (closes or stops admitting)
  before the server's HTTP/2 preface, its first non-ACK SETTINGS frame, fails
  the waiters of its dial with the connection's failure (its TLS failure,
  else `connection closed`) when nothing else usable is pooled, instead of
  starting another dial (#208): a server that ends each connection right
  after its handshake (TLS 1.3 rejecting a missing client certificate)
  otherwise kept a pooled caller dialing until its deadline. grpc-go treats
  a transport that never received the server preface as a failed connection
  attempt (`newHTTP2Client` waits for it), while a ready transport that
  drains or closes makes waiting RPCs re-pick; so a connection that closes or
  drains after the preface (a graceful GOAWAY) keeps the redial: its waiters
  join or start another dial, with no cap, as do the waiters of a connection
  the idle timeout closed. The tap (D12) records the preface once the whole
  SETTINGS frame has passed, as grpc-go reads the full frame; a server-frame
  reader that lost its place before one cannot vouch that none arrived and
  keeps the redial. Callers that did not wait for that
  dial evict it and dial as usual. The dial's outcome names its connection
  without a hyper sender, so a slow waiter never keeps an evicted connection
  open (D6).
  A second HTTP/2 connection never replaces a usable pooled one, including a
  retry's (D12).
  `:per_call` opens one per call. HTTP/1.1 mode dials per call and pools idle
  keep-alive connections, at most two per origin (Go's
  `DefaultMaxIdleConnsPerHost`); only a connection whose request and response
  both completed cleanly returns to the pool.
- D6 `idle_timeout: 1800` (grpc-go's 30 minutes) closes pooled connections with
  no calls in flight; nil disables. The idle timer claims the exact connection
  generation it observed idle, so it never closes a connection a caller has
  just checked out.
- D7 `connect_timeout: 10` (uniform with the connectrpc-async transports, Jim's
  decision; grpc-go's minimum connect timeout is 20 s) bounds DNS, TCP,
  proxy CONNECT, TLS and the HTTP/2 handshake as hyper completes it (preface
  written). hyper exposes no peer-SETTINGS boundary, so a peer that accepts and
  stays silent is bounded by the call deadline (and keepalive when set);
  document it. Each address attempt gets Go's `partialDeadline` share of the
  setup budget: `connect_timeout` alone for the shared HTTP/2 dial (D5), and
  the earlier of `connect_timeout` and the call deadline for a dial that serves
  one call. The call deadline always applies. `connect_timeout` must be finite
  positive seconds, with connectrpc-async's message (correction round 4): a
  pool dial outlives its callers, so it must end on its own, and one still
  running when its transport is garbage collected ends within
  `connect_timeout`.
  The cancellation registration exists before the first native wait, so a
  token cancels DNS, connect, TLS and a wait for the shared dial.
  The wire timeout headers (`grpc-timeout`, `connect-timeout-ms`) are
  recomputed from the remaining deadline at dispatch, after setup and on a
  retry, as the Async transport does.
- D7a Resolution: hostnames resolve through tokio's blocking pool
  (getaddrinfo), whose default 512-thread cap is close to Go's 500 cgo lookup
  threads. A stalled lookup keeps its thread after the caller leaves; document
  it. Go-style dual-stack fallback (300 ms) across address families.
- D8 `keepalive: nil | ConnectRPC::Keepalive.new(time:, timeout: 20,
  permit_without_calls: false)`, mapped to hyper's `keep_alive_interval`,
  `keep_alive_timeout` and `keep_alive_while_idle`. Document hyper's clock
  precisely: it measures response HEADERS, DATA and trailers as Ruby consumes
  them and PING ACKs, so it may ping earlier than grpc-go; DATA buffered while
  the peer went silent can move it later once consumed; without
  `permit_without_calls` an idle connection is never probed.
- D9 Flow control: `adaptive_window(true)` (BDP, capped at 16 MiB like grpc-go),
  explicit `max_send_buf_size`. No public window options.
- D9a Header limits: h2 0.4.20 marks a block over `max_header_list_size` (it
  counts pseudo-headers, `>=`) and stops retaining fields; HEADERS are
  rejected, but trailers pass through truncated (`recv.rs` `recv_trailers`).
  So the h2 cap must sit well above core's limit (grpc-go's 16 MiB default
  header list size), and the transport enforces core's exact 65,536-byte
  regular-field limit (`GRPCProtocol::FIELD_LIMIT` semantics) itself on
  received heads and trailers, plus outbound gRPC fields before dispatch as the
  Async transport does. Document the residual: a hostile block over 16 MiB can
  still truncate trailers.
- D10 TLS: `ca_file:`, `certificate_chain:`, `private_key:` with #159's types,
  validation and fixed errors (`Invalid TLS client configuration: <input>`),
  validated in Ruby with OpenSSL and passed to Rust as DER. Native roots
  (rustls-native-certs, honouring `SSL_CERT_FILE`/`SSL_CERT_DIR` at first use,
  loaded once per process) plus appended `ca_file` certificates. TLS 1.2
  minimum, resumption disabled, ALPN `h2, http/1.1` (or `http/1.1`), no SNI for
  IP literals. Errors never include paths or key material or carry an
  exception cause that does. Construction also imports the identity into the
  rustls provider, so an identity OpenSSL accepts but aws-lc-rs cannot sign
  with (Ed448) fails with the fixed `private_key` error. `SSL_CERT_FILE` or
  `SSL_CERT_DIR` replaces the platform store (rustls-native-certs semantics);
  `ca_file` appends to whichever roots were selected. TLS options on an
  `http://` request raise `ArgumentError, "TLS options require an HTTPS
  origin"` as #159 does.
- D10a ALPN: with `http2: true`, every streaming call (`open_stream`,
  `open_grpc_stream`) requires h2; if TLS negotiates `http/1.1` the call fails
  `unavailable` (`HTTP transport failed: ALPN negotiation failed`) before any
  request is queued, so the peer sees no application request. Connect unary
  `call` may proceed over HTTP/1.1. Callers that want HTTP/1.1 streaming set
  `http2: false`.
- D11 Fast unary path: Connect unary `call` is one native operation (connect or
  reuse, send, head, body under the response limit, trailers) under one GVL
  release. Core sends gRPC unary through `open_grpc_stream`
  (`lib/connectrpc/streaming/client.rb`), so it keeps exchange waits; cut its
  round trips where the exchange SPI allows (for example, open without a wait
  and let the first `write` or `response_head` connect). Streaming keeps
  per-operation waits.
- D12 Retry once, on a fresh connection, within the original deadline and with
  refreshed wire timeouts: (a) a request that never reached the wire, on any
  shape: one hyper hands back unsent from `try_send_request`, or one whose
  stream id is above the last stream id of a GOAWAY the peer sent, whose HEADERS never
  reached the socket and, when streaming, whose body yielded nothing; (b) a
  Connect unary `call`, before response headers, whose stream got a remote
  `REFUSED_STREAM` or whose stream id is above that last stream id
  (RFC 9113 6.8: unprocessed) and whose own error is h2's remote GOAWAY
  error. gRPC unary is not
  distinguishable from streaming at `open_grpc_stream`, so it gets only (a).
  This matches the Async transport's `retry_refused` scope. A stream at or
  below the last stream id is never replayed after a GOAWAY (#180); a remote
  `REFUSED_STREAM` still is, since RFC 9113 8.7 says that stream was not
  processed, as grpc-go treats it. h2 exposes no last stream id and hyper no
  stream id, so `wire.rs` derives both: a tap above TLS reads frame headers
  (the lowest last stream id the peer's GOAWAYs name, and the highest client
  stream id whose HEADERS h2 offered to the socket, written or not, since h2's
  `FramedWrite` offers its whole encoded buffer each time) and a body's first
  `size_hint`, which hyper 1.13's dispatch calls once right before h2 assigns
  the id, takes the connection's next stream index. The peer's GOAWAY is the
  authority on what it processed, as its REFUSED_STREAM is: the tap follows
  frame lengths as h2 does and does not repeat h2's frame validation, which
  depends on payloads and stream state the tap cannot see; a peer that lies
  about processing defeats any client anyway. A CONNECT request, or a GOAWAY
  that is malformed or raises the last stream id, disables the record for the
  connection.
  Error kind alone cannot decide: hyper reports a request dropped from its
  stream-slot wait, or failed right after `send_request`, with the same error
  whether or not the stream reached the wire. A peer that sends a 1xx and then
  disowns the stream contradicts RFC 9113 6.8 and is not guarded against, as in
  grpc-go. A call canceled before its retry is queued is never retried. When the pooled connection that refused
  or handed back the request still admits, it stays pooled and the retry dials
  a connection that serves only it (D5); an unusable one, including one that
  received GOAWAY, is evicted and the retry shares the pool dial.
  A stranded request (#208) also counts as (a) for Connect unary and is
  terminal, with the connection's failure, for streaming. tokio 1.54's
  unbounded `send` checks the channel is open, then pushes; the receiver's
  drop closes it, then drains what it can read (tokio-rs/tokio#7714). hyper
  queues each request and its reply callback in that channel, and the
  dispatcher (the HTTP/1.1 `Dispatcher`, HTTP/2's `ClientTask`) owns the
  receiver, so in hyper 1.12 a request pushed between the dispatcher's close
  and drain is never received and its envelope, whose drop would answer it,
  lives until the connection's last sender drops (the pool and sibling leases
  keep one). hyper 1.13.0 (hyperium/hyper#4222, #4242) closes the channel and
  drains it, waiting for a push under way, before the future of a dispatcher
  that shuts down completes, so such a dispatcher strands nothing (the
  adopting pull request records a reproducer run). The guard below stays as a
  safety net, since the receiver still drops as above for a future dropped
  unfinished (an aborted task) and for an HTTP/1.1 upgrade (a 101 response, or
  a 2xx reply to CONNECT other than 204), which completes without that drain,
  and the tokio race stays open. Each connection's owner of hyper's connection
  future fires a signal only after dropping it, which answers every request
  the dispatcher still held, and `send` races hyper's promise with that
  signal. After it fires, one more poll, outside tokio's cooperative budget
  (its oneshot checks the budget before its state), lets a ready answer win; a
  request still pending then is stranded on HTTP/1.1, and on HTTP/2 only when
  its mark has no index, since an indexed request belongs to h2 and its
  response task, which may still answer it during a graceful GOAWAY. A writer
  blocked on a stranded body returns, a later write raises the failure, and
  the head reports it.
  hyper 1.13.0 (hyperium/hyper#4275) changes how a request waiting in its
  HTTP/2 dispatcher for a stream slot fails when its connection fails; 1.12
  reported its dispatcher gone in every case. A connection error with a code
  other than NO_ERROR now gives the request only that code. A graceful close
  (NO_ERROR), or an error without a code such as an I/O error, gives a
  cancellation. A keepalive timeout still gives dispatcher gone. A
  cancellation and dispatcher gone both read as a closed connection, as
  before. `Failure::from_hyper` counts the bare code as a connection failure
  (`goaway`), as h2 reports the error to the connection's other streams and
  grpc-go reports any connection error (`unavailable`), not as a stream reset;
  the request still replays when the wire record shows it unprocessed and
  unsent.
- D13 `Transport#close(grace: 5) -> bool`, as the Async transports: stops
  admission (later calls fail `unavailable` with
  `HTTP transport admission is closed`), including callers waiting on a dial,
  lazy exchanges and pending retries; lets in-flight calls finish within grace,
  then aborts the rest and waits for the owned connection tasks to end (no
  detached tasks); true when drained, false after forced teardown. Repeated and
  concurrent calls share the first call's grace (reserved when it starts
  closing, so a later closer that reaches native close first still uses it)
  and outcome. Forced teardown refuses new tasks, then fires `shutdown`,
  then aborts the tasks: a call mid-poll registers no task (a dial's
  dispatcher would send its request), and a call that observes an abort's
  effect, such as a write finding its upload dropped, maps it to a forced
  phrase. A transport closed before fork
  stays closed. Teardown is a
  runtime task started by the first close, so it completes however closers
  are interrupted; every closer, concurrent ones included, waits in native
  close for the published outcome and raises a held exception afterwards.
  Under a fiber scheduler, on Ruby 3.4 and 4.0 alike, a closer on a
  non-blocking fiber runs native close on a helper thread joined
  with `Thread#value` (io-event's `process_wait` pattern), so the wait takes
  neither the reactor thread nor a worker-pool worker and sibling fibers'
  calls can drain; an exception raised into the closer meanwhile (async's
  `Task#stop`) is held until the helper returns. That closer waits for the
  transport lock as a non-blocking fiber with no interrupt mask, because
  threads that sibling fibers start would copy a mask, and `Mutex#unlock`
  wakes one waiter, which a sibling woken through the scheduler cannot take
  while a blocking wait sleeps the reactor thread. Under the lock, where
  nothing yields, interrupts are deferred across reserving the close,
  starting the helper and storing its reference, so no interrupt separates
  them and the helper starts with interrupts deferred. An exception that
  lands while the closer waits for the lock is held too, and the closer
  retries once before a helper runs. The helper records the published outcome
  itself; a closer that leaves before publication (a killed fiber or thread)
  records nothing, and the next closer waits on the same teardown. If no
  helper thread can start (`ThreadError`), the closer waits on its own
  thread. A closer without a scheduler or inside `Fiber.blocking {}` waits on
  the calling thread with thread interrupts deferred, so `Thread#kill` lands
  after close returns. Forced teardown fails each call it
  ends `unavailable` with one of two phrases: the admission phrase if the
  call's own setup saw closing first, otherwise
  `HTTP transport failed: connection closed`. Which of the two is not
  guaranteed, and no other phrase is possible; correction round 5
  replaced round 4's choice by whether a request was queued, which picked the
  wrong phrase once teardown landed during a streaming dispatch.
- D14 Windows: cross-build `x64-mingw-ucrt` and run a non-gating Windows CI job
  (specs minus fork and the Linux DNS shim, plus a Go interop subset) against
  that artifact. If green, `docs/support.md` lists Windows x64 as a provisional,
  hyper-only platform; otherwise it moves to a follow-up.
  `aarch64-mingw-ucrt` was build-only at first. Since #174 its install and
  smoke calls on Windows 11 arm64 gate CI and the release; since #175 the specs
  and the Go interop subset also run there, and the Windows job gates CI.
- D15 Release wiring: the gem's version must follow the train (its gemspec
  derives the `connectrpc` requirement from it). `ReleaseTrain#check_configuration!`
  requires Release Please `extra-files` to equal the version files of
  `ReleaseTrain::NAMES`, so split the inventory: a versioned set (NAMES plus
  `connectrpc-hyper`) drives version files, version checks and the Release
  Please configuration, while `NAMES` stays the published set used by the
  candidate and publisher. Add `release_train_spec` coverage. `release.yml`
  and publishing stay untouched until the follow-up PR.
- D16 Cargo: commit `Cargo.lock`, build with `--locked`, `hyper:check` runs
  `cargo fmt --check`, `cargo clippy -D warnings`, `cargo deny check` and
  `cargo audit`. A non-gating daily `canary:hyper` runs `cargo update` and the
  specs. `hyper:cargo:update` updates the lock; new crate releases must be at
  least 7 days old (checked by hand, as for tools). Rust 1.98.0 (the
  rb-sys-dock image's) pinned in `mise.toml` with `mise.lock` entries, scoped to
  the hyper, floors, canary and RBS lanes that build it; the declared
  `rust-version` MSRV is built in CI. `extconf.rb` and cross builds pass
  `--locked`. Source installs in package checks (`hyper:package`,
  `rbs:package`) build offline from a pre-fetched locked crate cache
  (`cargo fetch --locked`, then `CARGO_NET_OFFLINE=true`). The canary updates
  the lock in a temporary copy, since workflows check the tree stays clean.
  Exception: Jim approved hyper 1.13.0 (published 2026-11-23) on 2026-11-24, at
  about one day old, because it fixes hyperium/hyper#4202, the race behind
  #208, once the release checked out as expected. The adopting pull request's
  description records that check: crates.io metadata and checksum, the
  `.crate` against the `v1.13.0` tag's tree, a read of the 1.12.0..1.13.0
  diff, the lock moving only hyper, and `cargo deny` and `cargo audit`. The
  exception covers that version only; the next hyper release waits the 7 days.
  - On a hyper or h2 update, reread in the new release's source the
    dispatch order and frame writes `wire.rs` documents, which the GOAWAY
    retry (D12) relies on. `retry_spec.rb`'s stream-id and waiting-HEADERS
    specs fail when an update breaks them.
  - On a hyper or h2 update, also reread the request-body pipe that the
    HTTP/2 write wait (#193) relies on: `PipeToSendStream` in
    `proto/h2/mod.rs`, polled once by `ClientTask::poll_pipe` and then by
    `PipeMap` in `proto/h2/client.rs`. While a writer still holds the
    body's sender, the pipe must drop the body only on an exit that also
    ends the response attempt: the head has already arrived, the response
    future resolves, or the caller dropped it, even if the response task
    publishes that outcome later. A write that finds the body dropped waits
    for that publication, and only the write's deadline, cancellation,
    abort, forced teardown's `shutdown` or an interrupt otherwise ends the
    wait. Today the pipe drops the body on a reset, a stream h2 no longer
    streams, a body error hyper resets the stream for, the caller's cancel
    (after it dropped the response future), a GOAWAY whose last stream id
    is below the stream's, or a connection error h2 reports to every
    stream; each closes the stream, which resolves any response still
    pending. Its normal end after `close_send` leaves no writer behind. h2's
    `UserError::PayloadTooBig` from `send_data` (a chunk above the 2^31 - 1
    byte window, `Prioritize::send_data`) is the one exit that leaves the
    stream open, and `check_write_len` in `lib.rs` rejects such a chunk
    before it is queued. `send_data`'s other open-stream error,
    `UnexpectedFrameType`, needs the send side already ended, which a live
    sender rules out. The pipe's trailers branch returns `send_trailers`'s
    errors without a reset too, but `CallBody` yields only DATA frames.
    Check whether any of the pipe's exits, `send_data`'s errors or a body
    that yields trailers can now drop the body under a live sender without
    ending the response attempt: no spec fails unless it takes that exit,
    and a write without a deadline would wait until one of those bounds
    ends it. A new exit that depends on the input needs its own check
    before the write, as `check_write_len` is for `PayloadTooBig`.
- D17 `Keepalive` moves to core as `ConnectRPC::Keepalive` (Since 0.3.0,
  loaded by `require "connectrpc"`), with the same constructor, validation,
  equality and freezing as today's `ConnectRPC::Async::Keepalive`.
  connectrpc-async keeps `ConnectRPC::Async::Keepalive = ConnectRPC::Keepalive`
  so existing callers and `ConnectionPolicy` checks are unchanged; its
  `ServerKeepalive` stays in connectrpc-async. Core signatures, the inventory,
  Async's signature alias, the Async README and `docs/upgrading.md` follow, as
  do core's exact package manifest (`scripts/check_package.rb`) and the strict
  source set (`typecheck/source.rb`, `typecheck/Steepfile`). The inventory does
  not prove constant-alias identity, so a spec checks
  `ConnectRPC::Async::Keepalive.equal?(ConnectRPC::Keepalive)` from installed
  packages in both require orders. connectrpc-hyper still does not depend on
  connectrpc-async.
- D18 Proxy (phase 2, after #159 merged): `proxy:` through core's
  `ConnectRPC::Proxy` with the same values, default, validation and fixed
  phrases as NetHTTP. h2c and TLS targets tunnel with CONNECT (Basic auth) as
  grpc-go does; plaintext HTTP/1.1 (`http2: false`) is forwarded in absolute
  form as NetHTTP and Go net/http do. CONNECT keeps #159's bounds (10 MiB head,
  60 s exchange, exact 200), except that bytes after the head are kept for h2c,
  where a server may send SETTINGS behind the reply, as grpc-go keeps them.
  Pool connections are keyed by origin and proxy route. No environment reading
  in the native graph (hyper-util without proxy features, enforced by
  `cargo deny`).
  Verify the default User-Agent reaches the wire unchanged, and that explicit
  repeated and empty values survive the transport.

## Public API (Since 0.3.0)

```ruby
require "connectrpc/hyper"

transport = ConnectRPC::Hyper::Transport.new(
  connections: :pooled,   # or :per_call
  idle_timeout: 1800,     # pooled connections; nil disables
  keepalive: nil,         # ConnectRPC::Keepalive.new(time: 60)
  http2: true,            # false: Connect over HTTP/1.1, unary/server/client streaming
  connect_timeout: 10,
  ca_file: nil, certificate_chain: nil, private_key: nil
  # proxy: added in phase 2 (D18)
)
transport.call(request)            # core unary SPI
transport.streaming_capabilities   # [:server_stream, :client_stream, :bidi_stream] (h2) or [:server_stream, :client_stream]
transport.open_stream(request) { |exchange| ... }
transport.grpc_capabilities        # all four shapes (h2), [] with http2: false
transport.open_grpc_stream(request) { |exchange| ... }
transport.close(grace: 5)          # => true | false
```

Construction raises `ArgumentError` for invalid options and starts no thread
or socket. `inspect` shows no paths or key material. `Native`, the exchange and
error translation are internal (`private_constant` where possible).

## Approach and files

Gem layout (`gems/connectrpc-hyper/`), following connectrpc-health/sentry and
the spike:

- `connectrpc-hyper.gemspec`, `Gemfile` (cooldown 3, core `path:`, tool pins:
  rake, rake-compiler, rb_sys, rspec, rubocop, simplecov, google-protobuf
  4.36.2; `async` test-only for the offload spec), `Rakefile`
  (`RbSys::ExtensionTask`, cross platforms), `LICENSE` (root copy), a generated
  third-party notice for the native graph, `README.md`, `.rspec`.
- `Cargo.toml` (workspace, release profile), `Cargo.lock`, `deny.toml`,
  `ext/connectrpc_hyper/{Cargo.toml,extconf.rb,src/*.rs}`. Split the spike's
  `lib.rs` into `runtime.rs`, `gvl.rs`, `conn.rs` (DNS with Go-style
  dual-stack fallback, TCP, TLS, handshakes, failure classification), `tls.rs`,
  `pool.rs` (per-origin entries, closed-sender pruning, idle timeout, retry),
  `call.rs` (streaming exchange), `unary.rs` (fast path), `wire.rs` (stream
  ids and GOAWAY cutoffs for D12), `lib.rs` (init).
- `lib/connectrpc/hyper.rb`, `lib/connectrpc/hyper/{version,transport,
  exchange,errors,tls}.rb` (split as convenient). Core gains
  `lib/connectrpc/keepalive.rb` (D17).
- `sig/connectrpc-hyper.rbs`, `sig/manifest.yaml`.
- `scripts/{run,tools,ci_runtime,check,build,package,cross,smoke,interop}.rb`.
- `interop/` Go peer (own `go.mod`, grpc-go 1.85.0 and connect-go 1.21.1 at the
  repo's pins, `replace` onto `interop/grpc`): grpc-go and connect-go over h2c,
  TLS and mTLS, connect-go HTTP/1.1, the trailers-only case, held and flooding
  streams, keepalive and idle-GOAWAY listeners, a restartable listener, a
  CONNECT proxy for phase 2. Caches under `vendor/hyper/go`.
- `spec/` RSpec suites (see verification).

Repository wiring (additive): `mise.toml` and `mise.ci.toml` `hyper:*`,
`floors:hyper`, `canary:hyper`; `.github/workflows/ci.yml` jobs (below);
`.gitignore` (compiled `.so`/`.bundle`, `target/`, `tmp/`); `.rubocop.yml`;
`Rakefile`; `typecheck/` (api_check, collection, inventory, caller evidence,
installed checks as the sentry gem did); `.github/release-please-config.json`;
`README.md`, `AGENTS.md`, `docs/{support,upgrading,development,ci,rbs,design}.md`.

The gem README carries a short "which transport" note: core NetHTTP for
simple Connect unary; `ConnectRPC::Hyper::Transport` for any thread (Puma,
Sidekiq, scripts) with pooled HTTP/2, all shapes and gRPC; the Async
`Transport` and `ThreadedTransport` for applications already on Async or
needing its connection policy and observers; `PerCallTransport` (#163) for
calls from inside an Async reactor, where a hyper call blocks the reactor
thread unless Ruby 4.0 and async's worker pool offload it. It also covers
server-restart and keepalive behaviour against grpc-go, the platform list and
source-gem needs, the rustls differences and fork behaviour.

Ruby side keeps protocol logic as in the spike: deadlines, cancellation, fixed
phrases, `lifecycle.record_failure` before mapping, the native failure as
cause, abort in a masked `ensure`. Rust owns connections, streams and waits.

## Observable success

- A transport shared by many threads multiplexes calls over one HTTP/2
  connection per origin, and its idle connection answers server PINGs and
  processes GOAWAY.
- After a server restart (graceful GOAWAY or a hard kill), the next call
  reconnects; with keepalive, a silent peer is detected within
  `time + timeout`.
- Interrupts, deadlines and cancellation land on the caller at DNS, connect,
  TLS, blocked write and blocked read, and the peer sees `RST_STREAM(CANCEL)`
  or EOF.
- Inside an Async reactor on Ruby 4.0 with the worker pool, a call does not
  block sibling tasks.
- Prebuilt gems install without Rust on the listed platforms.

## Failure paths and risks

- Unsafe FFI (nogvl/UBF, `rb_protect`, `ForkGuard`, magnus wrapping): keep it
  small, comment invariants, adversarial review.
- Lock held at fork: no Mutex in the runtime slot; pool locks belong to driver
  state that a child never touches (the Ruby transport replaces the driver on
  PID mismatch).
- A stalled resolver: dropping the future abandons the blocking-pool task; GC
  and exit must not wait; the thread stays parked until getaddrinfo returns.
- Partial progress: retries, aborts, wakes and close each act after a wait,
  dispatch or ownership transition may have partly succeeded (D4, D5, D12,
  D13); each needs its own evidence, not only the happy path.
- Fiber scheduler without the worker pool (or Ruby 3.4): the call blocks the
  reactor thread; documented, PerCallTransport remains the reactor option.
  `close` on a non-blocking fiber under a fiber scheduler waits on a helper
  thread, so it does not block the reactor; inside `Fiber.blocking {}` it
  still waits on the calling thread. With the pool, async's default single
  worker runs parked waits from different fibers one at a time; a larger
  `IO::Event::WorkerPool` is the documented remedy.
- Helper-path close interrupt windows (accepted): a second thread interrupt
  that lands while the closer handles a first one can escape before
  publication; two exceptions before any helper starts make the closer raise
  early; a `Thread#kill` that lands on a closer whose `Thread.new` raised
  `ThreadError` leaves the transport closing until the next close; and a
  `Thread#raise` or `Thread#kill` aimed at the reactor thread lands on
  whichever fiber runs. Teardown that a helper started always finishes and
  records its outcome.
- Runtime start failure (#181): close can be the runtime's first use. When
  the OS refuses the first worker thread during a cold start, tokio panics
  and the closer raises `fatal`. Close starts the runtime before it claims
  teardown, so that close claims and stops nothing, the transport stays
  closing, and a later close starts teardown. A closer whose helper thread
  could not start retries once itself, and raises the `fatal` after the
  outcome if that retry starts teardown. `Tracker::spawn` likewise starts the
  runtime before it takes its lock, and `Driver::open` before admission,
  because each stream operation takes a stream half before it drives. A call
  or stream whose runtime start fails raises `fatal` the same way and leaves
  nothing claimed. When the OS refuses the second worker after the first
  started, tokio 1.54's `build` panics with that worker running and never
  returns (cause tokio-rs/tokio#8408; tokio-rs/tokio#8752, merged upstream but
  unreleased, makes `build` return an error instead). So the runtime starts
  on a short-lived `hyper-rt-start` thread, and the caller waits for it
  for at most 5 s (#218): a start that fails, returns an error or outlasts the
  bound raises `fatal` with nothing published, and a later use starts a fresh
  attempt. A start stuck in tokio keeps its start thread, first worker and I/O
  driver for the life of the process; if it finishes after all, its runtime
  serves later uses. The bound applies per attempt, so a closer whose helper
  thread could not start, and that retries once itself, can wait for it twice.
  `Thread#raise`, `Thread#kill` and signals wait for the start, as the caller
  holds the GVL.
- rustls strictness (CN-only, X.509 v1, single-label wildcard, CA as leaf,
  encrypted keys): `TLS verification failed` or `ArgumentError`; documented.
- Server RST_STREAM codes map as the Async transport does (3 and 11
  resource_exhausted, 7 unavailable, 8 canceled, 12 permission_denied, else
  internal) for gRPC; Connect reports `unavailable`.
- Packaging floors: glibc 2.29, macOS 11 (arm64); the source gem needs Rust >=
  MSRV, libclang and a C compiler. musl gems need `libgcc_s`.

## Non-goals

- Publishing, `release.yml` and release-script changes (follow-up PR; the PR
  description proposes them, including trusted publishing and one Sigstore
  bundle per archive).
- HTTPS and SOCKS proxies, proxy auth beyond Basic.
- A FIPS build, a current-thread runtime, public window or worker settings,
  `wait_for_ready` and background reconnect with backoff.
- Async-reactor offload on Ruby 3.4.

## Verification map

| Concern | Evidence |
| --- | --- |
| Interop, all shapes | grpc-go and connect-go over h2c, TLS, mTLS; connect-go HTTP/1.1 (unary, server, incremental client stream; ALPN `http/1.1` over TLS); trailers-only with typed details, and the negative case (status in trailers after HEADERS without `END_STREAM` is not trailers-only); Connect safe GET; Ruby 3.4.12 and 4.0.9 |
| Header limits | Leading fields and trailers below, at and above 65,536 bytes, with `grpc-status` before and after the boundary; outbound gRPC fields over the limit fail before dispatch |
| ALPN | `http2: true` against an HTTP/1.1-only TLS server: gRPC and Connect streaming fail before any request reaches the peer (zero application requests logged); Connect unary succeeds |
| Body limits | Connect unary over the response limit by advertised content-length and by incremental body both fail `resource_exhausted` |
| Interrupts | Matrix: DNS (LD_PRELOAD getaddrinfo shim), connect (full backlog), TLS (silent server), blocked write, blocked read; RuntimeError, an Exception subclass, `Thread#kill`; synchronised on parked-thread observation; follow-up call reuses the connection |
| Teardown | GC and process exit with resolvers parked |
| Streaming limits | Over-limit message fails `resource_exhausted`; flooding peer bounded by the window; peer that never reads blocks `write` until deadline |
| Writes after the stream ended | Raw peer ends the stream at the request's first DATA: a gRPC reset before the head, or after it with no unread DATA, makes the next write raise the mapped reset, which the head or body then reports; a status (gRPC trailers or Connect end-stream) or a reply then `RST_STREAM(NO_ERROR)`, and a reset behind unread DATA, raise `stream closed` while the response still reads byte for byte; a Connect reset raises `stream reset`; core's client stream stops at the failing write and reports `not_found` (gRPC and Connect) or `canceled`; a write parks until the response task publishes its outcome (hold point `respond:publish`) and a `Thread#raise` lands there promptly; a reader parked on the same stream gets the reset, never an overlap error; an HTTP/1.1 body error the write or a read consumed is still raised by the operation after it; over HTTP/1.1 a write never waits for the response (hold point `write:outcome` counts the HTTP/2 waits the publication specs synchronise on) and raises `connection closed` past a declared Content-Length (5 and 0), on a pooled HTTP/1.0 connection, and when the connection resets mid-upload; a write of 2 GiB or more raises `ArgumentError` before anything is sent, judged by the string's own length; a write in flight on a stream whose end the client has not seen yet (its status behind an unread message) gets `connection closed` when forced teardown, parked between `shutdown` and the task aborts (hold point `force:shutdown`), ends it; a server that ends its response without resetting the stream (gRPC and Connect statuses, a reply, trailers-only, and an end released on a gate with nobody reading) gets exactly one client `RST_STREAM(NO_ERROR)` on that connection and stream, counted after a later call reuses the connection: a write after the end raises `stream closed`, one still waiting to queue raises and one already queued returns, the response still reads, and core's client stream reports `not_found`; while the upload waits for send window the reset goes out only once the window opens; a write waiting while a reader takes and returns the receive half still sees the end; a writer that called `close_send` first sends its queued bytes and END_STREAM, with no reset |
| Cancellation | Sibling reset with and without DATA in flight on a shared connection; cross-thread `CancellationSource`; per-call deadline; token cancel during DNS/TCP/TLS and while waiting for the shared dial; silent h2 peer bounded by the deadline; the wire timeout after a delayed setup reflects the remaining deadline |
| Resolver | Several parked lookups, then cancellation, then a lookup of another origin succeeds; GC and exit do not wait |
| Liveness and restart | Idle pooled connection answers server PINGs; reconnect after idle GOAWAY; GOAWAY with an active stream: the sibling finishes on the draining connection while a new call uses a new one; hard kill and restart on the same port; keepalive: withheld ACK retires the connection and fails in-flight calls `unavailable`, `permit_without_calls` on and off, behaviour with buffered DATA and with BDP pings |
| Pool | A pool dial's waiter whose pooled connection closed before the server's SETTINGS (TLS 1.3 mTLS rejection) fails with that connection's failure after one dial, with no SETTINGS recorded (#208), while one whose connection drained after the server's SETTINGS (graceful GOAWAY) dials again and succeeds (two dials and accepts); concurrent first callers share one connection; callers before, during and after the pool dial pools its connection share one accept; a pool dial outlives the call that started it and splits `connect_timeout` alone; a failed dial fails every waiter alike and is not cached; an `http/1.1` outcome serves each waiter on a free pooled or idle connection, the dialed one included, or else its own dial, and fails a streaming waiter; close fails pool-dial waiters and ends the dial; `per_call` opens one each; idle timeout closes an idle connection and the next call reconnects; idle eviction racing a checkout never fails the call; HTTP/1.1: two-idle cap, reuse, and an interrupted upload or incomplete response never returns its socket |
| Retry | With one stream slot held, a GOAWAY replays both the request waiting in h2's slot and the one hyper hands back, each once on a new connection (Connect and gRPC), while a connection h2 fails itself (DATA on stream 0) fails the request in the slot `unavailable` with the closed phrase and no replay, as the held stream (Connect and gRPC); a Connect unary stream above a GOAWAY's last stream id succeeds once on a new connection while one at or below it fails without a replay; a gRPC stream whose HEADERS were sent, or were offered but not written while the socket was held back, is not replayed; a stranded Connect unary request (held at `request:strand`) is retried once as unsent over HTTP/1.1 and HTTP/2, per-call and pooled, and fails with the connection's TLS or closed failure, while a stranded gRPC stream fails without a retry and releases a blocked `write`; an indexed HTTP/2 stream whose head is withheld still gets its response after the dispatcher ended; a ready answer wins under an exhausted cooperative budget (Rust unit test); derived stream ids match the wire when hyper skips a request canceled in its queue; refused Connect unary retried once; a refusal from a pooled connection that still admits retries on a connection of its own while siblings and later calls keep the pooled one; no second retry; an ambiguous disconnect after the handler started is not replayed (the handler runs once) |
| Fork | Subprocess with an in-flight stream in another thread, h2c and TLS: child calls work, parent stream intact, parent reuses its connection; fork while the runtime or root cache initialises; parent closes while a long-lived child stays alive (the peer sees the close, or the documented limitation); a transport closed before fork stays closed in the child; a child that inherits a pending close, or never built its own driver, closes at once with `true` and then fails calls with the closed phrase |
| TLS parity | ca_file appended to native roots; `SSL_CERT_FILE` override; chain/key validation errors name only the input, without a leaking cause; mTLS; name mismatch and IP SAN; provider-rejected identity (Ed448); TLS options on `http://` raise; rustls rejections pinned (CN-only, v1, single-label wildcard, CA as leaf, encrypted key) |
| Errors | Fixed phrases for refused, DNS, TLS verify, closed, ALPN mismatch, deadline; server RST_STREAM with each mapped code |
| Offload | Ruby 4.0 with async worker pool: sibling task progresses, stop/cancel/deadline/raise/kill land; simultaneous read and write on one bidi stream from two fibers on a two-worker pool, then abort or stop while both wait bodies run; a scheduler's `blocking_operation_wait` hook receives waits on 4.0 and never on 3.4; Ruby 3.4 and without the pool behave as a plain blocking call; on both Rubies, with and without the pool, a sibling fiber's in-flight call drains while another fiber waits in close |
| Spurious wake | A wake without a pending interrupt during a blocked `write` does not duplicate the message on the wire |
| Close | `close(grace:)` drains in-flight calls (true), forced (false), later calls fail with the closed phrase; `close(grace: 0)` racing setup, a retry or a streaming dispatch (write first and head first), with one of the two forced-teardown phrases and never a bare `HTTP transport failed`; a stranded Connect unary call that forced teardown ends gets a forced phrase and no replacement dial; repeated close shares the outcome, and a concurrent closer stopped by `Task#stop` while it waits raises only after it is published; a task aborted before its first poll is still counted until its socket closes; a `Task#stop` on the closer lets a sibling fiber's call drain before the stop is raised; a first close whose runtime start fails (injected in a fresh process) raises, and a later close starts teardown and returns true; a first close whose start thread, first worker or only second worker the OS refuses (`pthread_create` shim, `runtime_start_spec.rb`) raises `fatal` within the start bound, the transport stays closing, and a later close starts the runtime and returns true; a stream whose runtime start fails raises before the exchange is handed out; a dial that finishes its handshake while forced teardown is parked after `shutdown` (hold point `force:shutdown`) registers no dispatcher and sends no request, per-call HTTP/2 and HTTP/1.1 |
| Keepalive move | Core `Keepalive` specs (validation, equality, freezing); `ConnectRPC::Async::Keepalive.equal?(ConnectRPC::Keepalive)` from installed packages in both require orders; existing Async keepalive specs pass |
| Fast path | Connect unary passes all unary specs and does one native wait (spec on an internal counter or a GVL-contention probe) |
| Package | Source gem builds and installs in a clean GEM_HOME and runs a smoke call; exact manifest, MIT plus third-party notice, push host, MFA |
| Cross | All platform gems build in CI as artifacts; Linux gems smoke-tested on their arch (native arm64 runner) and libc (Alpine containers); arm64-darwin smoke on a macOS runner (non-gating); x86_64-darwin is build-only, and `docs/support.md` says which artifacts were built, installed and exercised. Since #174 every platform gem and the source gem builds twice and smokes on a native runner for both Rubies, gating, including x86_64-darwin on macOS Intel |
| Windows | Gating job (since #175) runs the specs minus fork and the Linux DNS shim, and a Go interop subset, against the x64-mingw-ucrt and aarch64-mingw-ucrt gems on x64 and arm64 runners, both Rubies |
| Types | `rbs:check` (inventory, consumers, caller evidence) and `rbs:package` |
| Repository | `hyper:verify`, `mise run verify`, `mise run rack:verify`, `mise run check`, `mise run rbs:check`, `mise run rbs:package` before Ready |

Proxy and User-Agent evidence joins in phase 2: CONNECT with auth for h2c,
TLS and HTTP/1.1, environment precedence through the core helper, a hostile
environment ignored with an explicit proxy, and the User-Agent on the wire.

## CI

- `hyper` (Ruby 3.4.12, 4.0.9): `mise run hyper:verify` (setup and compile,
  check, specs, interop, package).
- `hyper_cross` (platform matrix): `hyper:cross` per platform, uploaded as a
  workflow artifact; plus the MSRV build.
- `hyper_smoke` (needs `hyper_cross`): install and smoke each Linux gem on its
  arch and libc for both Rubies.
- `hyper_windows`: x64 on `windows-2025` and aarch64 on `windows-11-arm`,
  both Rubies, against the cross-built gems. Non-gating until #175 made it
  gate.
- Since #174, `hyper_cross` and `hyper_smoke` are replaced by `hyper_gems`,
  which calls the reusable hyper-gems workflow that `release.yml` also calls:
  build jobs per archive, smoke jobs per platform and Ruby on native runners,
  a release candidate check and `Verified`. `hyper_msrv` is its own job and
  `hyper_windows` needs `hyper_gems`. See [CI](../ci.md#optional-hyper-lanes).
- `floors` gains `hyper`; `canary.yml` gains `canary:hyper`; the `rbs` job
  caches `vendor/hyper/bundle`. Every lane that compiles the extension
  (hyper, floors, canary, rbs) provisions the pinned Rust and libclang
  explicitly and pre-fetches the locked crates.

## Phase 2 (after #159 merged)

Rebased onto `ccffd92`, added `proxy:` (D18) and verified the User-Agent.
Core's `ClientTLS` stays a private constant, so the gem keeps its own copy of
the validation, with a spec feeding the same inputs to both transports.

## Windows (#175)

The non-gating Windows job's reactor-child failures had two causes, both
fixed. console 1.38.0 under `GITHUB_ACTIONS=true` picks its XTerm output even
for a pipe, whose `IO#winsize` raises `Errno::EBADF` on Windows; the task
warning replaced the task's own error and could leave a reactor running, so
spec children set `CONSOLE_OUTPUT=Serialized`. A closer's native wait
offloaded to async's single-worker pool (or, on Ruby 3.4 and without the
pool, held on the reactor thread) kept a sibling fiber's call from running
its next wait, so the grace expired into forced teardown; Windows hit that
ordering naturally, and Linux reproduced it by delaying the sibling. Close now
waits on a helper thread under a fiber scheduler (D13), and the offload cases
run on every platform. `hyper_windows` runs the specs and the Go interop
subset against the x64 and aarch64 gems on `windows-2025` and
`windows-11-arm`, for both Rubies. It gates CI since #175, after four
consecutive green heads; its four checks join the main ruleset once #178
merges.

## Follow-ups

- Release workflow: a build matrix before publish, one archive per platform
  plus the source gem, SHA256 checks against the candidate, one Sigstore
  attestation and `gem push --attestation` per archive with trusted publishing,
  platform gems before the source gem, digest-pinned images, and the darwin
  SDK question (native macOS runners as the alternative). Done in
  [#174](https://github.com/jimeh/connectrpc-ruby/issues/174): the reusable
  hyper-gems workflow builds each archive twice in digest-pinned osxcross and
  other rb-sys-dock images and smokes it on native runners, including macOS
  arm64 and Intel; see [releasing](../releasing.md#connectrpc-hyper-archives).
