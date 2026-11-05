# Async HTTP/1.1 Connect client

Base: `298ce60f3546d77ddc4d9a419f9c6338d3e5f1c6` (merged Falcon H1 server).
Delivery: one independent PR from updated main. No merge authority.

## Scope and public contract

Add explicit `ConnectRPC::Async::HTTP1Transport` to the existing optional
`connectrpc-async` package. It implements unary `call` and the existing Connect
streaming exchange SPI for `server_stream` and `client_stream`. It is an independent
class, never a subclass of H2 Transport. It exposes no gRPC methods or bidi
capability; existing core gates reject those before encoding/I/O. Generated clients
use it through the existing Client `transport:` option. Standalone Async Host stays
H2-only; Falcon owns Ruby H1 hosting.

Constructor: `origin:, ca_file: nil, certificate_chain: nil, private_key: nil,
connection_limit: 4, max_requests: 128, connect_timeout: 10`. Validate inputs like
H2 Transport. `max_requests` bounds all admitted operations, including lease waits;
full/closed admission returns unavailable immediately. `connection_limit` is a hard
per-transport cap, including in-progress connections. No arbitrary per-origin global
limit. No fairness promise. Four long streams can occupy all default capacity;
waiters need deadlines/cancellation or more configured connections. Wake waiters on
lease release, completed retirement, dial failure/cancellation/expiry releasing a
reservation, and close. Count a retiring slot until raw close and task joins
complete. A setup finishing after request cancellation is aborted rather than handed
to another request. Each connection has one exclusive lease, including setup,
request dispatch, upload, response read and cleanup.

Use a small private reactor-owned lease manager with atomic reservation before any
yielding dial. Waiters retry on a Condition, under operation cancellation and
deadline. Parallel reserved dials are permitted; a stalled dial cannot serialize all
unrelated capacity. The inspected async-pool 0.12.0 checks resources.size before a
yielding constructor and records a resource only afterward. Its default creation
guard serializes dials; increasing concurrency alone loses the hard cap. Do not
change shared H2 pool behavior to solve this.

Reuse idle connections until close or peer failure. At acquisition, use a
nonblocking viability check to retire FIN/unsolicited buffered data before any
request bytes; choosing a fresh connection here is not replay. No idle timer,
max-age, background health check or retry policy is introduced. The finite
connection cap bounds idle retention. Reuse only after request writer success/join,
complete HTTP response framing/EOF, successful owned cleanup, and native
idle+persistent state. Stale keepalive may fail one RPC with unavailable; never
replay it.

## Operation ownership

Use pinned protocol-http1 0.41.1 lower-level write_request/write_body,
read_headers/read_response_body and pinned body parsers. Compose a strict HTTP/1.1
status-line parser (exact version and three decimal status digits, including extension statuses such as 800), framing
validation before the native parser deletes fields, and native
persistence/state/count updates. Reject HTTP/1.0 responses for this explicit
HTTP/1.1 transport. Reject stacked/unsupported Transfer-Encoding and duplicate
Content-Length, as well as CL+TE. Use a plain native Connection or async-http H1
Connection, never its auto-release Client subclass. Do not call upstream H1
Client#call or Response.read, assign Response#pool, or let a detached writer own
lifecycle. Declare protocol-http1 directly and update the Async, Falcon and OTel
locked PATH dependency entries without version upgrades. Update the stale gemspec
summary too.

Streaming dispatch must finish request-head flush and expose the writable exchange
before waiting for response headers. Track the writer task explicitly; one response
reader can progress concurrently with upload. Use existing bounded OutputBody
semantics (one queued 16 KiB fragment plus one in-flight fragment), with the first
OutputBody#read as the post-head dispatch signal: pinned write_chunked_body flushes
the complete head before that read when ready? is false. Keep ready? false. A failed
writer must also resolve the dispatch wait. Use an H1-only private buffered stream
whose read path never flushes output. Pinned IO::Stream::Readable#fill_read_buffer
calls flush and can block behind a stalled upload; its stock Duplex wraps the same
coupled buffer. The private read path must keep pinned EOF/TLS behavior and cap
native body read fragments at 16 KiB so the existing remainder bound stays true. No
global monkeypatch or change to the H2 stream. First prove blocked-upload concurrent
response reads with real TCP and TLS peers before building the full transport. Also
probe reading a buffered response after a reported socket write failure over TLS. If
OpenSSL itself forbids that read, report unavailable with the failed wire evidence;
never promise recovery that native TLS cannot supply. Preserve original request,
call cancellation and lifecycle activation in every owned task. No new public SPI.

Response-head reads loop over at most 16 interim responses; reject 101/upgrade,
invalid response versions/status/framing and unsupported transfer coding. Keep the
pinned 8192-byte line bound for status, headers and chunk lines. No new claim of
total-header/RSS bounds. Snapshot leading metadata before reading a chunked body;
actual HTTP trailers must never alter the leading carrier. Unary uses an
operation-owned writer concurrently with response reads too. A complete non-200 HTTP
response wins immediately for core error validation, including while the writer is
still blocked: stop that writer destructively, retire the connection, and record its
outcome. A 200 response requires a successful writer under the original deadline.
Accept close-delimited unary as ordinary HTTP framing, documenting that clean EOF
cannot detect a truncated semantic payload without Content-Length; malformed
protobuf/JSON remains core validation. TLS unexpected EOF without close_notify must
remain a transport error. Unary bounded reads preserve response_limit and
declared-length validation; core retains streaming message/terminal/wire limits and
absolute tail/recovery policies.

A send failure records its original cause without destroying a still-readable
response before core's bounded terminal-only recovery. Remote error and success
follow the existing core arbitration. Writer normal completion is an explicit
transport flag, never inferred from native idle state (native writer ensure can mark
a canceled mid-chunk writer idle). Record writer failure independently and surface
it through the existing SPI as follows. Track writer state pending/ok/failed and a
reported flag. Only write marks a writer failure reported and raises unavailable,
including when a blocked producer wakes on OutputBody error. close_send is
idempotent and does not raise for writer failure, so it cannot block core recovery.
finish requires full native body EOF and joins the writer within its supplied
timeout. Normal writer completion succeeds; reported failure also permits completed
transport cleanup but always retires the connection; unreported writer failure or
pending writer at expiry raises unavailable and retires. Core then preserves
unhandled send failure on remote success, permits a handled send failure under its
existing policy, and can recover remote errors only for failures reported through
write. An upload failure first seen after the last accepted write returns
unavailable even if the terminal contains a remote error. Document that masking
explicitly; do not add a new SPI or decode Connect terminals in transport. Test all
of these result boundaries. Do not invent early-response success by swallowing
writer or cleanup failures. `finish` must attest full HTTP EOF and owned transport
completion, not only terminal-envelope receipt. Unfinished, abandoned, canceled,
malformed or failed exchanges retire their connection.

Retirement marks operation terminal before detaching cancellation listeners and
releasing a lease. Every nonsuccess teardown error-closes OutputBody or raw-closes
before any yield, so abandonment cannot emit a normal zero chunk. Treat abort after
a successfully completed finish as a no-op; other aborts retire. This permits reuse
after an EOF-verified remote Connect error followed by core abandonment. Cancel/join
all operation-owned tasks before reuse. Late writer/listener/cleanup must have no
path to a resource leased to a newer call. Setup abort includes the existing Ruby 4
shutdown(SHUT_RDWR) wake before raw close for blocked SSL handshake. Destructive
retirement closes raw TCP first, then cancels/joins operation tasks, then
buffered/TLS cleanup. Keep the existing single 100 ms cleanup allowance; force
retirement on cleanup expiry/failure, and preserve the owner failure in observation
and result where required. Healthy other leases continue.

Finite RPC deadlines cover reservation/wait, TCP/TLS creation, dispatch, headers,
upload, body and cleanup. Setup also has its own combined connect_timeout.
Foreign-thread cancellation only resolves an event; reactor tasks arbitrate and
close owned resources. Nil deadlines add no watchdog. The existing independent
per-operation cap remains where the SPI requests it. Application/codec/native
blocking stays outside hard timing guarantees. A peer that neither consumes nor
closes can block an upload without a deadline; the API cannot promise that a
write-only caller observes every early response while it remains blocked.

Use existing affinity/owner semantics: construction and calls on the owning
reactor/thread; `close(grace: 5)` stops admission, awaits admitted calls including
waiters, and closes owned idle connections. Expiry/owner cancellation aborts all
owned leases/setup/wait tasks, returns false for forced close, and cannot report
successful drain prematurely. Repeated/concurrent close shares first outcome.

TLS offers only ALPN `http/1.1`; accept `http/1.1` or absent ALPN as legacy
HTTP/1.1-over-TLS. Reject every other negotiated value, including h2, before request
bytes. This is a deliberate H1 absent-ALPN policy, never H2 fallback. Peer and
hostname checks remain on; support verified TLS/mTLS. Add a private TLS helper
option retaining its current `["h2"]` default. TCP rejects TLS options as H2 does.

## Verification map

1. Async-bundle real raw-socket peers: fixed/chunked/close-delimited response
   framing, repeated leading fields versus actual trailers, interim bound and
   upgrade rejection, malformed/truncated/oversized bodies and stalled tails. Cover invalid status
   forms/version, stacked/unknown TE, duplicate CL and CL+TE, extra bytes after
   framed EOF, exactly 16 versus 17 interim responses, and canceled writer
   falsely native-idle after full response. Extra buffered bytes must prevent
   reuse even though they are outside the framed response.
   Prove head-before-headers exposure and early remote rejection with unfinished
   upload. Use observable events or bounded condition polling, no race sleeps.
2. Exclusive lease safety: concurrent healthy call beside long stream, pool cap
   under stalled parallel TLS handshakes (TCP loopback backlog is insufficient), full admission, deadline/cancel during pool
   wait/setup/headers/upload/read, no replay, successful reuse, stale keepalive,
   no old writer/cancellation affecting a later lease, owner exit and close.
   Include regression-sensitive assertions and a representative red-before-green
   or focused perturbation when inspection cannot establish coverage.
3. Falcon bundle: real Ruby generated-client unary/SS/CS against merged Falcon
   H1 over TCP/TLS/mTLS, including early rejection/unfinished upload, full EOF,
   healthy concurrent/subsequent calls and shutdown. Preserve all server policies.
4. Independent pinned connect-go: explicit H1-only listener modes and actual
   HTTP/1.1 observation. Unary/SS/CS, protobuf/JSON, identity/gzip, TCP/TLS/mTLS
   on Ruby 3.4.12 and 4.0.9. Existing H2 modes/evidence unchanged.
5. Official native H1 CLIENT profile/task/inventories, separate from all existing
   NetHTTP/H2/server sets. Pinned runner 1.0.6 source-derived exact inventory,
   unary+SS+CS, both codecs/codings and declared TCP/TLS/client-cert features.
   Include safe GET if the profile declares it; the transport supports core GET.
   Discover counts from pinned source/runner. No exclusions, waiver, expected
   failure, skip or runner upgrade. A dedicated H1 observer must inspect actual operation tasks/leases/raw sockets
   rather than H2 response.stream; observe actual status-line HTTP/1.1 and
   negotiated ALPN including nil. Strict actual-lifetime telemetry and hosted
   coverage on both Rubies. Fail closed on missing/contradictory observations.
6. Owning signatures, API inventory, strict positive/negative and installed
   generated callers; packages and isolation. Update affected backend locks.
   Focused real OTel SDK lifecycle/propagation evidence plus H2 regressions.
   Run project checks and required final verify/rack:verify, owning Async/Falcon
   checks/package/interops and RBS checks/package. CI supplies clean matrix gates;
   preserve exact failures and distinguish personally run from hosted evidence.

## Delivery and limits

Update Async README, usage/streaming/design/support/conformance docs only as needed
for this usable API. Ordinary-thread persistence remains issue #29. No new
package/backend, gRPC, bidi, fallback, replay, pipelining or resilience layer. No
UI; UX review does not apply. Do not modify Falcon processor/admission,
response-first 500 ms total grace or 100 ms/256 KiB/64 KiB discard contracts, and do
not turn Web policy into Connect policy.

After plan review, freeze this contract, commit it separately, and use a fresh Codex
implementer. Inspect its result and evidence, commit scoped changes, file a draft
PR, then independent Codex/Claude dual review. Batch confirmed corrections;
concurrency/lifecycle changes require renewed dual coverage. Invoke authorized
CodeRabbit only after internal acceptance. Qualify exact head and current base,
ready status/checks/reviews/open threads, and report to coordinator without merging.

Historical server H2 aggregate failure remains unexplained and is not fixed or
reclassified by this feature. Historical artifacts are context, never new-head
qualification. Investigate concrete new failures; do not rerun blindly to green.

Plan reviewed twice by Claude Opus 5.5. The second pass checked the changed
buffering/parsing ownership and existing-SPI recovery semantics. Accepted
source-grounded corrections are incorporated above. Initial qualification exposed
an incorrect 100..599 restriction added during planning: pinned official 1.0.6
requires status 800 to reach core and map to unknown. Corrected the contract to
retain well-formed three-digit extension statuses and existing core fallback;
malformed status syntax remains rejected. No official case or assertion changes.

Open questions: none. Any material change of scope or architecture returns to
coordinator; code-grounded details stay with this run.
