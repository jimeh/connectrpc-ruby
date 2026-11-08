# ConnectRPC Async

Optional HTTP/2 hosting and explicit HTTP/1.1 and HTTP/2 transports. Require `connectrpc/async`
explicitly. Core, Rails and Rack remain independent. This experimental package
supports Connect unary and streaming, identity and opt-in core gzip, protobuf and ProtoJSON,
h2c prior knowledge and TLS with verified peers and ALPN `h2`.

The example assumes existing core `server`, `binding`, `method` and `message`
objects, as shown in the repository unary examples.

```ruby
require "async"
require "connectrpc/async"

Async do
  host = ConnectRPC::Async::Host.new(server: server).start
  origin = "http://127.0.0.1:#{host.port}"
  transport = ConnectRPC::Async::Transport.new(origin: origin)
  client = ConnectRPC::Client.new(base_url: origin, binding: binding, transport: transport)
  begin
    client.call(method, message)
  ensure
    transport.close
    host.close
  end
end.wait
```

The application owns a long-lived reactor task. Construction, networking and
lifecycle operations must use its reactor and thread. There is no hidden
synchronous reactor thread. Scheduler-compatible I/O yields; CPU, native codecs
and native DNS may block. Ensure graceful close before the owner exits. Owner
cancellation aborts only owned sockets/tasks and makes no delivery promise.

`Host.new(server:, host: "127.0.0.1", port: 0, tls: nil, max_connections: 128,
max_requests: 128, handshake_timeout: 10, idle_timeout: 60)` returns a host.
`start` binds/listens before returning self; `port` and `addresses` expose the
actual binding. States are new/running/closing/closed; hosts cannot restart.
Connection admission happens before TLS/preface. Request admission fails with
bare 503. Setup has one combined cooperative timeout. Idle expiry runs only
without streams or handlers and can be disabled with nil.

`Transport.new(origin:, ca_file: nil, certificate_chain: nil, private_key: nil,
connection_limit: 1, max_requests: 128, connect_timeout: 10)` implements core's
transport seam. Origins contain only scheme/host/port. Request paths, including
core's base prefix, remain intact. One connection multiplexes requests. Full
admission fails with unavailable; pool waits honor finite RPC deadlines. Nil
RPC deadlines remain unbounded, while actual setup still has its own timeout.
No retries, redirects, proxy environment routing or HTTP/1 fallback. The transport
carries opaque encoded bytes; core owns compression and negotiation.
A future separate call can open a fresh connection after failure/GOAWAY.

Both objects provide `close(grace: 5)`: stop admission, await handlers/calls and
complete wire streams, send GOAWAY when empty, then half-close and await EOF.
Returns true when drained, false after forced teardown. Grace is finite and
nonnegative; zero aborts. Repeated/concurrent calls share the first outcome.
Finite deadlines begin in core before encoding and cover pool/setup/headers/body.
Timers use at most 86400-second waits without shortening logical deadlines.
Cleanup uses one total internal 100ms allowance. Ordinary cancellation resets
its stream. A frame that cannot finish within cleanup deliberately fails the
whole connection and siblings. Forced teardown closes raw TCP before cancelling
and joining writers/readers, then performs buffered/TLS cleanup. These are
cooperative bounds, not arbitrary native/application interruption guarantees.

Forced teardown invalidates the connection immediately. External callers of
private connection abort and server session abort/close then wait for owned
tasks, their cleanup descendants and final buffered/TLS disposal. An owned task
initiating or reentering abort requests retirement without waiting on cleanup
that must join that same task. Each external abort/close joiner retains its own
first interruption until cleanup completes.
H2 pool release, acquisition or close can therefore wait beyond an RPC deadline
for actual retirement. The 100 ms fallback allowance triggers destructive
cleanup; it is not a hard deadline for that cleanup.

These joins require cooperative tasks and a live application reactor. A
non-yielding handler or native operation can delay them indefinitely. Cleanup
survives ordinary owner cancellation while an external waiter or lifetime guard
keeps the task tree alive; reactor termination cannot guarantee completion.

See the [supported 0.x contract](../../docs/support.md) for the target support
profile, matched package versions, compatibility policy and adoption gates.

## TLS and client authentication

Applications load their own files. Host TLS is a complete hash:

```ruby
tls = {
  certificate_chain: [File.read("leaf.pem"), File.read("intermediate.pem")],
  private_key: File.read("key.pem"),
  client_ca: nil # Or a nonempty array of explicit trust-anchor PEM certificates.
}
```

Each certificate element contains exactly one PEM certificate, leaf first.
Keys are unencrypted PEM private keys. Inputs are copied and parsed without
password prompts; key possession, leaf validity and adjacent chain signatures
are checked. Snapshots retain private parsed objects, redact inspection/errors,
and compile fresh contexts before publication. Explicit intermediates, including
an empty array, prevent auto-chaining through the client-auth trust store.
Both roles require TLS >=1.2 and post-check ALPN `h2`. Clients always verify
peer/hostname, including localhost, using system roots plus optional `ca_file`.
Client chain/key must appear together; TLS options are invalid for h2c.

A nonempty `client_ca` requires verified client certificates using only those
anchors, with partial-chain trust enabled deliberately. A scoped intermediate
can be the boundary; sibling issuers under its root are not trusted. Nil means
server-only TLS and no client authentication. There is no optional-client mode.
`host.peer_certificate` returns a detached authenticated client leaf only within
that host's exact handler task; outside/child/other-host/plain/server-only TLS
returns nil. It does not expose or claim a verified chain and does not change
core Context. Applications own authorization based on that identity.

`host.reload_tls(certificate_chain:, private_key:, client_ca:)` replaces the
complete snapshot for newly accepted sockets on the same listener. Generation
starts at 1 and increments only after successful validation; invalid candidates
leave the last good snapshot intact. Existing connections retain their prior
identity and authentication. Every generation has a fresh context/session ID,
OP_NO_TICKET and SESSION_CACHE_OFF. Reuse is disabled; this does not claim that
OpenSSL emits zero TLS 1.3 tickets. Replace a transport to change client identity.

`host.drain_tls(grace: 5)` captures the current generation cutoff and retires
only older accepted connections, including pending handshakes. Old later streams
receive 503 while admitted streams finish. Pending handshakes abort immediately.
A persistent retirement floor prevents old pending sockets from becoming live.
The listener and current/newer generations survive concurrent reloads/drains.
Close supersedes drains. Renewal alone does not instantly revoke existing peers.
There are no file watchers, ACME, secret services or authentication policy DSLs.

## Native application and managed TLS

`Application.new(server:, fallback: nil)` is a reusable Protocol::HTTP callable.
It routes whole registered service prefixes to core, splits the request target once, forwards the raw query to core, and returns
bare 404 or calls the native fallback for unrelated paths. HTTP/1 requests do
not need a stream object. Keep generic compression/cache middleware on fallback
so core owns RPC encoded bytes and limits.

Host accepts `tls_provider:`, `refresh_interval: 60`, `refresh_jitter: 0.1`,
`refresh_timeout: 10` and `on_tls_error:`. `refresh_tls` coalesces immediate source
checks; `tls_status` exposes safe state/generation and separate monotonic
source-check/publication ages. Managed `reload_tls` conflicts with provider
ownership. `TLSConfiguration` also exposes `start`, `refresh`, complete static
`reload`, `close`, `generation` and `status`. Construction is reactor/thread
bound; initial static/provider compilation happens on `start`, before accepts.

Providers return exactly `version`, `certificate_chain`, `private_key` and
`client_ca` symbol keys. A nonempty opaque String version identifies validated
canonical content. Same version/content preserves generation; changed content
under that version fails. A different previous version can deliberately roll
back through a fresh generation. Missing/nil data never disables TLS. Background
errors retain the last good snapshot and emit only safe immutable diagnostics.
Concurrent callers join one owned attempt, and waiter cancellation does not
cancel it. Timer intervals start after each completed attempt. Timeout and
shutdown remain cooperative for arbitrary native/CPU application code.

The earliest served identity-chain expiry is checked against actual wall time
at accept and again at pending handshake start. There is no maximum staleness
setting; established connections retain negotiated state until drain/close.
The [Falcon guide](../connectrpc-falcon/README.md) explains complete provider
semantics and safe diagnostics shared by both hosts.

`ServerSession` is the small hosting SPI: start the worker session, synchronously
`accept(raw)` before scheduling, then call the opaque accepted handle's `serve`.
`schedule` attaches accepted work to the independent session lifetime. The handle
also supports `close`, `wait_closed`, `timeout=` and `remote_address`, and exposes no SSLContext.
`wait_closed` waits for actual connection retirement, including owned task
unwind and final disposal, rather than merely observing invalidation. Retiring
server connections retain their admission slot until this boundary. H2 client
pool accounting remains owned by Async::Pool, which removes a retired pool slot
before joining its connection close.
The session forwards TLS lifecycle/identity operations, bounded `close` and
raw-first `abort` for listener/setup failure.
The optional `stop_accepting:` callback runs after affinity/grace validation,
before refresh stops and connection drain begins. Its one HTTP/1 construction hook is for the optional Falcon-local resource;
HTTP/2 resources do not pretend to implement HTTP/1. The standalone Host keeps
its strict h2 listener and composes the shared session.
The shared session's private resource completion and lifetime-anchor operations
are source-coupled to Falcon's local HTTP/1 resource. Use matched Async/Falcon
package versions; these internals are not an additional public API or SPI.

## Backend boundaries and verification

Enable gzip with `encoding: :gzip` on the core client and server. Client
`accept_encoding:` controls response negotiation independently. Core's
`request_limit:` and `response_limit:` bound serialized/decompressed messages;
the separate wire limits default to those same caps. The transport's
`TransportRequest.response_limit` remains the encoded wire read cap. Defaults
remain identity, and neither host nor transport performs transparent inflation.

Core owns codecs, errors, metadata and serialized byte limits. The backend
reads bounded chunks, reconstructs declared length from backend body.length,
preserves raw repeated leading fields and leading `trailer-*`, and ignores real
HTTP/2 trailing HEADERS for core metadata. The stack normalizes lengths using
Ruby Integer: `+10`, hexadecimal and underscores can normalize before core;
malformed/duplicate lengths may reset/close connections, and singleton header
policies can produce bare errors before core. These outcomes are tested and
are not a full protocol conformance claim. Settings use 16KiB frames, 64KiB
stream windows and bounded stream admission; they do not bound total process
memory or HPACK/header-continuation buffering.

Native HTTP/2 request and response inputs use a private coalescing queue. Stream
WINDOW_UPDATE replenishes only application-consumed or deliberately discarded
DATA bytes. Padding, including its length octet, uses wire credit and is discarded
without entering body bytes or Content-Length. Connection credit is independent,
so unread bodies do not prevent healthy streams or control frames from advancing.
The shared reader never waits for application consumption; serialized socket writes
can still block. Stream over-credit resets that stream with FLOW_CONTROL_ERROR;
connection over-credit fails the connection and its siblings. Upstream parsing and
stream-state errors remain authoritative. This follows [HTTP/2 flow control](https://www.rfc-editor.org/rfc/rfc9113.html#section-5.2).

With the current settings, each queue retains at most 65,536 payload bytes in at
most five data entries, independent of frame count. Each returned native chunk is
at most 16,384 bytes. The core input adapter can retain one such chunk's remainder;
the shared reader can hold one decoded DATA frame of at most 16,384 bytes.
Coalescing/copying can temporarily retain old and new bounded chunks. String byte
length does not bound Ruby backing allocations or RSS. Caller-retained strings,
buffered unary bodies, codecs/compression, socket/TLS buffers and HPACK/headers
are separate allocations. These are ingress payload bounds, not a total memory
or general denial-of-service guarantee. Streaming adds bounded output ownership without changing these ingress limits.

Normal producer EOF drains preceding payload. Closing the consumer releases its
queue once; later DATA is discarded while local output remains live, following
the pinned backend's finish-input and NO_ERROR policy. Reset/error/owned teardown
wakes readers and retires input references. Only peer CANCEL signals explicit
RPC cancellation; credit accounting adds no lifecycle instrumentation.

Discover `async:setup`, `async:test`, `async:check`, `async:build`, `async:package`,
`async:example`, `async:interop`, and `async:verify` with `mise tasks`. The separate
checksummed bundle uses `vendor/async` and coverage uses `coverage/async`.
Independent connect-go tests exercise both directions/codecs over h2c/TLS/mTLS,
with observed HTTP/2/ALPN and large typed wire-tail drains. Original core fixture
and official conformance harnesses remain separate. Installed package checks use
normal offline dependencies and a fresh GEM_HOME outside the checkout. This
graph activates OpenSSL 4.0.2/JSON 3.1.0; installed core NetHTTP TLS/JSON with
identity and gzip is also exercised under that activation. Focused combined
specs cover actual gzip bytes over h2c/TLS in both codecs and independent host
wire/inflated and transport wire/client inflated caps. Nothing is published.
Original project work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg.
Both archives include the complete license and retain the invalid
`https://gems.invalid` push guard. Publishing, releasing or deploying requires
separate, explicit authorization.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional [OpenTelemetry adapter](../connectrpc-otel/README.md).
Native hosts create explicit handles before request admission, activate the
actual handler tasks, and finish after full-write/reset/cleanup evidence.
Connection-level setup/admission before an RPC exists is outside this lifecycle.

## Static signatures

This package owns `sig/` and its discovery manifest. Add
`library "connectrpc-async"` to a strict Steep target with this gem installed.
See the [RBS guide](../../docs/rbs.md) for supported APIs, extension contracts,
partial records, runtime validation and framework-signature limits.
`mise run rbs:check` checks application callers for this package; strict core
source checking is a separate target from backend implementation checking.

Safe unary GET uses core `Client.new(use_get: true)` and declared
`:no_side_effects` methods. Native HTTP/2 requests carry HEADERS with END_STREAM
and a nil body; core owns query encoding, parsing and independent URL/payload
limits. The transport never retries or falls back to POST.

## Explicit unary cancellation

Pass `cancellation: source.token` to core or generated client calls, where
`source = ConnectRPC::CancellationSource.new`. The source is permanent and
shareable; only `source.cancel` initiates cancellation. `timeout: nil` keeps no
RPC deadline or wire timeout. Context offers cooperative `cancelled?`,
`cancellation_reason` and `check!`. See the [core guide](../../README.md) for
listener, arbitration and registration lifetime rules.

The Async transport accepts foreign-thread cancellation through a Promise;
only the owner reactor stops private SDK tasks and performs stream cleanup.
Healthy multiplexed siblings survive. A stuck frame retains the one 100 ms
raw-first whole-connection cleanup fallback. Native HTTP/2 Context observes
only an actual peer RST_STREAM(CANCEL), before input teardown. Other resets,
normal half-close, connection loss and host shutdown do not fabricate explicit
cancellation. Processing completion detaches the source independently of delivery.

## Native streaming

The core [streaming guide](../../docs/streaming.md) documents all three scoped
Connect streaming shapes, typed generated clients/handlers, early headers,
per-message limits and bounded ownership. Native Async/Falcon HTTP/2 supports
both codecs and identity/gzip over h2c/TLS/mTLS. Falcon additionally serves Connect server and client
streaming over HTTP/1.1,
with protobuf/ProtoJSON and identity/gzip over TCP/TLS/mTLS. The standalone
Async Host and original Async Transport remain H2-only. Use the independent
`HTTP1Transport` below for Connect unary, server streaming and client streaming. See the streaming guide for Falcon's cooperative 500 ms total
final-close budget and unchanged 100 ms / 256 KiB discard limits.
The client's one-second absolute HTTP tail policy applies even with a nil RPC
deadline and can reject otherwise valid late EOF or slow intermediaries.

Use `async:conformance` and `async:interop:streaming` for the exact official native
client/server inventories and independent pinned Go streaming matrix. Both belong
to `async:verify`; original unary interoperability remains separate.

## Native gRPC

Native HTTP/2 supports unary, server streaming, client streaming and bidi gRPC
with protobuf, identity/gzip, h2c, verified TLS and mTLS. Configure the core
Client with `protocol: :grpc` and the Server with `:grpc` or `:auto`. Generated
clients and handlers reuse the existing declarations. Portable HTTP/1 execution
retains its early rejection guard. See the [gRPC guide](../../docs/grpc.md) for
status codecs, actual final HEADERS, independent status bounds and native field
retention limits. Connect's one-second terminal tail policy remains Connect-only.

Use `async:conformance:grpc` for exact official inventories and
`async:interop:grpc` for generated official grpc-go peer checks. Both belong
to the owning verify task. `grpc:generation:check` reproduces official Go and
Ruby peer outputs; `grpc:status:check` reproduces private Any/Status descriptors.

## gRPC-Web

The native H2 server supports explicitly enabled binary/text unary and server-stream gRPC-Web. Browser incremental delivery uses text.
See the [Web guide](../../docs/grpc-web.md) for protocol, CORS, bounds and completion rules.

## Explicit HTTP/1.1 client

```ruby
Async do
  transport = ConnectRPC::Async::HTTP1Transport.new(origin: "https://rpc.example.com")
  client = ConnectRPC::Client.new(base_url: "https://rpc.example.com", binding: binding,
                                  transport: transport, use_get: true)
  begin
    client.call(method, message)
    # Existing generated clients and core server_stream/client_stream use it too.
  ensure
    transport.close
  end
end.wait
```

`HTTP1Transport.new(origin:, ca_file: nil, certificate_chain: nil, private_key:
nil, connection_limit: 4, max_requests: 128, connect_timeout: 10)` is an explicit
Connect H1 client. It supports safe unary GET, both codecs and identity/gzip
through core, and SS/CS through the existing streaming SPI. It has no gRPC or
bidi capability. Standalone Async Host remains H2-only; Falcon owns Ruby H1
hosting. Ordinary-thread persistence remains issue #29.

Each connection has one exclusive lease through setup, upload, full HTTP EOF
and cleanup. Pending dials and retiring connections count toward the hard cap;
lease waits count toward admission. Four long streams can occupy every default
slot. Configure capacity or give waiters cancellation/deadlines; fairness is not
promised. `close(grace: 5)` stops admission and drains admitted operations,
including waiters, returning false on forced teardown. Owner exit aborts owned
resources. There is no hidden thread, pipelining, retry, replay or fallback.

Finite RPC deadlines cover every wait including cleanup. Nil RPC deadlines add
no watchdog; setup still has its own combined connect timeout. Foreign-thread
cancellation only signals the reactor. Full native EOF, a successful joined
writer and successful cleanup permit reuse. Any abandoned/failed exchange
retires its connection. Clean close-delimited unary EOF is supported, but
cannot detect an otherwise valid semantic truncation without Content-Length.
TLS unexpected EOF without close_notify remains a transport error.

TLS offers only `http/1.1`, accepts absent or H1 ALPN, and rejects every other
negotiation before request bytes. Peer/hostname verification and mTLS remain
enabled. TCP rejects TLS options. Replace the transport to change TLS identity.

Response reads are independent of a blocked upload. A complete unary non-200
response stops pending output destructively so core can validate its error;
HTTP 200 requires writer success. For streaming, `write` alone reports a writer
failure. Core may then recover a remote error through its bounded terminal-only
policy. An unreported upload-tail failure returns unavailable even if a remote
error terminal arrived, masking that error. `close_send` never raises writer
failure; `finish` requires full HTTP EOF and joined cleanup under the supplied
completion budget. A write-only caller without a deadline can still block when
a peer neither consumes nor closes. See [streaming](../../docs/streaming.md#explicit-http11-client).

`async:conformance:http1` runs separate strict official 1.0.6 native H1 CLIENT
inventories: 386 identity and 352 gzip cases, including safe GET, both codecs,
TCP/TLS and client certificates. `async:interop:http1` runs independent pinned
connect-go H1-only TCP/TLS/mTLS listeners, with actual HTTP/1.1 and ALPN evidence.
Existing H2, NetHTTP and Falcon server inventories remain separate.
