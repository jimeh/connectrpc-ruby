# connectrpc-falcon

Optional Falcon process-worker hosting for Connect unary and streaming RPC, with native
Protocol::HTTP dispatch and an explicit ordinary Rack/Rails fallback.
Require `connectrpc/falcon`; core and Rails bundles do not load this package.
The API is experimental. This package is MIT licensed; publication remains
disabled by `allowed_push_host: https://gems.invalid`.

See the [supported 0.x contract](../../docs/support.md) for the target support
profile, matched package versions, compatibility policy and adoption gates.

## Run a service

The [runnable configuration](examples/falcon.rb) uses Falcon's normal service
DSL and Google protobuf's standard `StringValue` message. In an application
bundle containing `connectrpc-falcon` and `google-protobuf`, run:

```sh
bundle exec falcon host examples/falcon.rb
```

The example binds localhost, starts two workers and serves
`/demo.EchoService/Echo`. `mise run falcon:example` runs an ephemeral version,
checks both codecs and the Rack fallback, then stops all workers.

Include `ConnectRPC::Falcon::Environment` in each service. Define
`connectrpc_server` to construct a `ConnectRPC::Server` in the worker. Optional
`rack_app`, `tls`, `tls_provider` and `server_options` methods supply the fallback,
TLS source and shared limits. `url` describes the listener. Application
`prepare!` runs after fork, before static TLS compilation or the first provider
call. Initialize application-owned database clients there; the library never
opens a database connection. Falcon binds raw TCP in the parent and owns
worker count, readiness, health checks and its default restart policy.

For direct use inside an application-owned Async task:

```ruby
endpoint = ConnectRPC::Falcon::Endpoint.parse("https://127.0.0.1:9292")
host = ConnectRPC::Falcon::Server.new(
  server: connect_server,
  endpoint: endpoint,
  rack_app: ordinary_rack_app,
  tls_provider: -> { application_tls_snapshot }
)
begin
  host.run
  # Run the rest of the application in this reactor.
ensure
  host.close(grace: 5)
end
```

Rack fallback caching is disabled by default (`cache: false`). Falcon's fallback
content encoding remains active, and native RPC bypasses both middleware layers.
To enable Falcon's existing shared in-memory cache within each worker, pass
`cache: true` to `ConnectRPC::Falcon::Server.new`. In a service configuration,
use the supported `server_options` path:

```ruby
server_options { { cache: true } }
```

The inherited Falcon `cache` DSL setting does not configure this host. `cache:`
must be `true` or `false`; with no `rack_app`, either value has no effect.

Opt-in retains the pinned Falcon 0.57.0 / async-http-cache 0.4.6 semantics. Cache
keys use authority, method and path, plus response `Vary` headers. Custom
authorization headers are not automatically cache keys. GET/HEAD requests
without a body, `Authorization` or `Cookie` can have 200, 203, 300, 301, 302, 404
and 410 responses cached without an explicit `Cache-Control` header. Entries
without `max-age` can persist for the worker's lifetime.
Applications must set `Cache-Control` and `Vary` so responses are safe to share.
Use `private` or `no-store` for unshared content; response `no-cache` alone is
insufficient. Use appropriate `Vary` headers for deliberately shared content
that depends on request headers. These are upstream cache semantics, not a
promise of full RFC cache policy support.

A TLS provider returns a complete Hash with exactly these symbol keys:

```ruby
{
  version: "application-owned-opaque-version",
  certificate_chain: [leaf_pem, intermediate_pem],
  private_key: private_key_pem,
  client_ca: [explicit_client_ca_pem]
}
```

Use explicit `client_ca: nil` for server-only TLS. A missing/nil provider result,
missing field, invalid key/chain/trust or source exception fails closed; it never
selects plaintext. Listener scheme and TLS settings must agree. The same
complete configuration without `version` can be supplied as static `tls:`.

## TLS lifecycle

The worker caches immutable compiled snapshots. Handshakes perform no provider
or database work. Every raw accept captures one generation before scheduling,
including no-SNI/IP clients and sockets waiting to send their ClientHello.
Pending handshakes keep that captured identity even if a refresh completes.
TLS negotiates `h2` or `http/1.1`; no ALPN selects HTTP/1.1. Plaintext supports
HTTP/2 prior knowledge and HTTP/1. The standalone Async Host remains strict h2.

Provider defaults are `refresh_interval: 60`, `refresh_jitter: 0.1` and
`refresh_timeout: 10`. Scheduling uses monotonic time after completed source
checks, with uniform positive/negative jitter. Concurrent `refresh_tls` callers
join one worker-local attempt. Cancelling one waiter does not cancel that
attempt. Recursive provider refresh/close calls fail safely. These cooperative
timeouts and shutdown bounds cannot preempt arbitrary native or CPU-bound code.
There is no fixed database-outage propagation guarantee or fleet-atomic update.

Every result is validated. Equal versions with equal canonical identity/trust
content preserve generation and context; PEM whitespace and trust-anchor order
do not cause churn. Equal versions with changed content fail. A different
version can deliberately restore previously used material with a new generation.
Failed refreshes retain the last good snapshot and source-check timestamp.
There is no maximum-staleness policy. Actual wall-clock expiry of the earliest
served identity-chain certificate rejects new accepts and pending handshake
starts. Established connections retain their negotiated identity/trust.

`refresh_tls` requests an immediate provider check. `reload_tls` accepts the
three static TLS fields and conflicts with provider ownership. `tls_generation`
is initially 1 for TLS. `tls_status` returns safe state, generation, category,
`source_check_age` and `publication_age` in monotonic seconds; it never exposes
the source version, certificates, keys or fingerprints. `on_tls_error:` receives
an immutable safe category/status event. Default diagnostics are fixed safe
text; reporter exceptions are isolated. Initial failure prevents successful
TLS/application traffic before Falcon reports startup failure. TCP backlog can
still queue connections on the parent-bound listener.

`drain_tls(grace: 5)` captures a generation cutoff and retires older connections.
Pending older handshakes close immediately. Already admitted responses can
finish; later requests/streams are refused. `peer_certificate` returns a detached
authenticated client leaf only in the exact session handler task. Child tasks,
other servers, plaintext and server-only TLS have no client identity. It does
not claim that a peer-presented chain is a verified chain.

TLS keeps the shared Async policy: explicit-only trust anchors, scoped partial
chains, TLS 1.2+, fresh session context per generation, disabled session cache
and `OP_NO_TICKET`. This does not claim that OpenSSL emits zero TLS 1.3 tickets.

## Routing and shutdown

The native application owns each registered service prefix, including unknown
methods. It separates the raw query from the path for core dispatch. Safe unary GET
uses the same application over HTTP/1 and HTTP/2; the app must route intended
GET calls and own cache/security policy. Native RPC retains the core
opaque identity/gzip bytes, wire/inflated caps, deadlines, metadata and errors.
Falcon's generic content encoding/cache middleware wraps only `rack_app`.
Native handlers do not inherit Rails callbacks/authentication. Ordinary Rails
fallback preserves its controller, CSRF and session policy.

This listener rejects upgrades, CONNECT tunnels, full/partial Rack hijack and
protocol upgrades before successful upgrade bytes or socket transfer. WebSockets
and ActionCable are unsupported here. Ordinary enumerable, chunked, streamed
and callable HTTP bodies are supported. Callable bodies must close their supplied
stream according to the Protocol::Rack contract. Plain Falcon and the existing
Rack/Rails integration APIs are unchanged.

Native H2 uses the shared Async private bounded ingress for both RPC requests and
ordinary Rack fallback input. Slow or nonreading Rack consumers withhold stream
credit while healthy same-connection streams can advance. Connect HTTP/1.1 early responses use the bounded input retirement policy below.
See the [Async ingress bounds](../connectrpc-async/README.md#backend-boundaries-and-verification)
for padding, cleanup, allocation layers and the limits of this guarantee.

`close(grace: 5)` and normal managed worker stop stop accepts/refresh, then
perform bounded connection drain. HTTP/1 completion includes fixed/chunked wire
bytes and response-finished callbacks. On grace expiry, teardown closes raw TCP
before cancelling owned work and buffered/TLS cleanup. Callback close is guarded
against reentry. Fatal teardown or owner cancellation outside the managed stop
boundary uses raw-first abort and offers no response-delivery guarantee.
HTTP response completion is separate from TLS `close_notify`; orderly TLS
shutdown is not promised.

External abort and close callers wait for actual connection retirement, including
owned processors, output tasks, cleanup descendants and final disposal. Owned
tasks initiating or reentering abort request retirement without waiting on
cleanup that must join them. A session accepted handle's existing `wait_closed`
operation uses this same boundary, so the active connection metric and server
connection slot remain retained until cleanup completes. Each external abort/close
joiner retains its own first interruption through the join.

The grace period bounds cooperative drain before forced teardown; it is not a
hard cleanup timer. Non-yielding application/native work can delay retirement
indefinitely. Ordinary owner cancellation leaves independent cleanup running
while an external waiter or lifetime guard keeps the tree alive; terminating the
reactor cannot guarantee completion. H2 client pool cleanup can also outlast an
RPC deadline, as described in the [Async guide](../connectrpc-async/README.md).

Falcon's local HTTP/1 resource and the shared Async session use source-coupled
private completion and lifetime-anchor operations. Use matched Async/Falcon
package versions. These operations add no public API or SPI and change neither
HTTP/1 response timing nor the input retirement limits below.

## Development

The independent checksummed bundle uses `vendor/falcon` and reports full optional
library coverage under `coverage/falcon/<Ruby version>`:

```sh
mise run falcon:setup
mise run falcon:test
mise run falcon:check
mise run falcon:verify
```

`falcon:tools`, `falcon:build`, `falcon:package`, `falcon:example` and
`falcon:interop` are separate discoverable leaves. Both Ruby lanes exercise real
localhost protocols and workers. The Go peer uses the repository's independently
pinned Connect/Go graph. This is unary subset evidence, not full conformance.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional [OpenTelemetry adapter](../connectrpc-otel/README.md).
Native hosts create explicit handles before request admission, activate the
actual handler tasks, and finish after full-write/reset/cleanup evidence.
Connection-level setup/admission before an RPC exists is outside this lifecycle.

## Static signatures

This package owns `sig/` and its discovery manifest. Add
`library "connectrpc-falcon"` to a strict Steep target with this gem installed.
See the [RBS guide](../../docs/rbs.md) for supported APIs, extension contracts,
partial records, runtime validation and framework-signature limits.
`mise run rbs:check` checks application callers for this package; strict core
source checking is a separate target from backend implementation checking.

The finite upstream compatibility signatures in `rbs/async_service_0_25` are explicit
opt-ins, separate from normal gem discovery. See the [RBS guide](../../docs/rbs.md#opt-in-framework-compatibility)
for tested versions, supported framework forms and how to avoid duplicate
application-owned declarations.

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

Falcon HTTP/1 and portable Rack/Rails do not add peer detection or application
token env injection. Application handlers remain cooperative.

## Native streaming

The core [streaming guide](../../docs/streaming.md) documents all three scoped
Connect streaming shapes, typed generated clients/handlers, early headers,
per-message limits and bounded ownership. Native Async/Falcon HTTP/2 supports
both codecs and identity/gzip over h2c/TLS/mTLS. Falcon additionally serves Connect server and client
streaming over HTTP/1.1,
with protobuf/ProtoJSON and identity/gzip over TCP/TLS/mTLS. The standalone
Async Host and original Async Transport remain H2-only. The explicit
`ConnectRPC::Async::HTTP1Transport` supports Connect unary, safe GET, server
streaming and client streaming in an application-owned reactor.

The streaming guide defines Falcon's cooperative 500 ms total final-close budget
and unchanged 100 ms / 256 KiB discard limits. Native Connect streaming clients
have a separate one-second absolute HTTP tail policy. It applies even with a nil
RPC deadline and can reject otherwise valid late EOF or slow intermediaries.

Incomplete H1 uploads disable reuse at response-head resolution. Falcon delivers
the full response and HTTP EOF after original processor cleanup, then half-closes
raw TCP writes. One absolute final-close expiry starts at the actual final HTTP
flush plus 500 ms, before body callbacks or lifecycle observation. Original-input
discard retains one absolute 100 ms allowance and 256 KiB cap, including adapter
remainders and full 64 KiB read reservations. After discard stops, Falcon waits
only the remaining part of the original 500 ms budget, without further reads.
Following pipelined requests are never parsed or dispatched. Admission/socket
ownership lasts until retirement and original processing join. Earlier owner
shutdown can shorten either phase; failed delivery receives no success grace.

Both timers are cooperative and may overshoot under scheduler delay. Holding
admission during remaining grace increases resource retention. This is best
effort reset avoidance, with no arbitrary network-delivery guarantee. The separate
`falcon:conformance:http1` production profiles pass 174/174 identity and 160/160
gzip cases on Ruby 3.4.12 and 4.0.9 with unchanged assertions and inventories.
Web rejection-head ordering is repaired separately, with its gate excluded from
Connect response-first delivery. Historical Web failures and the baseline
repair remain recorded in the
[conformance status](../../conformance/README.md#connect-http11-server-profiles).

`falcon:conformance` runs actual server profiles. `falcon:interop:streaming`
exercises real workers with the pinned Go streaming client. Both belong to
`falcon:verify`; duplicated native-client runs are not Falcon server evidence.

## Native gRPC

Native HTTP/2 supports unary, server streaming, client streaming and bidi gRPC
with protobuf, identity/gzip, h2c, verified TLS and mTLS. Configure the core
Client with `protocol: :grpc` and the Server with `:grpc` or `:auto`. Generated
clients and handlers reuse the existing declarations. Portable HTTP/1 execution
retains its early rejection guard. See the [gRPC guide](../../docs/grpc.md) for
status codecs, actual final HEADERS, independent status bounds and native field
retention limits. Connect's one-second terminal tail policy remains Connect-only.

Use `falcon:conformance:grpc` for exact official inventories and
`falcon:interop:grpc` for generated official grpc-go peer checks. Both belong
to the owning verify task. `grpc:generation:check` reproduces official Go and
Ruby peer outputs; `grpc:status:check` reproduces private Any/Status descriptors.

## gRPC-Web

Native H1 and H2 support explicitly enabled binary/text unary and server-stream gRPC-Web. H1 retains original processor identity and admission through wire/callback completion. Browser incremental delivery uses text.
See the [Web guide](../../docs/grpc-web.md) for protocol, CORS, bounds and completion rules.

Rejected native Web HTTP/1 input now has an explicit resource-policy allowance:
one absolute monotonic 100 ms budget to finish the original fixed/chunked body
framing. Progress never resets it. The valid grpc-timeout captured at original
application entry can shorten this allowance; cancellation, host drain and
owner abort retain control. Reads use the pinned HTTP/1 body's at-most-64 KiB
chunks, without aggregating the upload or consuming the next request. An existing
input-adapter remainder can coexist with one discard read, each at most 64 KiB;
this is not a total allocation or RSS bound. Successful
calls already consume their input and do not enter this tail policy.

On expiry or malformed framing, cleanup retires only the affected connection,
closing raw TCP before body/TLS cleanup. The original RPC processing outcome
remains separate from aborted delivery and cleanup failure. Terminal bytes stay
staged until the original processor and application cleanup succeed. This is a
best effort within bounded cleanup, not a delivery promise for arbitrarily slow
uploads. The tail budget runs once and does not restart or stack; existing
backend input-cancellation and teardown allowances remain separate.
