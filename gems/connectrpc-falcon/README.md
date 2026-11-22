<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-falcon

**Falcon worker hosting for ConnectRPC with Rack and Rails fallback.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-falcon?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-falcon)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-falcon` serves ConnectRPC services from real
[Falcon](https://github.com/socketry/falcon) process workers. RPCs dispatch
natively through Protocol::HTTP, and every other request goes to an explicit,
ordinary Rack or Rails fallback app on the same listener.

Use it when you want one Falcon deployment to serve both your Rails or Rack
application and its RPCs, or when you need Connect streaming over HTTP/1.1. It
supports:

- Connect unary and streaming: server and client streaming over HTTP/1.1, and
  all four shapes over HTTP/2
- native gRPC and server gRPC-Web
- plaintext, TLS and mTLS, with TLS negotiating `h2` or `http/1.1`
- worker-local, versioned TLS providers with live refresh and drain

For a standalone HTTP/2 host without Falcon, use
[connectrpc-async](../connectrpc-async/README.md), which this package builds on.

The API is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

```ruby
gem "connectrpc-falcon", "~> 0.2.0"
gem "google-protobuf", ">= 4.36.2", "< 5"
```

Require `connectrpc/falcon`; core and Rails bundles do not load this package.
Falcon's local HTTP/1 resource and the shared Async session use source-coupled
private operations, so use matched `connectrpc-async` and `connectrpc-falcon`
versions.

## Run a service

The [runnable configuration](examples/falcon.rb) uses Falcon's normal service
DSL and Google protobuf's standard `StringValue` message:

```ruby
require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "google/protobuf/wrappers_pb"

service "connectrpc-echo" do
  include ConnectRPC::Falcon::Environment

  url { ENV.fetch("CONNECTRPC_URL", "http://127.0.0.1:9292") }
  count 2

  # Application initialization runs in each Falcon worker before TLS loads.
  prepare! do |_instance|
    # Initialize application-owned dependencies here, after fork.
  end

  connectrpc_server do
    method = ConnectRPC::Method.new(service: "demo.EchoService", name: "Echo",
                                    request_type: Google::Protobuf::StringValue,
                                    response_type: Google::Protobuf::StringValue, idempotency: :no_side_effects)
    service = ConnectRPC::Service.new(name: "demo.EchoService", rpcs: [method])
    ConnectRPC::Server.new(services: [service], handlers: { method.path => ->(message, _context) { message } })
  end

  rack_app do
    ->(_env) { [200, { "content-type" => "text/plain" }, ["Ordinary Rack fallback\n"]] }
  end
end
```

In an application bundle containing `connectrpc-falcon` and `google-protobuf`,
run:

```sh
bundle exec falcon host examples/falcon.rb
```

The example binds localhost, starts two workers and serves
`/demo.EchoService/Echo`. In this repository, `mise run falcon:example` runs an
ephemeral version, checks both codecs and the Rack fallback, then stops all
workers.

### Service environment

Include `ConnectRPC::Falcon::Environment` in each service:

- `connectrpc_server` (required) constructs a `ConnectRPC::Server` in the worker.
- `rack_app` supplies the optional fallback.
- `tls` or `tls_provider` supplies the TLS source; see [TLS lifecycle](#tls-lifecycle).
- `server_options` supplies shared limits and host options such as `cache:`,
  `keepalive:`, and since 0.3.0 `max_concurrent_streams:`,
  `request_body_timeout:`, `response_body_timeout:`, `frame_write_timeout:`,
  `stream_receive_window:`, `connection_receive_window:`,
  `dynamic_receive_windows:` and `max_receive_window:`.
- Since 0.3.0, `connectrpc_close_grace` sets the managed worker stop grace in
  seconds (default 5); see [Shutdown](#shutdown).
- `url` describes the listener.

When combining Falcon environments, include them first and
`ConnectRPC::Falcon::Environment` last. With `Falcon::Environment::Rack`
included first, its `config.ru` application becomes the fallback unless the
service defines `rack_app`. Including `ConnectRPC::Falcon::Environment` first
lets Falcon's application environment replace the listener endpoint, so
starting the service raises `ArgumentError` before binding.

Application `prepare!` runs after fork, before static TLS compilation or the
first provider call. Initialize application-owned database clients there; the
library never opens a database connection. Falcon binds raw TCP in the parent
and owns worker count, readiness, health checks and its default restart policy.

### Listener and connection capacity

`server_options` `max_connections:` caps each worker's connections, HTTP/1.1
and HTTP/2, from raw accept until retirement. Since 0.3.0 the default is 1024,
up from 128, and a worker at its cap stops taking peers from the shared listen
backlog, so a sibling worker accepts them, or the peer waits until a slot frees.
Before 0.3.0 a full worker accepted the peer and closed it. Idle connections
hold their slot until `idle_timeout` (default 60 seconds); the
[Async guide](../connectrpc-async/README.md#host) explains that trade-off for
long-lived gRPC clients. The cap bounds resources and is not denial-of-service
protection; limit connections per client in a proxy.

Since 0.3.0, accept errors follow the
[Async Host policy](../connectrpc-async/README.md#accept-errors): `EMFILE`,
`ENFILE`, `ENOBUFS` and `ENOMEM` retry with a warning and a delay from 5 ms
doubling to 1 second, and `ECONNABORTED` and `EINTR` retry at once, instead of
ending the worker's accept loop. `ConnectRPC::Falcon::Endpoint#bound` binds
close-on-exec by default since 0.3.0, so both the parent-bound listener and a
directly bound one stay out of exec'd child processes. Forked workers still
inherit it. Pass `close_on_exec: false` to keep the descriptor across exec.

### Run inside an application-owned Async task

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

## Routing and protocols

The native application owns each registered service prefix, including unknown
methods. It separates the raw query from the path for core dispatch. Safe unary
GET uses the same application over HTTP/1 and HTTP/2; the app must route
intended GET calls and own cache/security policy. Native RPC retains the core
opaque identity/gzip bytes, wire/inflated caps, deadlines, metadata and errors.

Falcon's generic content encoding/cache middleware wraps only `rack_app`. RPC
compression and byte limits stay outside it. Native handlers do not inherit
Rails callbacks/authentication. Ordinary Rails fallback preserves its
controller, CSRF and session policy.

TLS negotiates `h2` or `http/1.1`; no ALPN selects HTTP/1.1. Plaintext supports
HTTP/2 prior knowledge and HTTP/1. The standalone Async Host remains strict h2.

### Unsupported on this listener

This listener never switches protocols. It ignores HTTP/1.1 `Upgrade` offers,
such as the h2c offer from `curl --http2`, and serves those requests as
ordinary HTTP/1.1 RPC or Rack fallback requests. Their bodies use normal
Content-Length or chunked framing. The connection closes after the response, so
no bytes that follow an offer are parsed as a request, even behind a proxy that
starts tunnelling on the client's offer alone. The Rack fallback still sees the
offer in `rack.protocol` and `HTTP_UPGRADE`, as with plain Falcon. Accepting it
(a 101 status or interim response, a `rack.protocol` or `upgrade` response
header, or full/partial Rack hijack) gets 501 before any upgrade bytes or socket
transfer; other interim responses, such as 103 Early Hints, are sent. The
HTTP/1 connection's response writers refuse a 101 status too, even when code
reaches them through `request.connection`. Writing raw bytes to the connection
stream bypasses HTTP framing and is unsupported. CONNECT, HTTP/2 extended CONNECT and HTTP/2
requests carrying `:protocol` or an `upgrade` field also get 501. WebSockets
and ActionCable are unsupported here. Ordinary enumerable, chunked, streamed
and callable HTTP bodies are supported. Callable bodies must close their
supplied stream according to the Protocol::Rack contract. Plain Falcon and the
existing Rack/Rails integration APIs are unchanged.

### Request input

Native H2 uses the shared Async private bounded ingress for both RPC requests
and ordinary Rack fallback input. Slow or nonreading Rack consumers withhold
stream credit while healthy same-connection streams can advance. Connect
HTTP/1.1 early responses use the bounded
[input retirement policy](#http11-input-retirement). See the
[Async ingress bounds](../connectrpc-async/README.md#backend-boundaries-and-verification)
for padding, cleanup, allocation layers and the limits of this guarantee.

## Rack fallback caching

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

Opt-in uses Falcon's async-http-cache semantics, described here for Falcon
0.57.0 / async-http-cache 0.4.6; newer allowed releases may differ.

- Cache keys use authority, method and path, plus response `Vary` headers.
  Custom authorization headers are not automatically cache keys.
- GET/HEAD requests without a body, `Authorization` or `Cookie` can have 200,
  203, 300, 301, 302, 404 and 410 responses cached without an explicit
  `Cache-Control` header.
- Entries without `max-age` can persist for the worker's lifetime.

Applications must set `Cache-Control` and `Vary` so responses are safe to share.
Use `private` or `no-store` for unshared content; response `no-cache` alone is
insufficient. Use appropriate `Vary` headers for deliberately shared content
that depends on request headers. These are upstream cache semantics, not a
promise of full RFC cache policy support.

## TLS lifecycle

### Provide TLS material

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

TLS keeps the shared Async policy: explicit-only trust anchors, scoped partial
chains, TLS 1.2+, fresh session context per generation, disabled session cache
and `OP_NO_TICKET`. This does not claim that OpenSSL emits zero TLS 1.3 tickets.

### Snapshots and handshakes

The worker caches immutable compiled snapshots. Handshakes perform no provider
or database work. Every raw accept captures one generation before scheduling,
including no-SNI/IP clients and sockets waiting to send their ClientHello.
Pending handshakes keep that captured identity even if a refresh completes.

### Refresh

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
There is no maximum-staleness policy. A candidate whose chain has already
expired fails validation as `:invalid`. Since 0.3.0, an identity that expires
while it is published keeps being served to new and pending handshakes, so a
verifying client fails with a certificate-expired error instead of an EOF, and
each worker reports `:expired` once per generation when the wall clock reaches
the earliest identity-chain `not_after`. It re-reads the wall clock at least
every 60 seconds. Before 0.3.0 expiry rejected new accepts and pending
handshake starts. Established connections retain their negotiated
identity/trust.

### Operations and status

- `refresh_tls` requests an immediate provider check.
- `reload_tls` accepts the three static TLS fields and conflicts with provider
  ownership.
- `tls_generation` is initially 1 for TLS.
- `tls_status` returns safe state, generation, category, `source_check_age` and
  `publication_age` in monotonic seconds; it never exposes the source version,
  certificates, keys or fingerprints. `category` is the last reported event.
  Since 0.3.0, `identity_expires_at` is the served identity chain's earliest
  `not_after` as a frozen `Time`, so `identity_expires_at <= Time.now` tests
  expiry whatever `category` says.
- `on_tls_error:` receives an immutable safe category/status event, including
  `:expired` since 0.3.0. Default diagnostics are fixed safe text; reporter
  exceptions are isolated. Calling `close` or `refresh_tls` from the reporter
  raises `LifecycleError`; `reload_tls` and `drain_tls` work there.

Initial failure prevents successful TLS/application traffic before Falcon
reports startup failure. TCP backlog can still queue connections on the
parent-bound listener.

### Drain and client identity

`drain_tls(grace: 5)` captures a generation cutoff and retires older connections.
Pending older handshakes close immediately. Already admitted responses can
finish. Older HTTP/2 connections get the two-phase GOAWAY drain with debug data
`tls_rotation`, so streams opened before its cutoff are still served and later
ones are refused with `REFUSED_STREAM`. Later HTTP/1 requests get a bare 503.

`peer_certificate` returns a detached authenticated client leaf only in the
exact session handler task. Child tasks, other servers, plaintext and
server-only TLS have no client identity. It does not claim that a
peer-presented chain is a verified chain.

## Shutdown

`close(grace: 5)` and a normal managed worker stop both end accepts and TLS
refresh, then perform bounded connection drain. HTTP/2 connections get the
two-phase GOAWAY drain with debug data `graceful_stop` and keep serving streams
opened before its cutoff; HTTP/1 requests that arrive during the drain get a
bare 503. HTTP/1 completion includes fixed/chunked wire bytes and
response-finished callbacks. On grace expiry, teardown shuts raw TCP down
before cancelling owned work, then performs buffered/TLS cleanup and closes the
descriptor once that work has joined. Callback close is guarded against reentry.
Fatal teardown or owner cancellation outside the managed stop
boundary uses raw-first abort and offers no response-delivery guarantee.
HTTP response completion is separate from TLS `close_notify`; orderly TLS
shutdown is not promised.

Since 0.3.0, managed worker stop uses the service's `connectrpc_close_grace`
(default 5 seconds). Keep it below the Falcon container's graceful stop
timeout, after which the container kills workers. `falcon host` uses
async-container's default of 10 seconds, which `ASYNC_CONTAINER_GRACEFUL_STOP`
overrides.

### Close results

`Server#close` returns `bool?`:

- External callers join actual cleanup and receive the final boolean, true for
  drained or false for forced teardown.
- A resource-owned handler initiating positive-grace close starts the ordinary
  asynchronous drain and returns nil while pending. Admitted siblings keep their
  grace and can continue processing.
- Owned repeated requests and cleanup/close coordinator reentry also return nil
  while pending, without joining cleanup that must join them. Nil neither
  decides the outcome nor publishes completion.
- Repeated requests retain the first closer and expiry even when passed a
  different grace.
- Once closed, all calls return that same boolean, including coordinator
  reentry.
- Zero grace still forces teardown; explicit abort and already-decided outcomes
  are preserved.

### Connection retirement

External abort and close callers wait for actual connection retirement,
including owned processors, output tasks, cleanup descendants and final
disposal. Owned tasks initiating or reentering abort request retirement without
waiting on cleanup that must join them. A session accepted handle's existing
`wait_closed` operation uses this same boundary, so the active connection metric
and server connection slot remain retained until cleanup completes. Each
external abort/close joiner retains its own first interruption through the join.

An external close caller interrupted during a nonzero graceful drain requests
forced session retirement, joins actual cleanup, then raises its identical first
exception. Competitors keep their own result or exception, and an
already-decided outcome remains unchanged.

Private task joins distinguish a resolved result from actual finalization. They
block on required children and task-local progress/finalization notifications,
retaining captured child identities through reparenting or transient changes.
A scheduling opportunity after raw failure establishes no completion boundary;
all retained task joins still precede disposal and retirement publication.

### Limits of the grace period

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

## HTTP/2 keepalive and connection age

`Server.new(keepalive:)` takes the Async package's `ServerKeepalive` (default
`ServerKeepalive.new`, nil turns it off) and applies it to native HTTP/2
connections exactly as the Async `Host` does: server PINGs, grpc-go PING
enforcement with `too_many_pings`, and the graceful age drain. `idle_timeout`
drains idle HTTP/2 connections gracefully. HTTP/1 connections have no keepalive
and are still closed once idle. See
[Server keepalive](../connectrpc-async/README.md#server-keepalive).

Since 0.3.0, `max_concurrent_streams:` (default 100) and `request_body_timeout:`
(default 60 seconds) also apply to native HTTP/2 connections as on the Async
`Host`: a stream over the connection's limit or over full `max_requests`
admission is refused with `REFUSED_STREAM` while its HEADERS are read, before
any of its DATA, and a stalled request body other than a client or bidi stream
is reset with `CANCEL`. On HTTP/1.1, `request_body_timeout` bounds only the
[read of unread input](#http11-input-retirement) before a Connect response. A
refusal for full admission counts once in `requests_total`. See [Stream
admission](../connectrpc-async/README.md#stream-admission). HTTP/1 requests keep
the bare 503 when admission is full.

Since 0.3.0, `stream_receive_window:` (default 65,536) and
`connection_receive_window:` (default 16 MiB) size native HTTP/2 request
uploads as on the Async `Host`, with the same memory and throughput trade-off.
`dynamic_receive_windows:` (default true) grows them at run time from BDP PINGs
up to `max_receive_window:` (default 1 MiB, below grpc-go's 16 MiB to bound
unread request payload), as on the `Host`.
See [Flow-control windows](../connectrpc-async/README.md#flow-control-windows).
They do not apply to HTTP/1.

Since 0.3.0, native HTTP/2 responses are bounded in the same way as on the Async
`Host`. `response_body_timeout:` (default 60 seconds, nil turns it off) resets
with `CANCEL` a unary, client-streaming or Rack fallback response that waits
that long for flow-control credit; server-streaming and bidi responses are
exempt. Each frame write must finish within `frame_write_timeout:` (default 60
seconds, nil turns it off), or the connection is aborted. On Linux 4.6 and
later, a blocked write is renewed for another bound while the peer keeps
acknowledging data, so a slow reader whose TCP acknowledgements keep advancing
stays connected and one that stops is aborted one to two bounds after the later
of its last progress and the blocked write's start, much like grpc-go's
`TCP_USER_TIMEOUT`. On other platforms and older Linux kernels each write keeps
a fixed deadline, which a slow reader can miss because a blocked write waits for
the peer to drain about 950 KB; raise `frame_write_timeout:` there. Neither
grpc-go nor Go's net/http has such a bound by default. See [Stream
admission](../connectrpc-async/README.md#stream-admission). Neither bound
applies to HTTP/1 connections.

`falcon:interop:grpc` also runs a grpc-go bidi stream through an age drain on
two real workers over TLS. On a second pair of workers with the default
keepalive, the grpc-go default client's large uploads and downloads check that
its BDP PINGs are acknowledged and that the workers send their own and grow
their stream windows without a GOAWAY.

```ruby
server_options do
  { keepalive: ConnectRPC::Async::ServerKeepalive.new(max_connection_age: 1800, max_connection_age_grace: 30) }
end
```

## Native streaming

The core [streaming guide](../../docs/streaming.md) documents all three scoped
Connect streaming shapes, typed generated clients/handlers, early headers,
per-message limits and bounded ownership. Native Async/Falcon HTTP/2 supports
both codecs and identity/gzip over h2c/TLS/mTLS. Falcon additionally serves
Connect server and client streaming over HTTP/1.1, with protobuf/ProtoJSON and
identity/gzip over TCP/TLS/mTLS. The standalone Async Host and original Async
Transport remain H2-only. The explicit `ConnectRPC::Async::HTTP1Transport`
supports Connect unary, safe GET, server streaming and client streaming in an
application-owned reactor.

The streaming guide defines how Falcon reads unread HTTP/1.1 input before its
response and its cooperative 500 ms final-close budget. Native Connect streaming
clients have a separate, provisional one-second absolute HTTP tail policy. It
applies even with a nil RPC deadline and can reject otherwise valid late EOF or
slow intermediaries.

### HTTP/1.1 input retirement

Incomplete H1 uploads disable reuse at response-head resolution. Since 0.3.0,
Falcon then reads the unread body before it writes the response head, as Go's
`net/http` server does, so a client that sends its whole upload before reading
neither deadlocks nor loses the response to a reset on its late write:

- Falcon reads at most 256 KiB, and leaves a declared `Content-Length`
  remainder of 256 KiB or more unread. A chunked read returns at most 64 KiB,
  so the last one can pass the limit by less than that. An input adapter's
  readahead remainder is released, not counted.
- `request_body_timeout` (default 60 seconds) bounds each read wait, and the
  Connect deadline bounds the whole read. With neither, a stalled client holds
  the connection until owner shutdown, as `net/http` without `ReadTimeout` does.
- A stalled, expired or malformed remainder still receives the response.
- While the read runs, Falcon holds the original processor's remaining output
  (its response message and terminal) in memory, so processing never waits on
  the read or delivers a truncated message at its deadline.
- A head published before any output, such as client-stream headers sent
  before the handler reads its input, is written at once, and the remainder is
  read before the first output chunk.
- A bidi 505 and an unanswered `Expect: 100-continue` request skip the read.
  Falcon never sends `100 Continue`.

One absolute final-close expiry is anchored at the actual final HTTP flush plus
500 ms, before body callbacks or lifecycle observation. After those, Falcon
half-closes raw TCP writes and waits without reading until that expiry, like
`net/http`'s `rstAvoidanceDelay`.
Following pipelined requests are never parsed or dispatched. Admission/socket
ownership lasts until retirement and original processing join. Earlier owner
shutdown can shorten either phase; failed delivery receives no success grace.

Timers are cooperative and may overshoot under scheduler delay. Holding
admission during the read and the remaining grace increases resource retention.
This is best effort reset avoidance, with no arbitrary network-delivery
guarantee. The [support
contract](../../docs/support.md#falcon-connect-h1-retirement) promises this
behavior and treats the 256 KiB and 500 ms values as provisional tuning values.

### Streaming conformance

CI requires the separate `falcon:conformance:http1` production profiles to pass
all 174 identity and 160 gzip cases on Ruby 3.4.12 and 4.0.9, with unchanged
assertions and inventories. Web rejection-head ordering is repaired separately,
with its gate excluded from Connect delivery. Historical Web
failures and the baseline repair remain recorded in the
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
to the owning verify task. `falcon:interop:grpc` first reproduces the official Go
and Ruby peer outputs through the Falcon bundle, the same check root
`grpc:generation:check` runs through the core bundle. `grpc:status:check`
reproduces private Any/Status descriptors.

## gRPC-Web

Native H1 and H2 support explicitly enabled binary/text unary and server-stream
gRPC-Web. H1 retains original processor identity and admission through
wire/callback completion. Browser incremental delivery uses text. See the
[Web guide](../../docs/grpc-web.md) for protocol, CORS, bounds and completion
rules.

### Rejected HTTP/1 Web input

Rejected native Web HTTP/1 input now has an explicit resource-policy allowance:
one absolute monotonic 100 ms budget to finish the original fixed/chunked body
framing. Progress never resets it. The valid grpc-timeout captured at original
application entry can shorten this allowance; cancellation, host drain and
owner abort retain control. Reads use protocol-http1's framed body, which
returns at most 64 KiB per read, without aggregating the upload or consuming the
next request. An existing input-adapter remainder can coexist with one discard
read, each at most 64 KiB;
this is not a total allocation or RSS bound. Successful calls already consume
their input and do not enter this tail policy.

On expiry or malformed framing, cleanup retires only the affected connection,
closing raw TCP before body/TLS cleanup. The original RPC processing outcome
remains separate from aborted delivery and cleanup failure. Terminal bytes stay
staged until the original processor and application cleanup succeed. This is a
best effort within bounded cleanup, not a delivery promise for arbitrarily slow
uploads. The tail budget runs once and does not restart or stack; existing
backend input-cancellation and teardown allowances remain separate. The support
contract classifies the 100 ms allowance as a
[provisional budget](../../docs/support.md#falcon-connect-h1-retirement).

## Explicit unary cancellation

Pass `cancellation: source.token` to core or generated client calls, where
`source = ConnectRPC::CancellationSource.new`. The source is permanent and
shareable; only `source.cancel` initiates cancellation. `timeout: nil` without
`deadline:` keeps no RPC deadline or wire timeout. Context offers cooperative `cancelled?`,
`cancellation_reason` and `check!`. See the
[unary guide](../../docs/unary.md#cancellation) for listener, arbitration and
registration lifetime rules.

The Async transport accepts foreign-thread cancellation through a Promise;
only the owner reactor stops private SDK tasks and performs stream cleanup.
Healthy multiplexed siblings survive. A stuck frame retains the one 100 ms
raw-first whole-connection cleanup fallback. Native HTTP/2 Context observes
only an actual peer RST_STREAM(CANCEL), before input teardown. Other resets,
normal half-close, connection loss and host shutdown do not fabricate explicit
cancellation. Processing completion detaches the source independently of delivery.

Falcon HTTP/1 and portable Rack/Rails do not add peer detection or application
token env injection. Application handlers remain cooperative.

## Interceptors

Server interceptors configured on the core Server from `connectrpc_server` run
for every native RPC, on HTTP/2 and HTTP/1.1:
[unary](../../docs/unary.md#interceptors) calls through `server_unary`, and
[streams](../../docs/streaming.md#interceptors) through `server_stream`. They
run after core decodes a unary or server-streaming request, around the handler,
with the first interceptor listed outermost. They run in the session's
processing task, so `peer_certificate` works inside them as it does in the
handler.

The native Server's interceptors do not apply to the explicit Rack fallback. A
Rack mount behind it configures its own Server, and a Rails application uses
its controllers'
[`connect_interceptors`](../connectrpc-on-rails/README.md#interceptors) hook.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional
[OpenTelemetry adapter](../connectrpc-otel/README.md). Native hosts create
explicit handles before a request's handler starts, activate the actual handler
tasks, and finish after full-write/reset/cleanup evidence. An HTTP/2 stream
refused at HEADERS because admission is full is observed once after the refusal,
as on the [Async Host](../connectrpc-async/README.md#rpc-observation).
Connection-level setup/admission before an RPC exists is outside this
lifecycle.

## Static signatures

This package owns `sig/` and its discovery manifest. Add `library "connectrpc"`,
`library "connectrpc-async"` and `library "connectrpc-falcon"` to a strict Steep
target with this gem installed. The manifest lists only standard libraries, so
name each connectrpc library explicitly; this also works with `rbs collection`.
See the [RBS guide](../../docs/rbs.md) for supported APIs, extension contracts,
partial records, runtime validation and framework-signature limits.
`mise run rbs:check` checks application callers for this package; strict core
source checking is a separate target from backend implementation checking.

The finite upstream compatibility signatures in `rbs/async_service_0_25` are
explicit opt-ins, separate from normal gem discovery. See the
[RBS guide](../../docs/rbs.md#opt-in-framework-compatibility) for tested
versions, supported framework forms and how to avoid duplicate
application-owned declarations.

## Development

The independent bundle resolves the newest allowed dependencies into
`vendor/falcon` and reports full optional library coverage under
`coverage/falcon/<Ruby version>`. `floors:falcon` repeats specs and package
checks with every runtime dependency at its gemspec floor. `canary:falcon` is the
non-gating pins-lifted lane: it runs the upstream contract spec and the suite with
connectrpc-async's async-http and protocol-http2 patch pins lifted:

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
`falcon:soak` runs the non-gating long-running lifecycle campaign; see
[development](../../docs/development.md#lifecycle-campaign).

## License

This package is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg. It is
released with the whole [release train](../../docs/releasing.md).
