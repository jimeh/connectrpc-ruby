# connectrpc-falcon

Optional Falcon process-worker hosting for Connect unary RPC, with native
Protocol::HTTP dispatch and an explicit ordinary Rack/Rails fallback.
Require `connectrpc/falcon`; core and Rails bundles do not load this package.
The API is experimental. This package is MIT licensed; publication remains
disabled by `allowed_push_host: https://gems.invalid`.

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

`close(grace: 5)` and normal managed worker stop stop accepts/refresh, then
perform bounded connection drain. HTTP/1 completion includes fixed/chunked wire
bytes and response-finished callbacks. On grace expiry, teardown closes raw TCP
before cancelling owned work and buffered/TLS cleanup. Callback close is guarded
against reentry. Fatal teardown or owner cancellation outside the managed stop
boundary uses raw-first abort and offers no response-delivery guarantee.
HTTP response completion is separate from TLS `close_notify`; orderly TLS
shutdown is not promised.

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
