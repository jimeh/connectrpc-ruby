<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-prometheus

**Prometheus metrics for ConnectRPC clients and servers.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-prometheus?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-prometheus)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-prometheus` records server, client and Async client connection
metrics for ConnectRPC in a `prometheus-client` registry: RPC counts by code,
latency, message counts and encoded sizes, server rejections and connection
health. Names and labels follow
[go-grpc-middleware `providers/prometheus`](https://github.com/grpc-ecosystem/go-grpc-middleware/tree/main/providers/prometheus),
so dashboards built on go-grpc-prometheus queries keep working. Use the
[Yabeda package](../connectrpc-yabeda/README.md) instead when your application
records metrics through Yabeda.

The package is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

```ruby
gem "connectrpc-prometheus", "~> 0.2.0"
```

The only runtime dependencies are `connectrpc ~> 0.2.0` and
`prometheus-client >= 5.0, < 6`. The shared, backend-neutral layer lives in core
as the optional `require "connectrpc/metrics"`. Core and the HTTP/framework
packages do not depend on Prometheus.

## Usage

```ruby
require "connectrpc/prometheus"

metrics = ConnectRPC::Prometheus::Metrics.new(registry: Prometheus::Client.registry, namespace: "grpc")
instrumentation = ConnectRPC::Instrumentation.new(
  subscribers: [metrics.subscriber], on_error: ->(error) { application_logger.warn(error.class.name) }
)
# Pass instrumentation: instrumentation to ConnectRPC::Server.new and ConnectRPC::Client.new.
```

`Metrics.new` registers every instrument once. Constructing a second `Metrics`
with the same namespace on one registry raises the registry's
`AlreadyRegisteredError`; build one per process and share it. The keywords are
`registry:` (default `Prometheus::Client.registry`), `namespace:` (default
`"grpc"`, matching `/\A[a-z_][a-z0-9_]*\z/`), `duration_buckets:` and
`size_buckets:` (strictly increasing positive numbers). Exported names are
`<namespace>_<name>[_<unit>]`.

For the Async HTTP/2 client transport, add a connection observer with an
application-chosen transport name, unique per process:

```ruby
transport = ConnectRPC::Async::Transport.new(
  origin: "https://billing.internal:8443",
  connection_observer: metrics.connection_observer(transport: "billing")
)
```

Transport names match `/\A[a-z0-9_]{1,64}\z/`. Asking one `Metrics` for a second
observer with the same name raises `ArgumentError`.

Subscribers and observers run synchronously on the RPC's thread or reactor,
possibly from separate read and write fibers. prometheus-client's stores are
thread safe. A failing recorder call is reported through the instrumentation's
`on_error` and does not change the RPC. Async swallows connection observer
`StandardError`s, so connection metric failures are not reported through
`on_error`.

## Exposing metrics per host

Requiring `connectrpc/prometheus` does not mount an endpoint. Use
`Prometheus::Middleware::Exporter` (from `prometheus/middleware/exporter`) or
any other exporter over the same registry.

- **Rack** (`connectrpc-rack`): mount the exporter beside the RPC app.

  ```ruby
  require "prometheus/middleware/exporter"
  use Prometheus::Middleware::Exporter # serves /metrics
  run ConnectRPC::Rack.new(rails_or_other_app, server: connect_server)
  ```

- **Rails** (`connectrpc-on-rails`): mount the exporter middleware (or use
  yabeda-rails with yabeda-prometheus through the Yabeda package). Native
  controllers return the instrumentation from the private
  `connect_instrumentation` method, separately from `connect_limits`.
- **Falcon** (`connectrpc-falcon`): serve the exporter through the `rack_app`
  fallback. RPC traffic bypasses it; the fallback receives `/metrics`.
- **Async standalone `Host`**: since 0.2.2, pass a native Protocol::HTTP
  exporter as the Host's `fallback:`. RPC traffic bypasses it; it receives every
  other path.

  ```ruby
  require "prometheus/client/formats/text"

  registry = Prometheus::Client.registry
  exporter = lambda do |request|
    next Protocol::HTTP::Response[404, {}, nil] unless request.path == "/metrics"

    Protocol::HTTP::Response[200, { "content-type" => Prometheus::Client::Formats::Text::CONTENT_TYPE },
                             [Prometheus::Client::Formats::Text.marshal(registry)]]
  end
  host = ConnectRPC::Async::Host.new(server: connect_server, tls: tls, fallback: exporter).start
  ```

  The Host serves HTTP/2 only. Prometheus scrapes a TLS Host over h2 because
  its `enable_http2` scrape option defaults to true. A cleartext h2c Host is not
  scrapeable by HTTP/1.1 clients, including Prometheus, so those need a separate
  metrics listener on its own port (for example a small Rack server with the
  exporter) or Falcon, which serves HTTP/1.1 and HTTP/2 through `rack_app`.

## Multi-process

The default in-memory store is correct for one process with any number of
threads: Puma single mode, one Falcon worker, or an Async `Host`. Scrape each
pod or process on its own and sum in PromQL.

`DirectFileStore` is only for forked workers that share one `/metrics`
endpoint, where a scrape would otherwise see one random worker. It costs a file
write per observation, so do not use it otherwise.

- **Puma cluster / Unicorn**: set
  `Prometheus::Client.config.data_store = Prometheus::Client::DataStores::DirectFileStore.new(dir:)`
  before constructing `Metrics`, clear that directory in the master before
  forking, and serve `Prometheus::Middleware::Exporter`.
- **Falcon with several workers** (it forks one per CPU by default): use the
  same store and construct `Metrics` per worker after fork, consistent with the
  worker-local `prepare!`. Serve the exporter through the `rack_app` fallback.

Under `DirectFileStore`, the connection gauges are declared with
`aggregation: :sum`: each state of the state gauge sums to the number of
workers in it, and the connection and pending-call gauges sum the workers'
counts. The default store rejects store settings, so other stores get none.
Dead workers keep their last counter and gauge values until the directory is
cleared.

Build connection observers per worker after fork. An observer built in the Puma
or Unicorn master before fork, including one passed to a `ThreadedTransport`
created there, writes the master's `idle` 1 to the store, and the summed state
gauge keeps counting that entry.

## Metrics

Base labels `B` are `grpc_type`, `grpc_service`, `grpc_method` and
`rpc_protocol`. Names below use the default `grpc` namespace.

| Metric | Type | Labels | Recorded |
| --- | --- | --- | --- |
| `grpc_{server,client}_started_total` | counter | B | lifecycle start |
| `grpc_{server,client}_handled_total` | counter | B, `grpc_code` | lifecycle finish, see below |
| `grpc_{server,client}_handling_seconds` | histogram | B | with handled, processing duration |
| `grpc_{server,client}_msg_received_total`, `_msg_sent_total` | counter | B | per message, live |
| `grpc_{server,client}_msg_received_bytes`, `_msg_sent_bytes` | histogram | B | per message |
| `grpc_server_rejected_total` | counter | B, `reason` | lifecycle finish, see below |
| `grpc_server_delivery_aborted_total` | counter | B | handled and delivery aborted |
| `grpc_client_connection_attempts_total` | counter | `transport`, `outcome` | Async attempt finished |
| `grpc_client_connection_attempt_seconds` | histogram | `transport` | Async attempt finished |
| `grpc_client_connection_state_changes_total` | counter | `transport`, `state` | Async state change |
| `grpc_client_connection_state` | gauge | `transport`, `state` | 1 for the current state, starts `idle` |
| `grpc_client_admission_rejected_total` | counter | `transport` | Async admission rejection |
| `grpc_client_connections` | gauge | `transport`, `state` | current connections; `state` is `active` or `draining`, starts 0; since 0.2.2 |
| `grpc_client_pending_calls` | gauge | `transport` | current calls waiting for a connection, starts 0; since 0.2.2 |

Classification at finish, with `code` from the outcome's code or its normalized
error's code:

| Condition | Result |
| --- | --- |
| decision `admission_rejected` or `host_rejected` | `rejected_total{reason=<decision>}` |
| processed with a code | `handled_total{grpc_code=<Go name>}` |
| processed without a code but with an error (escaping exception, empty 429) | `handled_total{grpc_code="Unknown"}` |
| processed without an outcome, or a code-less outcome without error (native preflight, HTTP/1 505, Web preflight) | `rejected_total{reason="unprocessed"}` |
| client without a code | `handled_total{grpc_code="Unknown"}` |

In flight is `started - handled - rejected`: observed lifecycles not yet
finished, including delivery waits such as a held Rack body. It holds only when
no recorder call failed. There is no in-flight gauge and no per-stream message
histogram; average messages per stream is `msg_*_total / handled_total`.

- `grpc_type` is `unary`, `client_stream`, `server_stream` or `bidi_stream`.
  Service and method come only from the declared method. Undeclared paths use
  `grpc_service="_OTHER"`, `grpc_method="_OTHER"` and `grpc_type="unknown"`.
- `rpc_protocol` is `connect`, `grpc` or `grpc_web`.
- `grpc_code` uses Go `codes.Code` strings (`OK`, `Canceled`, `InvalidArgument`,
  ..., `Unauthenticated`); anything else is `Unknown`.
- There are no peer, address, path, metadata or error-text labels.
- Since 0.2.2, `grpc_client_connections{state="active"}` counts connections that
  accept new calls; `state="draining"` counts connections that no longer do
  (after a GOAWAY, or failed while calls unwind) until their last call ends.
  Their sum is the transport's open accounted connections.
  `grpc_client_pending_calls` counts calls waiting to acquire a connection;
  calls that find spare capacity never appear in it. Both return to 0 after the
  transport closes. There is no pending-bytes gauge. `HTTP1Transport` and server
  hosts publish no connection gauges.
- Message sizes are encoded payload bytes as framed: after compression,
  excluding the 5-byte envelope prefix, end-stream and trailer frames, gRPC-Web
  text base64 and GET query expansion. Receive counts after a successful decode.
  A send counts after local frame emission; it is not evidence of peer receipt.
  Discarded or undecodable messages are not counted.
- Durations use `[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]`
  seconds (Go's defaults; also connection attempts). Sizes use
  `[64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304]` bytes.

### Compatibility with go-grpc-prometheus

Names, help text, label names and code strings match go-grpc-middleware
`providers/prometheus`. `namespace:` swaps the `grpc` prefix. The compatibility
promise covers queries that aggregate by or without labels, the common
dashboard form. `rpc_protocol` adds a label, so recording rules that match exact
label sets need `sum without (rpc_protocol) (...)`. The byte histograms,
rejection, delivery and connection metrics are additions.

## Development

From the repository root use `mise run prometheus:setup`, `prometheus:test`,
`prometheus:check`, `prometheus:build`, `prometheus:package`,
`prometheus:example`, and `prometheus:verify`. The independent development
bundle resolves the newest allowed versions and includes the local Rack, Async, Falcon and testing packages
for real-host specs. Caches and archives are in `vendor/prometheus`, and coverage
is in `coverage/prometheus/<Ruby>/`. Both pinned Ruby 3.4.12 and 4.0.9 CI leaves
run `prometheus:verify`. `scripts/integration.rb` is also a runnable generated
example that checks the text exposition.

`floors:prometheus` repeats specs and package checks with every runtime
dependency at its gemspec floor, including prometheus-client 5.0.0. That floor
changes only Ruby support versus 4.2.5, whose `DirectFileStore` calls
`CGI.parse`, which Ruby 4.0 removed.

## Static signatures

This package owns `sig/` and its discovery manifest. Add
`library "connectrpc-prometheus"` to a strict Steep target with this gem
installed. The registry is typed through narrow structural capabilities. See the
[RBS guide](../../docs/rbs.md) for supported APIs and extension contracts.

## License

Original project work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg.
All package archives pin `allowed_push_host=https://rubygems.org`; these checks
never publish or release a gem.
