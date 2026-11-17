# Metrics: connectrpc-prometheus and connectrpc-yabeda

Status: implemented in [#66](https://github.com/jimeh/connectrpc-ruby/pull/66),
with the connection gauges and Host fallback follow-up in
[#100](https://github.com/jimeh/connectrpc-ruby/pull/100); see
[Follow-up (#70)](#follow-up-70). The contract below is kept as approved.
Base: `fc80614`. Branch `feat/metrics-prometheus-yabeda`, PR title
`feat: add Prometheus and Yabeda metrics packages`. The draft had a
cross-engine plan review; accepted findings were checked against the code.

## Objective

Apps get standard RPC metrics for ConnectRPC servers and clients (counts by
code, latency, message counts and sizes, rejections, Async client connection
health) in a `prometheus-client` registry or through Yabeda. Core takes no
metrics backend dependency. Dashboards built on go-grpc-prometheus queries
keep working.

## Verified facts

- go-grpc-middleware `providers/prometheus` (pin the commit in the spec):
  `grpc_{server,client}_started_total`, `_handled_total` (+`grpc_code`),
  `_msg_received_total`, `_msg_sent_total`, opt-in
  `_handling_seconds` histogram with `DefBuckets`, no code label. Labels
  `grpc_type` (`unary`, `client_stream`, `server_stream`, `bidi_stream`),
  `grpc_service`, `grpc_method`. Codes are Go `codes.Code.String()`: `OK`,
  `Canceled`, `InvalidArgument`, ..., `Unauthenticated`.
- `prometheus-client` 5.0.0 changes only Ruby support versus 4.2.5. 4.x
  `DirectFileStore` calls `CGI.parse`, which is gone on Ruby 4.0 (checked:
  `require "cgi"` loads, `CGI.parse` is undefined on 4.0.9). Gauges in
  `DirectFileStore` need an `aggregation`; the default store rejects settings.
- `yabeda` 0.16.0: `Yabeda.configure` works before and after `configure!`;
  metrics are group + name (+unit) with `tags:`, `buckets:`, `aggregation:`.
  `yabeda-prometheus` 0.9.1 (Jan 2024) pins `prometheus-client >= 3, < 5` and
  exports `group_name_unit`.
- Core lifecycle: `Call` carries role, protocol, method (nil only when the
  server finds no declaration) and client `server_address`/`server_port`
  (excluded from metrics). `Outcome#request_bytes`/`response_bytes` exist for
  Connect unary only. gRPC and streams have no byte or message data.
  `Completion` carries `decision`, `processing_state`, `delivery`.
- Async `ConnectionEvent` only fires on state change, attempt finish and
  admission rejection. Pending bytes, waiter removal and connection-count
  changes inside `ready` publish nothing (`connection_manager.rb:74-100,327`).
- `Async::Host` builds `Application.new(server:)` internally with no
  `fallback:` (`host.rb:23`). Falcon has `rack_app:`.

## Decision 1: shared layer location (settled: A)

| Option | For | Against |
| --- | --- | --- |
| **A. Core optional require `connectrpc/metrics`** (recommended) | No third gem or dependency. Sinks stay thin. A later OTel-metrics sink in `connectrpc-otel` and custom StatsD sinks reuse it. | Core public surface, RBS, inventory and every core package manifest grow. Metric names become a core contract. |
| B. In `connectrpc-prometheus`, yabeda depends on it | One place. | Forces `prometheus-client` on Datadog/StatsD Yabeda apps and conflicts with `yabeda-prometheus` (`< 5`). Rejected. |
| C. Third gem `connectrpc-metrics` | Core keeps lifecycle-only API; clean ownership. | Third gem, publisher, CI job and bundle. |

Async event shapes enter core only as a structural RBS interface, so core
signatures never reference Async.

## Decision 2: core message events (settled: per-message callbacks)

Recommended: optional observer callbacks `message_received(bytes)` and
`message_sent(bytes)`, discovered once through private `Callbacks`, each
isolated by `safely`. Alternative: final counts on `Outcome` (no live counts
for long streams, sizes stay Connect-unary-only).

- `bytes` = encoded message payload bytes as framed: after compression,
  excluding the 5-byte prefix, end-stream/trailer frames, gRPC-Web text base64
  and GET percent/base64 expansion (matching today's GET `request_bytes`).
  `Framing#read_frame` returns the envelope length beside the payload;
  `Framing#emit` returns the written payload length.
- Receive fires after successful decode and the following cancellation check.
  Send fires after successful frame emission. Recovery/discard reads fire
  nothing. Connect unary: server receive after request decode, send after
  response compression and limit validation; client send after compression
  and GET construction, receive after decode. Docs state that a send event
  means local acceptance, not peer receipt.
- Callbacks run on the session's owning thread/reactor, possibly from separate
  read and write fibers. Recorders must be thread and fiber safe.

## Approach and scope

### Core

- `instrumentation.rb`: callbacks on `Callbacks`/`Handle`; public capability
  interfaces in `sig/connectrpc.rbs`; implementation records in
  `typecheck/internal/core.rbs`.
- Emit points: `streaming/framing.rb`, `streaming/session_messages.rb`,
  `server.rb`, `client.rb`. gRPC unary (both roles) and gRPC-Web go through the
  session; supported combinations are client Connect/gRPC and server
  Connect/gRPC/gRPC-Web (Web has no compressed envelopes).
- `lib/connectrpc/metrics.rb` + `metrics/` (not loaded by `require "connectrpc"`):
  - `Metrics::Instrument = Data.define(:key, :kind, :name, :unit, :labels, :buckets, :help)`.
  - `Metrics::Definitions.new(namespace: "grpc", duration_buckets:, size_buckets:)`;
    validates namespace `/\A[a-z_][a-z0-9_]*\z/` and strictly increasing
    positive buckets. Keywords default to the constants below; no `nil`
    meaning, no histogram disabling in this PR.
  - `Metrics::Subscriber.new(recorder:, definitions:)` (Instrumentation subscriber).
  - `Metrics::ConnectionObserver.new(recorder:, definitions:, transport:)` (callable).
    It records nothing at construction; sinks call `#publish_state` once their
    metrics exist (Prometheus immediately, Yabeda after `configure!` or on collect).
  - Recorder SPI: `increment(key, labels, by)`, `observe(key, labels, value)`,
    `set(key, labels, value)`.

### `connectrpc-prometheus`

```ruby
require "connectrpc/prometheus"
metrics = ConnectRPC::Prometheus::Metrics.new(registry: Prometheus::Client.registry, namespace: "grpc")
instrumentation = ConnectRPC::Instrumentation.new(subscribers: [metrics.subscriber])
transport = ConnectRPC::Async::Transport.new(..., connection_observer: metrics.connection_observer(transport: "billing"))
```

Registers all instruments once; constructing twice on one registry raises the
registry's error. Name `namespace_name[_unit]`. Gauges get
`aggregation: :sum` only on `DirectFileStore`. A repeated `transport:` name on
one `Metrics` raises. Deps: `connectrpc ~> 0.1.0`, `prometheus-client >= 5.0, < 6`.

### `connectrpc-yabeda`

```ruby
require "connectrpc/yabeda"
metrics = ConnectRPC::Yabeda::Metrics.new(group: :grpc)
instrumentation = ConnectRPC::Instrumentation.new(subscribers: [metrics.subscriber])
```

Declares through `Yabeda.configure`; group, name and unit come from the same
`Definitions`, so yabeda-prometheus exports the same names. Gauges declare
`aggregation: :sum`. Deps: `connectrpc ~> 0.1.0`, `yabeda >= 0.16, < 1`.

## Metric set

Base labels `B` = `grpc_type`, `grpc_service`, `grpc_method`, `rpc_protocol`.

### Server and client RPCs (prefix `grpc_server_` / `grpc_client_`)

| Metric | Type | Labels | Recorded |
| --- | --- | --- | --- |
| `started_total` | counter | B | subscriber `start` |
| `handled_total` | counter | B + `grpc_code` | `finished`, per the table below |
| `handling_seconds` | histogram | B | with handled, `Outcome#duration` |
| `msg_received_total`, `msg_sent_total` | counter | B | per message, live |
| `msg_received_bytes`, `msg_sent_bytes` | histogram | B | per message |
| `rejected_total` (server only) | counter | B + `reason` | `finished`, per the table below |
| `delivery_aborted_total` (server only) | counter | B | handled and `delivery: :aborted` |

Completion classification (`code = outcome&.code || outcome&.normalized_error&.code`):

| Condition | Result |
| --- | --- |
| `decision` is `admission_rejected` or `host_rejected` | `rejected_total{reason=<decision>}` |
| processed, `code` present | `handled_total{grpc_code=map(code)}` |
| processed, no code, outcome has an error (escaping exception, empty 429) | `handled_total{grpc_code="Unknown"}` |
| processed, no outcome or code-less outcome without error (native preflight, H1 505, Web preflight) | `rejected_total{reason="unprocessed"}` |
| client, no code | `handled_total{grpc_code="Unknown"}` |

In flight = `started - handled - rejected`, meaning observed lifecycles not yet
finished, including delivery wait (a held Rack body counts). This holds only
when no recorder call failed; `safely` makes each callback independent, so a
failing recorder can break it. No in-flight gauge and no per-stream message
histogram; average messages per stream is `msg_*_total / handled_total`.

### Async client connections (`connection_observer`)

Label `transport`: app-chosen, `/\A[a-z0-9_]{1,64}\z/`, unique per process.

| Metric | Type | Labels |
| --- | --- | --- |
| `grpc_client_connection_attempts_total` | counter | `transport`, `outcome` |
| `grpc_client_connection_attempt_seconds` | histogram | `transport` |
| `grpc_client_connection_state_changes_total` | counter | `transport`, `state` |
| `grpc_client_connection_state` | gauge, 1 for current state (`idle` until the first change, published by `#publish_state`) | `transport`, `state` |
| `grpc_client_admission_rejected_total` | counter | `transport` |

Under `DirectFileStore` the state gauge sums to the number of workers in each
state. Active/draining/pending gauges are left out because events do not publish
those counts on every change. The [follow-up](#follow-up-70) added them with a
`:counts_changed` event. Async swallows observer `StandardError`s, so connection
recorder failures are not reported through `on_error`; documented.

## Labels and naming

- Service and method only from the declared method (all four declaration
  types). No declaration: `_OTHER`/`_OTHER`, `grpc_type="unknown"`.
- 17 Connect codes map to Go strings; anything else is `Unknown`.
- No peer, address, path, metadata or error-text labels.
- go-grpc-prometheus names by default; `namespace:`/`group:` swaps the prefix.
  Compatibility promise: queries that aggregate by or without labels (the
  common dashboard form). `rpc_protocol` adds a label, so recording rules that
  match exact label sets need `sum without (rpc_protocol)`.

## Buckets

Durations `[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]` (Go
default; also connection attempts). Sizes `[64, 256, 1024, 4096, 16384, 65536,
262144, 1048576, 4194304]`. Both overridable.

## Multi-process and exposition (README content)

- Store choice: the default in-memory store is correct for one process with
  any number of threads (Puma single mode, one Falcon worker, Async `Host`).
  Each pod or process is scraped on its own and summed in PromQL.
  `DirectFileStore` is only for forked workers that share one `/metrics`
  endpoint, where a scrape would otherwise see one random worker. It costs a
  file write per observation, so docs do not recommend it otherwise.
- Puma cluster / Unicorn: `DirectFileStore` set before constructing `Metrics`,
  directory cleared in the master before fork, `Prometheus::Middleware::Exporter`.
- Falcon with several workers (it forks one per CPU by default): same store;
  construct per worker after fork, consistent with the worker-local
  `prepare!`; exporter in the `rack_app:` fallback.
- Async standalone `Host`: a separate metrics listener (own port), since Host
  has no fallback. The [follow-up](#follow-up-70) later added `fallback:`.
- Rack: exporter middleware beside the app. Rails: exporter middleware or
  yabeda-prometheus/yabeda-rails; instrumentation via private
  `connect_instrumentation`.
- Yabeda + yabeda-prometheus with `DirectFileStore` on Ruby 4.0 needs the `cgi`
  gem (upstream pins `prometheus-client < 5`).
- Dead workers keep their last counter and gauge values until the directory is
  cleared.

## Packaging, typecheck and release train

- Each gem mirrors `connectrpc-otel`: gemspec, version file, `lib/`, `sig/` +
  manifest, specs, scripts, LICENSE, README, own bundle, `vendor/<name>`,
  `coverage/<name>`, mise `prometheus:*`/`yabeda:*` tasks, CI jobs on both Ruby
  lanes, Rakefile rubocop globs.
- Core manifest owners to update with exact entries: `scripts/check_package.rb`,
  `scripts/check_rails_package.rb`, and the rack/async/testing/otel
  `scripts/package.rb`.
- Typecheck: `typecheck/source.rb` `FILES`, `typecheck/Steepfile`,
  `typecheck/internal/core.rbs`, `typecheck/surface.json`, `typecheck/api_check.rb`
  `PACKAGES`, `typecheck/installed_runtime.rb`, `rbs:package` prerequisites,
  positive and negative caller fixtures.
- Release: `scripts/release_train.rb` (`NAMES`, guides, lock map; OTel's lock
  entry becomes an explicit seven-package list so its graph stays unchanged),
  `release:candidate` prerequisites, `release_publisher` order and specs,
  `release_train_spec` version smoke, OTel `package.rb` `NAMES` consumer,
  `.github/release-please-config.json`, `docs/{support,rbs,development,releasing}.md`,
  README package table, AGENTS.md map.
- The yabeda dev bundle does not include `yabeda-prometheus`: its
  `prometheus-client < 5` would break the installed-API rule that every lock
  shares one version map (`typecheck/installed_api.rb:23`). Name equality is
  structural through shared `Definitions` and asserted with a spec that
  reproduces yabeda-prometheus's `group_name_unit` rule.
- The lockfiles/constraints PR (not pushed yet) will change bundles, cooldown,
  CI and release machinery, and may remove the version-map constraint above.
  Implementation follows `main` at start and rebases onto it. READMEs follow
  `docs/readme-revision` once merged.
- Jim creates two RubyGems pending trusted publishers (`connectrpc-prometheus`,
  `connectrpc-yabeda`; `release.yml`, environment `release`) within 12 hours
  before the release that first ships them.

## Non-goals

OTel metrics; Async connection-count/pending gauges; Async `Host` fallback;
go-grpc per-message latency histograms; per-stream message histograms;
in-flight gauges; custom labels (a later `labels:` hook); exemplars;
pushgateway; HTTP-level metrics.

## Verification

| Concern | Evidence |
| --- | --- |
| Message events: Connect unary POST/GET, Connect stream shapes, gRPC unary and stream (client and server), gRPC-Web text and binary (server) | Core specs with a recording subscriber; counts and payload sizes, gzip on Connect and gRPC |
| Events fire live, not at stream end | Spec asserts a server-stream receive event before the stream finishes |
| No event on decode/codec failure, recovery discard, failed emit | Focused core specs |
| Raising or absent callbacks leave RPCs and OTel unchanged | Raising-observer spec; `otel:verify` |
| Classification table, `_OTHER`, code map, invariant | Neutral-layer specs with a fake recorder through real core server/client |
| Host completion: held Rack body, Async H2 admission rejection, Falcon H1 host rejection, native gRPC preflight, delivery abort | Specs in the prometheus bundle (has local Rack, Async and Falcon packages, like otel) |
| go-grpc-prometheus names/labels/codes | Text exposition golden spec, upstream commit cited |
| `DirectFileStore` across forked processes, gauge `:sum` | Fork spec in a tmpdir |
| Connection metrics | Real Async transport: refused ephemeral port, ready local host, admission rejection |
| Yabeda declare before and after `configure!`, values | `yabeda/testing` adapter specs |
| Packaging, RBS, release train | `prometheus:verify`, `yabeda:verify`, `rbs:check`, `rbs:package`, `release:check`, canonical nine-archive `release:candidate`, `mise run verify`, `rack:verify`, `actionlint` |

Residual: Rails metrics rely on Rack-path coverage plus docs; no Rails spec.

## Settled questions

1. Shared layer: core optional require `connectrpc/metrics`.
2. Message events: per-message callbacks.
3. `rpc_protocol` always on, with the narrowed compatibility promise.
4. `prometheus-client >= 5.0, < 6`.
5. Async standalone Host: documented separate metrics listener; no Host API change.

## Follow-up (#70)

Implemented after this plan, superseding the non-goals and settled question 5
for these two items:

- Async client connection gauges: the Async connection manager publishes a
  `:counts_changed` event when `active`, `draining` or `pending_calls` changes
  without another event, and the shared `ConnectionObserver` records
  `<namespace>_client_connections{transport,state}` (`state` is `active` or
  `draining`) and `<namespace>_client_pending_calls{transport}` from the counts
  on every event. `pending_bytes` changes do not publish and have no gauge.
  Scope is `Transport` and `ThreadedTransport`; servers and `HTTP1Transport`
  publish none.
- Async standalone Host fallback: `Host.new(server:, fallback:)` routes every
  non-RPC request to a native Protocol::HTTP callable, guarded against CONNECT
  and protocol switches. The Host stays HTTP/2 only, so cleartext h2c Hosts
  still need a separate metrics listener for HTTP/1.1 scrapers.
