<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-yabeda

**Yabeda metrics for ConnectRPC clients and servers.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-yabeda?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-yabeda)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-yabeda` declares the same RPC, message, rejection and Async
connection metrics as [connectrpc-prometheus](../connectrpc-prometheus/README.md)
through [Yabeda](https://github.com/yabeda-rb/yabeda), so any Yabeda adapter
(Prometheus, Datadog, StatsD and others) receives them. Use it when your
application already records metrics through Yabeda.

The package is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

```ruby
gem "connectrpc-yabeda", "~> 0.2.0"
gem "yabeda-prometheus" # or another Yabeda adapter
```

The only runtime dependencies are `connectrpc ~> 0.2.0` and
`yabeda >= 0.16, < 1`. This package does not depend on `prometheus-client` or
any adapter; the application chooses and configures its adapters. Core and the
HTTP/framework packages do not depend on Yabeda.

## Usage

```ruby
require "connectrpc/yabeda"

metrics = ConnectRPC::Yabeda::Metrics.new(group: :grpc)
instrumentation = ConnectRPC::Instrumentation.new(
  subscribers: [metrics.subscriber], on_error: ->(error) { application_logger.warn(error.class.name) }
)
# Pass instrumentation: instrumentation to ConnectRPC::Server.new and ConnectRPC::Client.new.
```

`Metrics.new` declares its metrics through `Yabeda.configure`. Before
`Yabeda.configure!` the declaration is deferred, as for any Yabeda plugin;
after it, the metrics are declared immediately. Construct one `Metrics` per
group, typically in an initializer. Recording before `Yabeda.configure!` fails
inside the subscriber and is reported through the instrumentation's `on_error`
without changing the RPC; yabeda-rails and similar integrations call
`configure!` during boot.

`group:` (default `:grpc`, matching `/\A[a-z_][a-z0-9_]*\z/`) is the Yabeda group.
`duration_buckets:` and `size_buckets:` override the histogram buckets. For the
Async HTTP/2 client transport:

```ruby
transport = ConnectRPC::Async::Transport.new(
  origin: "https://billing.internal:8443",
  connection_observer: metrics.connection_observer(transport: "billing")
)
```

Transport names match `/\A[a-z0-9_]{1,64}\z/` and must be unique per process;
asking one `Metrics` for a repeated name raises `ArgumentError`. An observer
built after `Yabeda.configure!` publishes `idle` 1 and 0 for the other states,
and 0 for its connection and pending-call gauges (since 0.2.2), immediately. One
built before it registers a Yabeda collector that publishes its current state
and last known counts on each `Yabeda.collect!`, such as a yabeda-prometheus
scrape; until then its series appear only as events record them. Events recorded
before `configure!` still update the values that collector publishes. Async
swallows connection observer `StandardError`s, including recording before
`configure!`, so those failures are not reported through `on_error`. Callbacks
may run from separate threads and fibers; Yabeda stores values in concurrent
structures, and adapters own their own safety.

## Metrics and names

Each metric is declared with a group, name, optional unit, `comment`, `tags`
and, for histograms, `buckets`. Every gauge (connection state and, since 0.2.2,
`client_connections` and `client_pending_calls`) declares `aggregation: :sum`.
yabeda-prometheus exports `group_name_unit`, which produces exactly the
connectrpc-prometheus names, for example `grpc_server_handled_total`,
`grpc_server_handling_seconds` and `grpc_client_msg_sent_bytes`. Yabeda metric
keys omit the unit: `Yabeda.grpc_server_handling` is the latency histogram.

The [connectrpc-prometheus README](../connectrpc-prometheus/README.md#metrics)
is the reference for metrics, labels (tags), completion classification, message
size semantics, buckets and the `rpc_protocol` compatibility note. Both packages
record through the core `ConnectRPC::Metrics` layer, so values are identical.

## Prometheus export and multi-process

With yabeda-prometheus, follow its exporter setup (`Yabeda::Prometheus::Exporter`
or yabeda-rails). The connectrpc-prometheus
[multi-process guidance](../connectrpc-prometheus/README.md#multi-process)
applies: the default store is correct for one process with any number of
threads; use `DirectFileStore` only for forked workers that share one
`/metrics` endpoint. Under `DirectFileStore`, build connection observers per
worker after fork. An observer built in the Puma or Unicorn master before fork,
including one passed to a `ThreadedTransport` created there, writes the
master's `idle` 1 to the store, and the summed state gauge keeps counting that
entry.

yabeda-prometheus 0.9.1 pins `prometheus-client >= 3, < 5`. prometheus-client
4.x `DirectFileStore` calls `CGI.parse`, which Ruby 4.0 removed from the
default `cgi` library. On Ruby 4.0 with `DirectFileStore`, add the `cgi` gem to
the application's Gemfile. The default in-memory store does not need it.

## Development

From the repository root use `mise run yabeda:setup`, `yabeda:test`,
`yabeda:check`, `yabeda:build`, `yabeda:package`, `yabeda:example`, and
`yabeda:verify`. Specs record through the `yabeda/testing` adapter. The
development bundle resolves the newest allowed versions and deliberately
excludes yabeda-prometheus: its `prometheus-client < 5` is below the range
connectrpc-prometheus supports. Name equality is structural through the shared definitions and checked
against yabeda-prometheus's `group_name_unit` rule. Caches and archives are in
`vendor/yabeda`, and coverage is in `coverage/yabeda/<Ruby>/`. Both pinned Ruby
3.4.12 and 4.0.9 CI leaves run `yabeda:verify`.

`floors:yabeda` repeats specs and package checks with every runtime dependency
at its gemspec floor, including yabeda 0.16.0.

## Static signatures

This package owns `sig/` and its discovery manifest. Add `library "connectrpc"`
and `library "connectrpc-yabeda"` to a strict Steep target with this gem
installed. The manifest lists only standard libraries, so name each connectrpc
library explicitly; this also works with `rbs collection`. See the
[RBS guide](../../docs/rbs.md) for supported APIs and extension contracts.

## License

Original project work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg.
All package archives pin `allowed_push_host=https://rubygems.org`; these checks
never publish or release a gem.
