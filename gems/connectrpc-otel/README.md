# connectrpc-otel

Optional, experimental OpenTelemetry traces for Connect unary RPC. Runtime
requires only `connectrpc ~> 0.1.0` and `opentelemetry-api >= 1.11.1, < 2`.
Core and the HTTP/framework packages do not depend on OpenTelemetry.

```ruby
require "connectrpc/otel"

subscriber = ConnectRPC::OTel::Subscriber.new(
  tracer_provider: application_tracer_provider,
  request_metadata: ["x-request-id"], response_metadata: ["x-region"]
)
instrumentation = ConnectRPC::Instrumentation.new(
  subscribers: [subscriber], on_error: ->(error) { application_logger.warn(error.class.name) }
)
# Pass instrumentation: instrumentation to both ConnectRPC::Client.new and
# ConnectRPC::Server.new. Their other options and generated callers are unchanged.
```

The application owns its SDK, sampling, processors and exporters. Requiring
`connectrpc/otel` never loads or configures an SDK/exporter, replaces global
providers/propagators, or creates network clients. The default tracer provider is
the application's OpenTelemetry provider. An explicit W3C trace-context plus
baggage propagator is used by default, including when the global propagator is
no-op. Pass `propagator:` to select another application propagator. Span finish
invokes application-owned processors synchronously; a synchronous exporter can
block the calling thread/reactor. This package makes no nonblocking guarantee.

## Spans and privacy

The adapter emits one CLIENT or SERVER span for each configured RPC side.
Names for declared methods are `fully.qualified.Service/Method`.
`rpc.system.name` is `connectrpc`, `rpc.method` is the same declared name,
and `rpc.response.status_code` is `OK` for known success or the lower-case
Connect error actually observed. Bare preflight/admission responses have no
invented RPC code. Error rendering may replace an oversized error with
`resource_exhausted`; an empty 429 has no server wire code, while clients use
the existing HTTP fallback `unavailable`.

The baseline is [Connect semantic conventions v1.44.0](https://github.com/open-telemetry/semantic-conventions/blob/v1.44.0/docs/rpc/connect-rpc.md),
which are Development. CLIENT treats every non-OK Connect code as an error;
SERVER treats `unknown`, `deadline_exceeded`, `unimplemented`, `internal`,
`unavailable`, and `data_loss` as errors. Delivered `invalid_argument`, for
example, leaves SERVER status UNSET. Independent operation failures and host
aborts are errors. `error.type` contains bounded codes or local classifications;
success remains UNSET. There are no automatic exception events, descriptions,
stacks, payloads, raw URLs/queries, certificate data or arbitrary metadata.

Unknown methods use span name `connectrpc` and `rpc.method=_OTHER`. This adapter
redacts `rpc.method_original` by default. That attribute is conditionally
required by v1.44.0 for unknown methods, so this privacy default is a specific
conformance limitation. This is not a claim of full semantic-convention
compliance. Instrumentation scope `connectrpc-otel` identifies its package
version and has `connectrpc.semconv.version=1.44.0`. API 1.11.1/SDK 1.13.1 support
scope attributes, but no tracer schema URL; none is supplied.

Metadata capture requires explicit names, at most 32 per direction, each at most
128 bytes. Values retain repeated arrays, limited to eight printable ASCII
entries of 256 bytes each, under `connectrpc.request.metadata.<name>` and
`connectrpc.response.metadata.<name>`. No wildcards are accepted. Selecting
sensitive names is the application's responsibility. Baggage propagates but
is not captured as span attributes. Request injection is staged and validated
before merging into copied metadata; invalid injection is discarded and
reported without changing caller metadata or protocol headers.

## Processing and delivery

Core observation spans timeout/metadata validation, codecs, compression,
transport and response validation. Servers retain original failures for trusted
neutral subscribers separately from normalized/rendered errors. The OTel
adapter never serializes those exception objects.

Custom attributes distinguish `connectrpc.processing.state`,
`connectrpc.decision` (`processed`, `admission_rejected`, `host_rejected`) and
`connectrpc.delivery`:

- `unobserved`: direct core or Testing transport; no host-delivery evidence.
- `body_closed`: portable Rack body close only.
- `response_finished`: Rack's response-finished callback ran. It does not prove
  completion of other callbacks or peer receipt. This extension takes precedence
  over body close.
- `completed`: native Async/Falcon full-write and cleanup boundary, including
  Falcon HTTP/1 body callbacks. This does not promise peer/application receipt.
- `aborted`: cancellation, reset, write failure or forced teardown.

`connectrpc.processing.duration` is monotonic processing seconds. Measured
encoded sizes appear as `connectrpc.request.wire_bytes` and
`connectrpc.response.wire_bytes`; unknown sizes are omitted. Successful
processing followed by delivery abort retains `OK` and ends ERROR.

Configure native hosts through their Server. Lifecycle handles are passed
explicitly into independent operation/setup/handler tasks. Shared connection
readers never retain one RPC's context. Connections/setup rejected before any
RPC exists are outside per-RPC observation. Rack consumers must close the body,
or invoke the promised response-finished callbacks; a noncompliant consumer can
leave a span unfinished. For native Rails controllers, override private
`connect_instrumentation` separately from `connect_limits`. Only failures
escaping Rails execution are captured; a successful `rescue_from` does not
mark a successful RPC as failed. Configuration failures before Server
construction remain outside its lifecycle.

## HTTP instrumentation coexistence

The SDK-backed suite tests Rack instrumentation 0.31.2 (stable middleware,
`use_rack_events: false`) and NetHTTP instrumentation 0.29.2, with the application
installing a W3C global propagator. Native Rails 8.1.4 is tested through its real
controller pipeline. Generic HTTP tracing remains enabled. The observed tree is:
application -> RPC CLIENT -> HTTP CLIENT -> RPC SERVER and HTTP SERVER as
siblings. NetHTTP injects its HTTP CLIENT; both server integrations extract that
serialized parent. The RPC adapter deliberately does not inspect ambient HTTP
spans to guess a parent. In-process Testing transport also extracts serialized
headers, even while the caller's RPC context is ambient. Other instrumentation
combinations need their own runtime verification.

## Development

From the repository root use `mise run otel:setup`, `otel:test`, `otel:check`,
`otel:build`, `otel:package`, `otel:example`, and `otel:verify`.
The independent checksummed development bundle includes the real SDK,
InMemorySpanExporter, HTTP instrumentations and local integration packages.
Existing runtime closures and locks remain independent. Caches/archives are in
`vendor/otel`, and coverage is in `coverage/otel/<Ruby>/`. Both pinned Ruby
3.4.12 and 4.0.9 CI leaves run `otel:verify`. `scripts/integration.rb` is also a
runnable generated-client example with a real in-memory SDK exporter.

The new lock's API/SDK were released 2026-09-16. New dependencies, including
native variants, were checked against primary RubyGems release timestamps and
API SHA256 values before adoption, under the seven-day cooldown. The versioned
lock preserves the eligible existing cross-package graph. Original project
work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg. All package
archives retain `allowed_push_host=https://gems.invalid`; nothing here
publishes or releases a gem.

## Static signatures

This package owns `sig/` and its discovery manifest. Add
`library "connectrpc-otel"` to a strict Steep target with this gem installed.
See the [RBS guide](../../docs/rbs.md) for supported APIs, extension contracts,
partial records, runtime validation and framework-signature limits.
`mise run rbs:check` checks application callers for this package; strict core
source checking is a separate target from backend implementation checking.
