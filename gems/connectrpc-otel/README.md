<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-otel

**OpenTelemetry tracing for ConnectRPC clients and servers.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-otel?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-otel)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-otel` turns the core ConnectRPC per-call lifecycle into
OpenTelemetry CLIENT and SERVER spans for Connect unary/streaming, native gRPC
and server gRPC-Web RPCs. Use it when your application already configures an
OpenTelemetry SDK and you want one span per RPC, linked across services by the
application's propagator.

The package is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

```ruby
gem "connectrpc-otel", "~> 0.2.0"
```

The only runtime dependencies are `connectrpc ~> 0.2.0` and
`opentelemetry-api >= 1.11.1, < 2`. Core and the HTTP/framework packages do not
depend on OpenTelemetry. Install and configure the OpenTelemetry SDK and
exporters yourself.

## Usage

Create a subscriber and pass the resulting instrumentation to both the client
and the server:

```ruby
require "connectrpc/otel"

subscriber = ConnectRPC::OTel::Subscriber.new(
  tracer_provider: application_tracer_provider,
  request_metadata: ["x-request-id"], response_metadata: ["x-region"],
  response_trailers: ["x-cache"]
)
instrumentation = ConnectRPC::Instrumentation.new(
  subscribers: [subscriber], on_error: ->(error) { application_logger.warn(error.class.name) }
)
# Pass instrumentation: instrumentation to both ConnectRPC::Client.new and
# ConnectRPC::Server.new. Their other options and generated callers are unchanged.
```

Every `Subscriber.new` keyword is optional. `tracer_provider:` defaults to
`OpenTelemetry.tracer_provider`, `propagator:` to the application's global
propagator described below, and `request_metadata:` (request headers),
`response_metadata:` (response headers) and, since 0.3.0,
`response_trailers:` (response trailers) to capturing nothing. For native Rails controllers, override the private
`connect_instrumentation` hook instead; see [host notes](#host-notes).

`mise run otel:example` runs a generated caller with a real in-memory SDK
exporter from the repository root.

### What the application owns

The application owns its SDK, sampling, processors and exporters. Requiring
`connectrpc/otel` never loads or configures an SDK/exporter, replaces global
providers/propagators, or creates network clients.

- The default tracer provider is the application's OpenTelemetry provider.
- Since 0.3.0, the default propagator is the global `OpenTelemetry.propagation`,
  looked up at each use, so SDK configuration made after the subscriber is
  built applies, including `OTEL_PROPAGATORS` (for example `b3`). Without SDK
  configuration the global propagator is no-op and nothing propagates, as with
  other OpenTelemetry instrumentation. Pass `propagator:` to use a specific
  propagator instead.
- Span finish invokes application-owned processors synchronously; a synchronous
  exporter can block the calling thread/reactor. This package makes no
  nonblocking guarantee.

## Spans and privacy

The adapter emits one CLIENT or SERVER span for each configured RPC side.
Names for declared methods are `fully.qualified.Service/Method`.
`rpc.system.name` is `connectrpc`, `rpc.method` is the same declared name,
and `rpc.status_code` is `OK` for known success or the lower-case
Connect error actually observed. Bare preflight/admission responses have no
invented RPC code. Error rendering may replace an oversized error with
`resource_exhausted`; an empty 429 has no server wire code, while clients use
the existing HTTP fallback `unavailable`.

CLIENT spans carry `server.address` and `server.port` from the client's base
URL. Since 0.3.0, SERVER spans carry `server.address` from the request's
`Host` or HTTP/2 `:authority`, the address the client used rather than the
listening socket, and `server.port` only when that authority states a port.
Rack and Rails pass `Host`; the native Async and Falcon hosts pass the
authority, which wins over an ordinary `host` field. A missing, repeated,
over-255-byte or malformed value omits both. Like HTTP server conventions,
the value is client-controlled, so its cardinality is too.

### Semantic conventions and error status

Since 0.3.0, the baseline is
[Connect semantic conventions v1.45.0](https://github.com/open-telemetry/semantic-conventions/blob/v1.45.0/docs/rpc/connect-rpc.md)
and, for gRPC and gRPC-Web,
[gRPC semantic conventions v1.45.0](https://github.com/open-telemetry/semantic-conventions/blob/v1.45.0/docs/rpc/grpc.md).
The adapter emits these names directly, with no older names alongside them
and no `OTEL_SEMCONV_STABILITY_OPT_IN` mode; the
[upgrade guide](../../docs/upgrading.md#breaking-change-opentelemetry-semantic-conventions-1450)
maps the names that 0.2 emitted. Version 1.45.0 keeps the role policy below:
it lists the same SERVER error codes and treats every non-OK CLIENT code as
an error, and lets instrumentation use additional context, as the delivery
rules below do.

- CLIENT treats every non-OK Connect code as an error.
- SERVER treats `unknown`, `deadline_exceeded`, `unimplemented`, `internal`,
  `unavailable`, and `data_loss` as errors. Delivered `invalid_argument`, for
  example, leaves SERVER status UNSET.
- Independent operation failures are errors.
- An aborted delivery after `OK` or without an RPC code is an error. With a
  non-OK code, the role policy above decides: a client cancellation aborts
  server delivery but leaves the SERVER span `canceled` and UNSET, while the
  CLIENT span reports the code as an error.
- `error.type` contains a bounded code, an exception class name or a local
  classification; success remains UNSET. Codes are the RPC codes above and take
  precedence. Otherwise an original non-Connect exception reports its class
  name, such as `RuntimeError`. Local classifications are `operation_failed` (a
  failure without an emitted code whose exception is a `ConnectRPC::Error` or
  has an anonymous class), `preflight_rejected` (a bare HTTP rejection with no
  RPC code), `delivery_aborted` (an aborted delivery after `OK` or without an
  RPC code) and the `admission_rejected` and `host_rejected` decisions. Local
  classifications are lower-case on every protocol.

There are no automatic exception events, descriptions, stacks, payloads, raw
URLs/queries, certificate data or arbitrary metadata.

### Unknown methods and conformance limits

Unknown methods use span name `connectrpc` and `rpc.method=_OTHER`. This adapter
never records `rpc.method_original`, because the core call carries no request
path. That attribute is conditionally required by v1.45.0 for unknown methods,
so this is a specific conformance limitation. The adapter also records no
`network.peer.*` attributes. gRPC lists `rpc.status_code` as Required, but a
span without a core RPC code, such as a preflight or admission rejection or an
early escape, omits it rather than inventing one. `http.response.status_code`
is supplemental transport evidence outside the RPC attribute tables. This is
not a claim of full semantic-convention compliance. Instrumentation scope
`connectrpc-otel` identifies its package version and has
`connectrpc.semconv.version=1.45.0`. API 1.11.1/SDK 1.13.1 support scope
attributes, but no tracer schema URL; none is supplied.

### Metadata capture

Metadata capture requires explicit names, at most 32 per list, each at most
128 bytes, and never a reserved name such as `content-type`, `grpc-*` or
`trailer-*`. Every protocol records request headers as
`rpc.request.header.<name>`, response headers as `rpc.response.header.<name>`
and, since 0.3.0, response trailers as `rpc.response.trailer.<name>`.
Response headers are the metadata sent before the response payload; trailers
are the trailing metadata sent after it, named without the Connect unary
`trailer-` wire prefix. Values retain repeated arrays, limited to eight
printable ASCII entries of 256 bytes each. A name ending in `-bin` holds
binary metadata, which core keeps as its base64 wire text: each value,
including each comma-joined part, is decoded and recorded as standard padded
base64, and a malformed or over-256-byte value is omitted rather than
truncated. No wildcards are accepted. Selecting
sensitive names is the application's responsibility. Baggage propagates when
the propagator carries it, as the SDK default does, but is never captured as
span attributes. Request injection is staged and validated before merging
into copied metadata; invalid injection is discarded and reported without
changing caller metadata or protocol headers. Injected keys replace caller
values, except `baggage`: the wire carries the caller's `baggage` values first,
then the injected ones. When an earlier subscriber has staged `baggage`, that
baggage stays first, each list member exactly once, followed by the members the
propagator added, whether the propagator replaces, joins, edits, removes or
leaves the value alone. Core keeps duplicate members and does not validate W3C
syntax; see
[design](../../docs/design.md#rpc-lifecycle-observation) for receiver limits.

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
`connectrpc.response.wire_bytes`; unknown sizes are omitted. For GET, request
wire bytes measure the query transport payload after percent/base64 decoding
and before inflation, independently of the empty HTTP body. Zero means measured
empty; rejected requests before measurement remain unknown. RPC spans never
capture request URLs, query controls or payloads. Generic HTTP instrumentation
and host/proxy logs have their own application-owned redaction policies.
Successful processing followed by delivery abort retains `OK` and ends ERROR.

### Host notes

Configure native hosts through their Server. Lifecycle handles are passed
explicitly into independent operation/setup/handler tasks. Shared connection
readers never retain one RPC's context. Connections/setup rejected before any
RPC exists are outside per-RPC observation.

- Rack consumers must close the body, or invoke the promised response-finished
  callbacks; a noncompliant consumer can leave a span unfinished.
- For native Rails controllers, override private `connect_instrumentation`
  separately from `connect_limits`. Only failures escaping Rails execution are
  captured; a successful `rescue_from` does not mark a successful RPC as failed.
- Configuration failures before Server construction remain outside its
  lifecycle.

## Native streaming

Streaming keeps one span and fixed lifecycle state per RPC, with no per-message
events or payload capture. The original processor retains SDK context and peer
identity through codecs and cleanup; publication, processing and full native
wire completion remain independent. Late application cleanup failure aborts
delivery. Falcon H1 records its final-flush expiry before body callbacks and
lifecycle observation; delayed callbacks consume that same 500 ms budget.
Post-delivery discard or grace failure preserves an already completed span,
while socket/admission ownership continues through retirement and processing join.

The core [streaming guide](../../docs/streaming.md) documents all three scoped
Connect streaming shapes, typed generated clients/handlers, early headers,
per-message limits and bounded ownership. Native Async/Falcon HTTP/2 supports
both codecs and identity/gzip over h2c/TLS/mTLS. Falcon additionally serves
Connect server and client streaming over HTTP/1.1, with protobuf/ProtoJSON and
identity/gzip over TCP/TLS/mTLS. The standalone Async Host and original Async
Transport remain H2-only. The explicit `ConnectRPC::Async::HTTP1Transport`
supports Connect unary, safe GET, server streaming and client streaming in an
application-owned reactor.

The streaming guide defines Falcon's cooperative 500 ms total final-close budget
and unchanged 100 ms / 256 KiB discard limits. Native Connect streaming clients
have a separate, provisional one-second absolute HTTP tail policy. It applies even with a nil
RPC deadline and can reject otherwise valid late EOF or slow intermediaries.

## Native gRPC

The actual protocol is fixed before subscriber start. gRPC uses the 1.45.0
conventions: `rpc.system.name=grpc`, uppercase `rpc.status_code` and
code-based `error.type`, including `CANCELLED`. Only status codes are
uppercased; local `error.type` classifications stay lower-case. Client and
Server retain the role-specific error policy above. Allowlisted gRPC metadata
uses the same `rpc.request.header.<key>`, `rpc.response.header.<key>` and
`rpc.response.trailer.<key>` names as Connect, whose status codes stay
lower-case. Unknown gRPC methods use
span name `grpc` and `rpc.method=_OTHER`, with the same explicit privacy limit.
Processing and delivery remain independent; successful handler processing cannot
prove terminal HEADERS delivery. SDK configuration remains application-owned.

## gRPC-Web

Web calls have `Call.protocol=:grpc_web` before subscriber start. The project
maps them to gRPC semconv 1.45.0 with `rpc.system.name=grpc` and bounded
`connectrpc.protocol=grpc_web`. Status codes, metadata prefixes, error roles and
original-task activation follow native gRPC. See the
[Web guide](../../docs/grpc-web.md) for protocol, CORS, bounds and completion
rules.

## HTTP instrumentation coexistence

The SDK-backed suite tests the newest allowed Rack instrumentation (stable
middleware, `use_rack_events: false`) and NetHTTP instrumentation, with the
application installing a W3C global propagator. Native Rails 8.1 is tested
through its real controller pipeline. Generic HTTP tracing remains enabled. The observed tree is:
application -> RPC CLIENT -> HTTP CLIENT -> RPC SERVER and HTTP SERVER as
siblings. NetHTTP injects its HTTP CLIENT; both server integrations extract that
serialized parent. The RPC adapter deliberately does not inspect ambient HTTP
spans to guess a parent. In-process Testing transport also extracts serialized
headers, even while the caller's RPC context is ambient. Other instrumentation
combinations need their own runtime verification.

## Static signatures

This package owns `sig/` and its discovery manifest. Add `library "connectrpc"`
and `library "connectrpc-otel"` to a strict Steep target with this gem
installed. The manifest lists only standard libraries, so name each connectrpc
library explicitly; this also works with `rbs collection`. See the
[RBS guide](../../docs/rbs.md) for supported APIs, extension contracts, partial
records, runtime validation and framework-signature limits. `mise run rbs:check`
checks application callers for this package; strict core source checking is a
separate target from backend implementation checking.

## Development

From the repository root use `mise run otel:setup`, `otel:test`, `otel:check`,
`otel:build`, `otel:package`, `otel:example`, and `otel:verify`.
The independent development bundle includes the real SDK,
InMemorySpanExporter, HTTP instrumentations and local integration packages.
The SDK and instrumentations float to their newest allowed releases; only
tools stay pinned. Existing runtime closures remain independent. Caches/archives are in
`vendor/otel`, and coverage is in `coverage/otel/<Ruby>/`. Both pinned Ruby
3.4.12 and 4.0.9 CI leaves run `otel:verify`. `scripts/integration.rb` is also a
runnable generated-client example with a real in-memory SDK exporter.

`floors:otel` repeats specs and package checks with the API and every other
local package's runtime dependency at its gemspec floor. That lane alone
constrains `json` to `< 3`, because the Action Pack floor's ActiveSupport passes
a JSON option that json 3.x rejects.

## License

Original project work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg.
All package archives pin `allowed_push_host=https://rubygems.org`; these checks
never publish or release a gem.
