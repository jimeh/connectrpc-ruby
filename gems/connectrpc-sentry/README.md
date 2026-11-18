<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-sentry

**Native Sentry tracing and handler exception capture for ConnectRPC.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-sentry?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-sentry)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-sentry` turns the core ConnectRPC per-call lifecycle into native
Sentry transactions and spans for Connect unary/streaming, native gRPC and server
gRPC-Web RPCs, and reports unexpected server handler exceptions. Use it when your
application already uses the Sentry Ruby SDK, with or without `sentry-rails`,
and you want one Sentry observation per RPC side, linked across services by
`sentry-trace` and `baggage`.

The package is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

```ruby
gem "connectrpc-sentry", "~> 0.2.0"
```

The only runtime dependencies are `connectrpc ~> 0.2.0` and
`sentry-ruby >= 6.7.0, < 8`. Core and the HTTP/framework packages do not depend
on Sentry. `sentry-rails` is optional; Rails RPCs work through the active-span
rule below.

## Usage

Create a subscriber and pass the resulting instrumentation to both the client
and the server:

```ruby
require "connectrpc/sentry"

subscriber = ConnectRPC::Sentry::Subscriber.new
instrumentation = ConnectRPC::Instrumentation.new(
  subscribers: [subscriber], on_error: ->(error) { application_logger.warn(error.class.name) }
)
# Pass instrumentation: instrumentation to both ConnectRPC::Client.new and
# ConnectRPC::Server.new. Their other options and generated callers are unchanged.
```

`capture_exceptions:` defaults to `true`; pass `false` to keep tracing without
reporting handler exceptions. For native Rails controllers, override the private
`connect_instrumentation` hook instead; see [host notes](#host-notes).

`mise run sentry:example` runs generated callers against the real SDK with an
in-memory transport from the repository root.

### What the application owns

The application owns `Sentry.init`: DSN, sampling (`traces_sample_rate` or
`traces_sampler`), `before_send`, transports, `excluded_exceptions` and
`trace_ignore_status_codes`. Requiring `connectrpc/sentry` never initializes or
reconfigures the SDK, and the subscriber records nothing until
`Sentry.initialized?`.

Fiber hosts need fiber hub isolation. Applications that serve or call RPCs on
the native Async or Falcon hosts, `ConnectRPC::Async::Transport` or
`ConnectRPC::Async::ThreadedTransport` must set:

```ruby
Sentry.init do |config|
  config.hub_isolation_level = :fiber # sentry-ruby >= 6.7.0
end
```

With the default `:thread` level, every fiber on a reactor thread shares one
Sentry scope, so interleaved RPCs would merge traces and swap spans. The
subscriber fails closed instead: when a fiber scheduler is active and isolation
is `:thread`, at RPC start or at any activation, it records no span, injects no
headers and leaves the scope untouched. It logs one warning per subscriber
through the SDK logger. Unexpected server exceptions are still reported, on a
cleared temporary scope that carries only the RPC tags. `ThreadedTransport`
callers on ordinary threads keep their client spans; only the activation inside
the transport's reactor thread is skipped.

## Lifetime model

Each RPC side produces at most one observation, chosen when the RPC starts:

| Side | Current scope has an open span | No open span |
| --- | --- | --- |
| Server | Child span `rpc.server`, finished when processing finishes | Own `rpc.server` transaction continued from `sentry-trace`/`baggage`, finished at terminal delivery |
| Client | Child span `rpc.client`, finished at terminal completion | No span; outgoing propagation only, as Sentry's Net::HTTP integration does |

Rails, `sentry-rails` and `Sentry::Rack::CaptureExceptions` finish their
`http.server` transaction when the Rack call returns. ConnectRPC processing runs
inside that call, while response delivery comes after it. A nested `rpc.server`
span therefore ends at `processing_finished`, while its parent is still open, so
controller and ActiveRecord spans stay below it in the request transaction.
When the parent is Sentry's `http.server` transaction, its name becomes the RPC
route (`example.v1.EchoService/Echo`, source `route`); unknown methods keep the
host's name.

Native Async and Falcon servers have no request transaction, so each RPC owns
one. Owned transactions record delivery facts (`completed`, `aborted`,
`body_closed`, `response_finished` or `unobserved`). Admission rejections finish
an owned transaction without ever activating it.

A child is promoted to its own transaction, with the same trace id, span id,
parent span id, start time and sampling decision, when its parent transaction
has already finished by the time the child ends, or when the SDK's span recorder
(1,000 spans per transaction) rejected it at start. A typical case is a caller
that finishes its transaction inside a streaming block and keeps consuming. A
promoted child is never also sent as a child.

Documented limits:

- Rack and Rails delivery facts, including an abort after the response, are
  recorded nowhere, because Sentry's middleware has already sent its
  transaction.
- Spans nested under a promoted child stay in the original parent's recorder
  and are orphaned or dropped.
- A parent finished concurrently on another thread between the check and its
  serialization can still drop the child.
- Sentry's default `trace_ignore_status_codes` drops transactions whose
  `http.response.status_code` is 301-303, 305-399 or 401-404. That includes
  owned Connect RPCs that end `unauthenticated`, `permission_denied` or
  `not_found`. Change the setting if you want those transactions.

### Scopes

An owned server transaction activates on a pushed scope, which is popped only
if it is still current. That scope's propagation context comes from the
incoming headers, so errors-only applications (no tracing) still continue the
caller's trace. Nested and client spans save and restore the current span
without pushing, so `Sentry.set_user` and tags set in a Rails request stay on
the request. A child pushes its own scope only when it activates in a context
that has no span to restore; a nested server span seeds that scope from the
scope the RPC started on. A nested server span's error event and promoted
transaction use the scope its handler ran on, even when the RPC started in
another fiber or thread. When that context already holds its own unrelated
span, its scope is left as is, so start-scope request data is not added. If
seeding a pushed scope fails, the child pops it again before the handler runs.
Finishing an owned or promoted transaction runs in `Sentry.with_scope` on a
cleared copy of the RPC scope (tags, user, contexts,
`rack_env`, level and event processors), never on whatever scope the finishing
host fiber or thread holds. Error events use a copy too. Copies are
independent, so the RPC tags an error event adds never reach the request scope
or later events. Starting an RPC never mutates a scope.

An owned transaction often finishes on another fiber's or thread's hub than the
one that started it. Finishing releases the transaction's profiler registration
on the starting hub, so later RPCs started on that hub can still be profiled.

## Data and privacy

Names are `fully.qualified.Service/Method`. Unknown methods use `connectrpc` or
`grpc` with `rpc.method=_OTHER`. Ops are `rpc.server` and `rpc.client`; origin
is `auto.rpc.connectrpc`.

Span data is bounded: `rpc.system.name` (`connectrpc` or `grpc`), `rpc.method`,
`connectrpc.protocol=grpc_web` for gRPC-Web, `rpc.status_code` (lower-case
Connect codes, upper-case gRPC codes including `CANCELLED`),
`http.response.status_code`, `server.address`/`server.port`, measured wire bytes
(`connectrpc.request.wire_bytes`, `connectrpc.response.wire_bytes`),
`connectrpc.processing.duration`, `connectrpc.processing.state`,
`connectrpc.decision` and `connectrpc.delivery`. There are no payloads,
metadata, URLs, query strings or exception text on spans.

The RPC data names follow the OpenTelemetry
[semantic conventions 1.45.0](https://github.com/open-telemetry/semantic-conventions/blob/v1.45.0/docs/rpc/README.md),
as `connectrpc-otel` records them. Client spans take `server.address` and
`server.port` from the endpoint. Since 0.3.0, server spans take them from the
request's `Host` or HTTP/2 `:authority`, with `server.port` only when the
request states one; a missing or malformed authority records neither.

Span status maps the RPC code: `OK` to `ok`, `canceled` to `cancelled`,
`unknown` to `unknown_error`, `internal` to `internal_error`, and every other
code to the Sentry status of the same name. Bare HTTP rejections without a code
use Sentry's HTTP status mapping. A successful RPC whose delivery aborted ends
`unknown_error`.

## Propagation

Owned server transactions read a single `sentry-trace` value and the joined
`baggage` values. They follow Sentry's continuation rules: inherited sampling
without a `traces_sampler`, deferred decisions from the propagated
`sentry-sample_rand`, and new traces for malformed headers or a mismatched
`org_id`. An application `traces_sampler` takes precedence over the parent's
decision and receives `parent_sampled` plus
`connectrpc: { role:, protocol:, method: }`; it receives no Rack `env`. Nested
spans inherit the host's continued trace.

Clients inject `sentry-trace` and Sentry `baggage` only when
`config.propagate_traces` is true and a `trace_propagation_targets` entry
matches. Since 0.3.0, targets match the request URL in the form Sentry's own
HTTP integrations use: `scheme://host/path`, without the port or query, and
with IPv6 hosts in brackets. A client with base URL
`https://api.internal:8443/rpc/` calling `example.v1.EchoService/Echo` matches
against `https://api.internal/rpc/example.v1.EchoService/Echo`, so the usual
Sentry forms work: a URL string such as `"https://api.internal"`, a host
substring such as `"api.internal"`, or a regex such as
`%r{\Ahttps://api\.internal/}`. With a client span, the headers carry that span
and its sampling decision; without one, they carry the scope's propagation
context. W3C `traceparent` is not injected.

Sentry baggage is prepended to `baggage` staged by earlier subscribers. Core
keeps caller `baggage` metadata passed to the RPC ahead of the injected values.

Sentry's `:http` patch, on by default in `config.enabled_patches`, already
propagates on `Net::HTTP` requests. Since 0.3.0, when that patch has been applied
and will propagate a call on the core `ConnectRPC::Transports::NetHTTP` transport
(or a subclass), this subscriber leaves propagation to it instead of injecting a
second set of `sentry-*` baggage members. The patch's `http.client` span is a
child of the `rpc.client` span, so `sentry-trace` names the `http.client` span,
and the patch prepends Sentry baggage to the whole `baggage` header, ahead of
caller and other subscribers' values. The subscriber still injects when the
patch will not: for requests to the DSN's host, which the patch skips, and when
only the URL above matches a target. The patch escapes a percent-encoded base
path again, so it sees `/a%2520b/` where the URL above has `/a%20b/`. Other
transports, including the Async ones and wrappers around `NetHTTP`, are always
injected by this subscriber.

## OpenTelemetry and OTLP

Pick one path into Sentry: native Sentry tracing (this package), or OpenTelemetry
with `connectrpc-otel` exporting to Sentry through `sentry-opentelemetry` or
OTLP. Never feed both into Sentry. When `config.instrumenter == :otel`, or when
`sentry-opentelemetry` is loaded and `config.otlp.enabled` is true, this
subscriber records and propagates nothing and only reports unexpected handler
exceptions on the current scope.

When both packages feed different backends, register them in either order:

```ruby
ConnectRPC::Instrumentation.new(subscribers: [otel_subscriber, sentry_subscriber])
```

Outgoing `baggage` carries caller values, then Sentry's, then OpenTelemetry's.
On the core `NetHTTP` transport with Sentry's `:http` patch applied, Sentry's
come first instead, as [Propagation](#propagation) describes. The two
subscribers use different trace ids.

## Exception capture

Server handler exceptions never escape `ConnectRPC::Server#call`, so Sentry's
Rack middleware never sees them. With `capture_exceptions: true` (the default)
the subscriber reports the original server exception when it is a
`StandardError`, not a `ConnectRPC::Error`, and the normalized code is
`internal` or `unknown`, or processing ended without one. Events use mechanism
`connectrpc` with `handled: false`, carry the RPC scope and link to the RPC span
or transaction. SDK `excluded_exceptions` and `before_send` apply.

Never reported:

- Client failures.
- Deliberate `ConnectRPC::Error`s.
- Codec, compression and metadata input failures, which normalize to
  `invalid_argument`.
- Failures that cancellation or a deadline arbitrates to `canceled` or
  `deadline_exceeded`.
- Admission and host rejections, and delivery failures.
- Rails `rescue_responses` errors, which map to other codes.

The SDK skips an exception object that was already captured, so an application
or `sentry-rails` report of the same exception is not duplicated. Native Rails
controllers report handler errors through
`Rails.error.report(handled: false, source: "connectrpc.rails")`, which
`sentry-rails` captures only with `config.rails.register_error_subscriber = true`;
either way the bug produces one event.

Limits: an internal error whose rendered body exceeds a very small error limit
becomes `resource_exhausted` and is not reported.

## Host notes

- Rack consumers must close the body, or invoke the promised
  `rack.response_finished` callbacks, before an owned transaction finishes.
- For native Rails controllers, override private `connect_instrumentation`. The
  RPC nests under the `sentry-rails` request transaction when one is open.
- Native hosts and transports need `config.hub_isolation_level = :fiber`; see
  [What the application owns](#what-the-application-owns).
- Connections and setup rejected before any RPC exists are outside per-RPC
  observation.

## Static signatures

This package owns `sig/` and its discovery manifest. Add `library "connectrpc"`
and `library "connectrpc-sentry"` to a strict Steep target with this gem
installed. The manifest lists only standard libraries, so name each connectrpc
library explicitly; this also works with `rbs collection`. The supported API is
`Subscriber.new(capture_exceptions: bool)` and `Subscriber#start`; observers are
private. The signatures do not describe the Sentry SDK. See the
[RBS guide](../../docs/rbs.md) for supported APIs and extension contracts.

## Development

From the repository root use `mise run sentry:setup`, `sentry:test`,
`sentry:check`, `sentry:build`, `sentry:package`, `sentry:example`, and
`sentry:verify`. The independent development bundle includes the real SDK,
`sentry-rails` with Rails and in-memory SQLite ActiveRecord,
`sentry-opentelemetry`, the OpenTelemetry SDK and local integration packages.
They float to their newest allowed releases; only tools stay pinned. Specs use
`Sentry::DummyTransport`, no background worker threads and a `.invalid` DSN, so
nothing leaves the process. Caches and archives are in `vendor/sentry`, and
coverage is in `coverage/sentry/<Ruby>/`. Both pinned Ruby 3.4.12 and 4.0.9 CI
leaves run `sentry:verify`.

`floors:sentry` repeats specs and package checks with `sentry-ruby` 6.7.0 and
every other local package's runtime dependency at its gemspec floor. That lane
constrains `json` to `< 3` for the Rails floor, as the OpenTelemetry lane does.

On Ruby 3.4, `require "connectrpc/sentry"` also loads `cgi/util` when nothing
else has: `sentry-ruby` parses incoming baggage with `CGI.unescape` after
requiring only `cgi/escape`, which lacks the default charset on that Ruby.

## License

Original project work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg.
All package archives pin `allowed_push_host=https://rubygems.org`; these checks
never publish or release a gem.
