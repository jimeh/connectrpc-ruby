# Sentry: connectrpc-sentry

Status: approved by Jim (D1-D5 as recommended); one cross-engine plan review
(Codex) applied. Implemented on `feat/sentry`; see implementation notes.
Base `8c2dd4b`. Branch `feat/sentry`, PR title
`feat: add connectrpc-sentry native Sentry tracing package`. Closes #45.

## Objective

Applications on the native Sentry SDK (including `sentry-rails`) get one Sentry
observation per RPC side, linked across services by `sentry-trace`/`baggage`,
with RPC method, code and lifecycle facts, plus reports of unexpected server
handler exceptions. Core gains no Sentry dependency; the OTel gem is unchanged.

## First-gate proof (run; scratch scripts, sentry-ruby 7.2.0 and 6.7.0)

Real SDK, `Sentry::DummyTransport`, `.invalid` DSN, no network. Results were
identical on both SDK versions. Items marked *design* are not proven yet and
become planned specs.

| Gate item | Observed |
| --- | --- |
| Rails parent/child | sentry-rails `CaptureExceptions` finishes its `http.server` transaction when `app.call` returns. `ConnectRPC::Rack` and the Rails controller run processing inside that call (buffered `Server#call`); delivery (`body_closed`/`response_finished`) comes after. Without this gem every RPC transaction is named `EchoController#connect`. |
| Parent finishes first | `TransactionEvent` serializes only children with a timestamp. A child finished at delivery was dropped and spans nested under it pointed at a missing parent. A child finished at `processing_finished` was kept, renamed host transaction included. |
| Interleaved Async RPCs | Default `hub_isolation_level = :thread`: two interleaved RPCs on the native Async host shared one scope, merged traces, swapped DB spans and broke the caller's scope. `:fiber` (SDK >= 6.7.0): isolated, trace continued over real h2c. |
| Finish context | `Transaction#finish` captures with the *current* hub scope, including `rack_env`, `level` and event processors. |
| Incoming propagation | Without `traces_sampler`, `-0`/`-1` are inherited; a deferred decision follows propagated `sentry-sample_rand` (0.2 sampled, 0.7 dropped at 0.5); malformed headers and mismatched `org_id` start new traces. An application `traces_sampler` takes precedence over `parent_sampled` (SDK behavior, both versions) and receives it. `Sentry.continue_trace` mutates the current scope; `Sentry::PropagationContext.new(nil, headers)` applies the same rules without side effects. |
| Outgoing propagation | With a span: child span id, sampled flag, full DSC baggage. Without one: the scope propagation context, as Sentry's Net::HTTP does. No W3C `traceparent` natively. |
| OTel coexistence | Two subscribers on one client: OTel writes `baggage` as a String and overwrites Sentry's when it runs second; Sentry second keeps both. Trace ids differ. `instrumenter = :otel` makes native spans no-ops; modern `config.otlp.enabled` does not change `instrumenter`. |
| Exceptions | Unexpected handler exceptions never escape `Server#call`, so Sentry Rack middleware never sees them. Rails reports them via `Rails.error.report(handled: false, source: "connectrpc.rails")`, which sentry-rails captures only with `register_error_subscriber = true` (default false). `capture_exception` skips an already captured exception object. |
| SDK floor | 6.7.0 added `hub_isolation_level`; every API used (`PropagationContext`, `Transaction.new(sample_rand:)`, `push_scope`/`pop_scope`, scope readers, `generate_propagation_context`, `exception_captured?`) is unchanged in 6.7.0 and 7.2.0 (current, 2026-10-28). |

Design items not yet proven: promotion, fail-closed guard, exception
classification and dedup, target filtering, errors-only continuation,
finish-scope restoration, `ThreadedTransport`.

## Decisions (settled: all recommended options)

### D1. Supported SDK range

| Option | For | Against |
| --- | --- | --- |
| **A. `sentry-ruby >= 6.7.0, < 8`** (recommended) | Fiber isolation; covers apps on 6.7 and 7.x; identical proofs. | Two majors (floors lane runs 6.7.0). |
| B. `>= 7.0, < 8` | One major. | Excludes 6.7 with no API reason. |
| C. `>= 5.x` | Wider. | No fiber isolation, so Async hosts cannot be correct. |

`sentry-rails` stays a dev dependency; Rails works through the active-span rule.

### D2. Lifetime model

| Option | For | Against |
| --- | --- | --- |
| **A. Nest-or-own with promotion** (recommended) | One transaction per Rails/Rack RPC, renamed to the RPC route; controller/DB spans nest under `rpc.server`; native hosts keep delivery facts. | Rack/Rails delivery facts (including an abort after the response) are recorded nowhere, because Sentry's middleware has already sent its transaction. Promotion orphans spans nested under a promoted child. |
| B. Server always owns a transaction | One rule; delivery always recorded. | Two transactions per Rails/Rack RPC (quota, two Performance entries). |
| C. Child span ending at terminal completion | Simple. | Proven dropped under sentry-rails. |

Option A rules:

- Server with an open active span where the RPC starts: child `rpc.server`
  span, finished at `processing_finished` (or at `finished` when processing
  never ran). Terminal facts are added while the parent is still open. If the
  parent is Sentry's `http.server` root transaction, the scope transaction name
  becomes `pkg.Service/Method` (`:route`) after processing.
- Server without one: own transaction continued from the carrier, finished at
  `finished` with delivery facts.
- Client with an active span: child `rpc.client` span, finished at `finished`.
  Client without one: no observation, propagation only (Sentry's Net::HTTP
  convention). "One observation per side" holds when the caller is traced.
- Promotion: when a child would end and its parent transaction already has a
  timestamp, or the SDK recorder (1,000 spans) rejected the child at start, the
  child is sent as its own transaction with the same trace id, span id, parent
  id, start time and sampling, and is never finished as a child, so it cannot
  appear twice. Limits, documented: a parent finished concurrently on another
  thread between the check and its serialization can still drop the child.

### D3. Hub and scope isolation

| Option | For | Against |
| --- | --- | --- |
| **A. Fail closed** (recommended): the app sets `config.hub_isolation_level = :fiber` for fiber hosts (documented). At `start` and at every `attach`, if a fiber scheduler is active on the current thread and isolation is `:thread`, the subscriber records no span, injects nothing and leaves the scope untouched, with one SDK-logger warning per subscriber. Unexpected exceptions are still captured, on a cleared temporary scope carrying only RPC tags. | No proven corruption; no config changes; covers `ThreadedTransport` reactor activations. | Tracing stays off (with a warning) until configured. |
| B. Report every call through `Instrumentation` `on_error` | Loud. | Noisy per RPC; same data. |
| C. Gem sets `hub_isolation_level` | Works by default. | Changes global config; #45 rejects it. |

Scope rules: an owned server transaction activates on a pushed scope, popped only
if still current, whose propagation context comes from the carrier, so errors-only
apps continue the incoming trace. Nested and client spans save and restore the
current span without pushing, so Rails `set_user`/tags stay on the request.
Finishing an owned transaction runs in `Sentry.with_scope` after clearing it and
copying every attribute of the RPC scope (`update_from_scope`, `rack_env`,
`level`, event processors). `start` never mutates a scope.

### D4. Propagation and OTel coexistence

- Incoming: owned transactions read `sentry-trace` (single value) and joined
  `baggage`; nested spans inherit the host's continued trace.
- Outgoing: only when `propagate_traces` is true and `trace_propagation_targets`
  matches. **Sub-decision:** `Call` has no URL, so targets match `host` and
  `host:port` (recommended; scheme-anchored regexes then fail closed).
  Alternative: ignore targets and document it. Sentry baggage is prepended to
  baggage staged by earlier subscribers.
- Known core limit (#73, not this PR): `Client#injected_metadata`
  replaces caller `baggage` metadata with injected values via `Hash#merge`,
  for OTel too.
- Sampling: owned transactions use `Sentry.start_transaction` with parent
  sampled, propagated `sample_rand` and baggage; `traces_sampler` gets
  `connectrpc: { role:, protocol:, method: }` and no Rack `env`.
- Inert for tracing and propagation when `instrumenter == :otel` or
  `config.otlp.enabled` (sentry-opentelemetry present); exception capture stays.
  Docs: pick native Sentry (`connectrpc-sentry`) or OTel to Sentry
  (`connectrpc-otel` + sentry-opentelemetry/OTLP), never both feeding Sentry.
  When both gems feed different backends, list `connectrpc-otel` first.

### D5. Exception capture

| Option | For | Against |
| --- | --- | --- |
| **A. On by default, server only** (recommended): capture `outcome.error` when it is a `StandardError`, not a `ConnectRPC::Error`, and the normalized code is `internal` or `unknown` (or processing ended with no normalized error). Mechanism `connectrpc`, `handled: false`, linked to the RPC span and scope; `capture_exceptions: false` disables it. | Rack and native hosts otherwise never report handler bugs. Client input failures (codec, compression, metadata), cancellation and deadlines arbitrate to other codes; Rails `rescue_responses` map to other codes. sentry-rails' subscriber and ours dedupe by object. | A bug whose internal error body exceeds a tiny response limit is renamed `resource_exhausted` and is not captured (documented). |
| B. Opt-in | Conservative. | Default Rack/native apps lose handler bugs. |
| C. Never | Smallest. | Same loss; #45 asks to settle capture. |

Never captured: client role, deliberate `ConnectRPC::Error`, admission/host
rejections, delivery failures. SDK `excluded_exceptions` applies. #26 M1's
residual host `$ERROR_INFO` sites can misattribute `outcome.error`; documented.

## Approach and scope

New `gems/connectrpc-sentry/` mirroring `connectrpc-otel`:

- `lib/connectrpc/sentry.rb`, `sentry/version.rb`, `sentry/subscriber.rb`;
  `ConnectRPC::Sentry::Subscriber.new(capture_exceptions: true)` with private
  observers. The SDK is always `::Sentry`. `start` returns nil unless
  `::Sentry.initialized?` (or the observer only captures exceptions).
- Callbacks: `attach`, `detach`, `processing_finished`, `finished`. Each is
  exception-safe and independent of the executing context; client unary
  processing/terminal callbacks run after activation unwinds, native hosts hold
  processing through cleanup, and admission rejection finishes without attach.
- Names `pkg.Service/Method`; unknown methods `connectrpc`/`grpc` and
  `rpc.method=_OTHER`. Ops `rpc.server`/`rpc.client`, origin
  `auto.rpc.connectrpc`, source `:route`.
- Bounded data: `rpc.system.name`, `rpc.method`, `connectrpc.protocol`
  (gRPC-Web), `rpc.response.status_code`, `http.response.status_code`, client
  `server.address`/`server.port`, wire bytes, processing duration,
  `connectrpc.processing.state`, `connectrpc.decision`, `connectrpc.delivery`.
  No payloads, metadata, URLs or exception text.
- Status: code to Sentry status (`canceled`->`cancelled`,
  `unknown`->`unknown_error`, `internal`->`internal_error`, others by name);
  bare HTTP via `set_http_status`; delivery abort after OK -> `unknown_error`.
- Wiring per #66/#65, only where a new optional package needs it: Gemfile
  (`cooldown: 3`; sentry-ruby, sentry-rails, railties, local path gems; tools
  pinned), scripts (run/tools/check/build/package/example/integration/
  ci_runtime), `mise.toml` `sentry:*` and `floors:sentry`, `mise.ci.toml`, CI
  jobs on Ruby 3.4.12 and 4.0.9 plus the floors leaf, `.gitignore`,
  `.rubocop.yml`, Release Please config and extra files, release train/
  publisher order, `spec/ci_runtime_spec.rb` and package/release spec
  inventories, `sig/` + manifest, `typecheck/` caller and negative,
  `docs/rbs.md` inventory, `docs/{development,ci,releasing,support,design}.md`,
  root README, AGENTS.md, `docs/plans/sentry.md` (this plan). No core files.

## Observable success

- Rails + sentry-rails: one `http.server` transaction named
  `example.v1.EchoService/Echo`, an `rpc.server` child with the code, and
  controller/ActiveRecord spans below it.
- Native Async/Falcon with `:fiber`: one `rpc.server` transaction per RPC,
  child of the caller's `rpc.client`, delivery `completed`, interleaved RPCs
  separated.
- A handler bug yields exactly one error event linked to the RPC span on Rack,
  native and Rails (both error-subscriber settings).

## Non-goals

Metadata allowlists, payloads, message events, breadcrumbs, profiler, logs,
metrics, OTLP/sentry-opentelemetry setup, W3C `traceparent`, client exception
capture, connection-level observation, core or OTel changes, the caller-baggage
core limit.

## Verification

All specs assert exported Sentry events; async specs synchronize on queues and
observable events, never sleeps. Run on SDK latest and the 6.7.0 floor.

| Concern | Evidence |
| --- | --- |
| Rails nesting, rename, ActiveRecord spans (sqlite in-memory) | `rails_spec`, real controller |
| Rack with and without Sentry middleware | `rack_spec`: nested; owned with `body_closed` and `response_finished` |
| Interleaved Async/Falcon isolation, h2c propagation, admission rejection | `native_spec`, `:fiber` |
| `:thread` under a scheduler fails closed, warns once, still captures | `native_spec` |
| `ThreadedTransport` from concurrent ordinary threads, unary and streaming, scope restored | `threaded_spec` |
| Promotion: caller finished inside a streaming block, consumption continued; host closed; recorder saturated (999 children); no duplicate | `lifetime_spec` |
| Finish scope: RPC context kept, foreign `rack_env`/processor absent | `lifetime_spec` |
| Incoming: inheritance without sampler, sampler override sees `parent_sampled`, raising sampler, `sample_rand`, malformed, org mismatch; DSC values | `propagation_spec` |
| Outgoing: span/no-span headers, `propagate_traces`, targets, baggage order with the real OTel subscriber | `propagation_spec` |
| `instrumenter :otel` and `otlp.enabled` inert, capture kept | `otel_spec` |
| Exceptions: bug once; codec error, deliberate, real cancel, deadline, client, opt-out not captured; Rails dedup both settings | `exception_spec`, `rails_spec` |
| gRPC, gRPC-Web codes, delivery abort | `native_spec`, `grpc_spec` |
| Floor | `floors:sentry` (6.7.0) |
| Installed consumer without Rails; types | `sentry:package`, `rbs:check`, `rbs:package` |
| Final gates | `sentry:verify`, `mise run verify`, `mise run rack:verify`, CI on both lanes |

Perturbation: finishing the nested span at `finished` must fail the Rails
assertion.

## Open questions

None. D4 targets match `host` and `host:port`; the caller-baggage limit is #73.

## Implementation notes

Minor choices made while implementing, within the decisions above:

- Unknown methods do not rename Sentry's `http.server` transaction, so
  unmatched paths keep the host's name instead of collapsing into `connectrpc`.
- Captured error events also carry the bounded `rpc.system.name` and
  `rpc.method` tags, the same tags the fail-closed temporary scope carries.
- A child that activates where the current scope has no span pushes its own
  scope and pops it only if it is still current; otherwise children save and
  restore the span without pushing.
- On Ruby 3.4, `require "connectrpc/sentry"` also loads `cgi/util` when nothing
  else has. sentry-ruby parses incoming baggage with `CGI.unescape` after
  requiring only `cgi/escape`, which raises `NameError` there without it. Ruby
  4.0 needs nothing.
- Status for a bare HTTP rejection uses Sentry's `set_http_status`, which leaves
  statuses outside its map (415, for example) unset.
- `sentry-opentelemetry` is a development dependency for the OTLP-inert spec. It
  follows sentry-ruby's version; 6.7.0 and 7.2.0 both provide `config.otlp`, so
  the example runs on both lanes (it skips only if a release lacks it).
- A raising `traces_sampler` falls back to `traces_sample_rate` on sentry-ruby
  7, but raises through `start` on 6.7.0, where the lifecycle reports it and the
  RPC is served untraced. The propagation spec asserts both.
- Hosts activate an RPC more than once in the same context (native hosts wrap
  `Server#call`, which activates again). An owned transaction's inner activation
  reuses the active RPC scope instead of pushing a copy, so handler changes such
  as `Sentry.set_user` reach the transaction and its error event.
- A nested child that already ended at processing still receives terminal facts
  while its parent is open; a promoted child never does.
- Scope copies (error events, owned activations in another context, finishing)
  read from `Scope#dup`, because `update_from_scope` shares the source's tag,
  context, extra, user, breadcrumb and attribute containers. Adding the RPC tags
  to an error event therefore no longer reaches the request scope, or the owned
  RPC scope and its transaction. The copy keeps the source's span object, which
  `Scope#dup` would otherwise duplicate.
- `Sentry.start_transaction` registers the profiler on the starting hub, and
  `Transaction#finish` stops it only on the finishing hub. An owned transaction
  keeps its starting hub and calls its `stop_profiler!` (an SDK `@api private`
  method, present in 6.7.0 and 7.2.0) before finishing, so a transaction
  finished on another fiber's hub releases the registration.
- A nested server child records the scope it activates on (the current scope it
  saves and restores the span on, or the scope it pushes) and uses it for error
  capture and promotion, so handler `Sentry.set_user` and tags reach both when
  the RPC started in another context. The `http.server` rename still uses the
  start scope, and client children still promote with their start scope.
