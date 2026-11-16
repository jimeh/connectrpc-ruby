# Health: connectrpc-health (grpc.health.v1)

Status: approved by Jim; implementation contract for one PR (issue #30).
Base: `8c2dd4b`. Branch `feat/grpc-health`, PR title
`feat: add the standard gRPC health service`. The draft had a cross-engine
plan review; accepted findings were checked against the code.

## Objective

An application opts in to the standard `grpc.health.v1.Health` service,
registers per-service serving status and updates it safely from any thread or
fiber. Kubernetes gRPC probes, `grpc_health_probe`, load balancers and grpc-go
health clients can query it (Check, List) or stream it (Watch).

## Verified facts

- The canonical [health.proto](https://github.com/grpc/grpc-proto/blob/master/grpc/health/v1/health.proto)
  now has `Check`, `List` and `Watch`, plus `HealthListRequest/Response`.
  grpc-go 1.85.0 (`interop/grpc/go.mod`) implements all three in
  `google.golang.org/grpc/health`: `""` starts SERVING, Check unknown is
  `NOT_FOUND`, List over 100 entries is `RESOURCE_EXHAUSTED`, Watch sends
  `SERVICE_UNKNOWN` for unknown names, coalesces through a one-slot channel,
  skips repeated statuses (`lastSentStatus`), and keeps streams open after
  `Shutdown`. Its client package `grpc_health_v1` is already in the pinned
  module graph.
- The generator emits `require "<proto path>_pb"` and looks up every message
  class at load (`lib/connectrpc/generator/renderer.rb:13,73`). Its only
  parameter is `rbs=true`; protoc 36.2 `--rbs_out` is already used by
  `scripts/build_grpc_peer.rb`.
- Server-stream handlers send only on the owning reactor/thread
  (`StreamingContext#check_headers_scope!`). Status updates from other threads
  can only signal; the handler fiber sends.
- Peer `RST_STREAM(CANCEL)` only cancels the Context token
  (`async/connection.rb:364`); the handler task keeps running. Other reset codes
  and an idle HTTP/1.1 peer that disappears signal nothing. Connection loss seen
  by the H2 reader and forced retirement cancel the handler task, which unwinds
  a Mutex/ConditionVariable wait. Graceful close waits for handlers until grace
  expires. There is no public server-side cancellation hook today;
  `CallCancellation#on_cancel` exists internally (`cancellation.rb`). Its
  listeners run synchronously in the cancelling thread, or immediately on
  registration when already cancelled, and swallow `StandardError`.
- `Server` accepts mixed services and validates declared paths, not host
  capability. Portable adapters (Rack, Rails, Testing) reject Watch, but by
  protocol: Connect streaming media gets 415 before dispatch, a unary-encoded
  request to the Watch path gets `unimplemented`, generated Watch callers on
  portable transports fail locally, gRPC-Web unary has its own error.
- Watch bypasses `invoker:` (`streaming/server_execution.rb:60`); the invoker
  covers unary only.
- Releases are synchronized across packages (`scripts/release_train.rb`).
  `typecheck/installed_api.rb` and `typecheck/archive.rb` assume one package
  `.rbs` plus `manifest.yaml` and MIT-only license metadata.
- Kubernetes gRPC probes use plaintext h2c only.

## Decisions (Jim approved all recommendations)

- D1: new optional gem `connectrpc-health`, not core.
- D2: `shutdown` sets every entry `:not_serving`; existing Watch streams send
  `NOT_SERVING` (if not already sent) and end with OK; a Watch started after
  shutdown fails `unavailable` before sending any message (grpc-go's client
  resets backoff after any received message, so a message-then-end reply would
  cause tight retry loops during drain). Final delivery is best effort, bounded
  by host grace.
- D3: add a small public `ConnectRPC::Context#on_cancel { }` in core, returning
  a `CancellationRegistration`, delegating to `CallCancellation#on_cancel`.
- D4: non-CANCEL resets and idle HTTP/1.1 disappearance are a documented
  residual: such a watcher is released at the next status change for its
  service, at connection retirement, or at shutdown. `max_watchers` bounds the
  cost. No Watch lifetime knob.
- D5: `""` starts `:serving`; `max_watchers` defaults to 100; List included.
- D6: grpc-go's health client is the independent standard client; no
  `grpc_health_probe` pin.

## Package layout

```
gems/connectrpc-health/
  connectrpc-health.gemspec  # connectrpc ~> x.y.0; google-protobuf >= 4.36.2, < 5; licenses MIT, Apache-2.0
  Gemfile .rspec LICENSE README.md scripts/ spec/
  proto/grpc/health/v1/health.proto  # unchanged upstream at a pinned grpc-proto commit
  proto/LICENSE                      # Apache-2.0
  proto/README.md                    # commit, SHA256, source URL, license note
  lib/connectrpc/health.rb           # descriptor guard, generated glue, Registry, SERVICE
  lib/connectrpc/health/registry.rb
  lib/connectrpc/health/version.rb
  lib/grpc/health/v1/health_pb.rb        # generated, protoc 36.2 builtin ruby
  lib/grpc/health/v1/health_connect.rb   # generated, protoc-gen-connectrpc-ruby
  sig/connectrpc-health.rbs sig/manifest.yaml
  rbs/grpc_health_v1/grpc/health/v1/health_pb.rbs       # generated, opt-in bridge
  rbs/grpc_health_v1/grpc/health/v1/health_connect.rbs  # generated, opt-in bridge
```

- Deviation: the generated `.rbs` files were first planned under `sig/grpc/`.
  Their messages subclass `Google::Protobuf::AbstractMessage`, declared only in
  core's opt-in `rbs/google_protobuf_4_36`, and Steep 2.1.0 builds `library`
  signatures before `signature` paths, so `library "connectrpc-health"` failed
  with `NoSuperclassFoundError`. They ship as the opt-in `rbs/grpc_health_v1`
  bridge instead; typed callers add it and the protobuf bridge as `signature`
  paths.
- `scripts/build_generation.rb` runs protoc for this proto into temporary
  roots, copies `.rb` under the gem's `lib/` and `.rbs` under
  `rbs/grpc_health_v1/` (schema-relative paths, no doubled `grpc/`), checks
  exact file sets for both scoped to the generated subtrees, and checks the
  vendored proto's pinned SHA256. `generation:build` and `generation:check`
  cover it.
- The generated files use the canonical feature name `grpc/health/v1/health_pb`,
  which the `grpc` gem shares. Ours is a superset (has List), so loading
  `connectrpc/health` first satisfies both. It loads its own `_pb` by path
  before the generated glue, so the grpc gem's file earlier on the load path
  cannot win the shared feature name (review finding). It checks the generated
  pool: a preloaded `grpc.health.v1.HealthCheckRequest` without
  `grpc.health.v1.HealthListRequest` raises a clear `LoadError` telling the user
  to require `connectrpc/health` before the grpc gem's health files.

## Public API

```ruby
require "connectrpc/health"

health = ConnectRPC::Health::Registry.new(max_watchers: 100)
health.set("demo.v1.DemoService", :serving) # => true; false (ignored) after shutdown
health.status("demo.v1.DemoService")        # => :serving | :not_serving | :unknown | nil
health.shutdown                             # idempotent
health.shutdown?                            # => bool
health.watcher_count                        # => Integer
health.handlers                             # => { path => callable } for Check, List, Watch
ConnectRPC::Health::SERVICE                 # Grpc::Health::V1::Health::SERVICE

server = ConnectRPC::Server.new(
  services: [Demo::V1::DemoService::SERVICE, ConnectRPC::Health::SERVICE],
  handlers: demo_handlers.merge(health.handlers),
  binding: ConnectRPC::Bindings::GoogleProtobuf.new, protocol: :auto
)
```

- Settable statuses: `:unknown`, `:serving`, `:not_serving`. Service names are
  Strings (stored as frozen copies). Other values raise `ArgumentError`.
  `max_watchers` must be a positive Integer. No unregister.
- Check: registered name returns its status; unknown name fails `not_found`.
- List: snapshot of all entries; more than 100 fails `resource_exhausted`.
- Core `Context#on_cancel` gets RBS, supported/SPI inventory and a core spec.

## Registry and Watch concurrency

- One Mutex guards the status map, the shutdown flag and the watcher table.
  Each watcher has one pending-status slot, a cancelled flag and its own
  ConditionVariable. `set` and `shutdown` update the slots and signal.
- Watch admission and initial-status capture are atomic under the mutex. Over
  `max_watchers`, fail `resource_exhausted` before sending anything; after
  shutdown, fail `unavailable`. Then send the current status, or
  `SERVICE_UNKNOWN` for an unregistered name.
- Register the cancellation listener outside the registry mutex (it may run
  synchronously, and the Mutex is not reentrant). The listener only takes the
  mutex, sets the watcher's cancelled flag and signals. The waiter re-checks
  that flag under the mutex before every wait, so a cancel before the wait is
  never lost.
- Never call Context checks or writer sends while holding the mutex.
- Wait timeout is `context.deadline&.remaining`; expiry fails
  `deadline_exceeded`; cancellation fails `canceled`.
- The slot holds only the latest status: slow consumers see coalesced updates,
  never a queue. Consecutive duplicates are never sent.
- `ensure` removes the watcher and unregisters the listener on every exit path,
  including failure during admission or the first send, send failures and task
  cancellation from host retirement.
- Shutdown: watchers of registered names send `NOT_SERVING` if not already sent,
  then end OK; watchers of unregistered names end without another message.

## Hosts

| Host | Check / List | Watch |
| --- | --- | --- |
| Async Host (H2: h2c/TLS/mTLS) | Connect + gRPC | Connect + gRPC |
| Falcon H2 | Connect + gRPC | Connect + gRPC |
| Falcon H1 | Connect | Connect |
| Rack / Rails / Testing | Connect (gRPC-Web unary where enabled) | unsupported, protocol-specific rejection |

The README covers: Kubernetes `grpc:` probes and `grpc_health_probe` need
Async or Falcon H2; a second plaintext h2c `Host` on a probe port sharing the
registry (also the recommended authentication boundary); handler wrapping for
authenticated exposure (the invoker does not cover Watch); calling
`health.shutdown` before `host.close(grace:)`; Falcon registries are
worker-local; the D4 residual; grpc gem load order.

## Interop

- `interop/grpc/health.go` (new file) uses grpc-go's `grpc_health_v1` client.
  `main.go` gets only a minimal command hook. No `go.mod` or artifact change.
- The Ruby server under test serves Health plus the existing `Peer` service.
  Its `Peer.Unary` handler accepts test-only control values
  (`health:set:<svc>:<STATUS>`, `health:watchers`, `health:shutdown`), so status
  changes happen inside the worker that owns the client's connection.
- `health-client <origin> <mode> <fixtures>`: Check `""`, registered, unknown
  (`NOT_FOUND`); List; Watch initial, change, unknown then registered; Watch
  cancel then poll `health:watchers` back to baseline under a bounded deadline;
  Watch then `health:shutdown`, expect `NOT_SERVING` then EOF; a new Watch then
  fails `Unavailable`.
- `health-control <mode> <fixtures>` runs the same client against grpc-go's
  reference `health.NewServer()`. Documented exceptions: no watcher count, and
  after shutdown the reference keeps the stream open and admits new watchers.
- A Ruby driver runs it against Async Host and real Falcon workers over h2c,
  TLS and mTLS, exposed as `health:interop`.

## Wiring (PR #66 pattern)

mise tasks `health:setup|test|check|build|package|floors|verify|interop`;
`vendor/health` bundle/package outputs and `coverage/health`; CI jobs on Ruby
3.4.12 and 4.0.9 (with Go for interop) and the floors leaf; CI bundle caches;
`mise.ci.toml` overrides; release-please extra file; `ReleaseTrain::NAMES`,
`GUIDES` and fixtures; publisher order; `rbs:package` and release-candidate
dependencies; `spec/package_build_spec.rb` inventory; Rakefile syntax coverage;
`.rubocop.yml` generated-code exclusion; `typecheck/installed_runtime.rb`;
`typecheck/installed_api.rb` and `typecheck/archive.rb` package-specific
expectations (generated signatures, MIT + Apache-2.0) without loosening other
packages; the opt-in generated health and protobuf bridges for typed consumers; installed
Steep caller `typecheck/api/health.rb` plus a negative; `docs/support.md`
inventory; root README package list; AGENTS.md map line; gem README.

## Non-goals

Client-side health-aware routing; automatic host shutdown hooks; a
`grpc_health_probe` pin; Rails helpers; reflection; unregistering services; an
authentication DSL; host-level stream-termination signals for non-CANCEL resets.

## Verification map

| Concern | Evidence |
| --- | --- |
| Check/List semantics, validation, unknown, List limit, sets ignored after shutdown | health unit specs |
| Watch initial, change, dedupe, coalescing with a blocked writer | unit specs with a controllable writer |
| Cancel before registration, between check and wait, concurrent unregister | deterministic unit specs |
| Cross-thread `set` wakes a reactor watcher | Async host spec, `set` from a plain Thread |
| `max_watchers` admission and re-admission | spec |
| Idle CANCEL ends handler and frees the slot | Async host spec, wire progress first |
| H2 connection loss, forced close, grace expiry without shutdown | Async host specs |
| Graceful close after `shutdown` finishes well before grace | Async host spec |
| Non-CANCEL reset residual released at next status change | Async host spec |
| Falcon H1 Connect Watch and shutdown | Falcon spec |
| Portable rejection of Watch, Connect Check via Rack/NetHTTP | specs |
| Wrapped-handler denial reveals no status, allocates no watcher | spec |
| grpc gem descriptor guard | subprocess spec with a synthetic older descriptor |
| Independent standard client | `health:interop`, grpc-go vs Async and Falcon, h2c/TLS/mTLS, plus reference control |
| Generated Ruby client over Connect and gRPC | host specs |
| Descriptor reproducibility and notice | `generation:check` |
| Typed and installed consumers | `rbs:check`, `rbs:package`, `health:package` |
| Floors | `health:floors` CI lane |
| `Context#on_cancel` | core spec + RBS |
