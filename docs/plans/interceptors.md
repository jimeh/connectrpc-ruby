# Typed client and server interceptors

Status: design for review. No implementation is approved by this document.
Base: `7a07f26` (release 0.1.2).
Issue: [#32](https://github.com/jimeh/connectrpc-ruby/issues/32). Related:
[#26](https://github.com/jimeh/connectrpc-ruby/issues/26) groups L1/L3/L6 and
L2/L5, the lifecycle metadata group L8/L9/L11, and
[#73](https://github.com/jimeh/connectrpc-ruby/issues/73).
Delivery: three implementation PRs, each needing separate approval, after the
prerequisites in [Dependencies](#dependencies-on-26-and-the-lifecycle-metadata-work).

## Objective

Applications need one place to put policy that applies to many RPC methods:
authentication, authorization, request validation, metadata injection, error
translation and similar concerns. Today they have two partial tools, and
neither fits:

- Instrumentation subscribers observe every RPC, but they are deliberately
  observers. They cannot reject a call, change a message or add metadata other
  than the staged propagation carrier
  ([design](../design.md#rpc-lifecycle-observation)).
- A custom server `invoker` can wrap handler execution, but it covers unary
  calls only, there is one per Server, and the client has nothing equivalent.
  Streaming handlers never reach the invoker
  (`lib/connectrpc/streaming/server_execution.rb:60-79`), so the health
  service's Watch bypasses it (`gems/connectrpc-health/README.md:113`).

This design adds typed interceptors to `ConnectRPC::Client` and
`ConnectRPC::Server` for all four RPC shapes and all supported protocols. It
answers each acceptance criterion in #32:

| #32 acceptance criterion | Answered in |
| --- | --- |
| Typed interfaces, ordering and lifetime across shapes and protocols; generated-client and custom transport/binding compatibility | [API](#api), [Ordering](#ordering), [Host coverage](#host-coverage), [Static typing](#static-typing) |
| Ordering relative to encoding, deadlines, instrumentation, native handlers and Rails callbacks; host policy preserved | [Ordering](#ordering), [Host coverage](#host-coverage) |
| Short-circuit errors, leading/trailing metadata, immutable snapshots, user-exception identity, cancellation arbitration | [Unary semantics](#unary-semantics), [Cancellation and deadlines](#cancellation-and-deadlines) |
| Original processing task, exactly-once cleanup through streaming scopes, continuation misuse | [Continuation rules](#continuation-rules), [Streaming model](#streaming-model) |
| Proof of order, success, rejection, failure, cancellation and stream isolation, with static negatives | [Testing strategy](#testing-strategy), [Delivery](#delivery-and-verification) |

## Decisions

Jim accepted these choices on 2026-11-16:

| Question | Decision |
| --- | --- |
| API shape | One interceptor object with up to four optional methods: `client_unary`, `client_stream`, `server_unary` and `server_stream`. Each stream method covers server, client and bidi streaming. Methods are discovered once at construction. Registration is `Client.new(interceptors: [...])` and `Server.new(interceptors: [...])`. |
| Continuation | A no-argument `yield`, guarded by core: once only, on the owning fiber, and only while the interceptor runs. Misuse raises `ConnectRPC::InterceptorStateError`. Changes go through a per-RPC call object that is open until dispatch. Client unary interceptors may return without yielding; server interceptors and stream interceptors must yield or raise. |
| Server placement | After the bounded read and decode, as in connect-go v1. Unary and server-streaming interceptors see the typed request. Rejection before decode stays with host middleware. |
| Instrumentation | Interceptors run inside the instrumentation activation. Measured processing durations include interceptor time, and observer spans are active while interceptors run. Subscribers stay observers. |
| Streaming model | The stream methods wrap the whole scope. `call.wrap_stream { \|stream\| ... }` decorates the stream endpoint. Core applies wrappers innermost first, so the application sees the outermost wrapper. Core cleanup always uses the raw session. |
| Errors | A short-circuit is a raised `ConnectRPC::Error`. Core stages each exception at every continuation boundary and records the first one only when the call fails, so observers keep the original and a recovered call stays successful. Interceptor-raised exceptions are independent user exceptions and keep their identity on the client. |
| Metadata | Client request metadata is mutable until dispatch, then frozen. Response metadata and trailers are frozen snapshots on the client. Server request metadata is read-only; server response metadata is mutable until headers are published and trailers until the terminal response. Interceptors cannot change the deadline. |
| Hosts | No host-specific code except a private Rails `connect_interceptors` hook. Rails interceptors run outside the controller lifecycle. |
| Delivery | Prerequisites from #26 land first. Then three PRs: core unary, core streaming, then the Rails hook with host evidence and documentation. |

Design review on 2026-11-16 narrowed two of these choices without changing
their direction:

- Jim's planning choice let any unary interceptor return without yielding.
  On the server that would skip the invoker, which on Rails is the only path
  into authentication, CSRF and controller callbacks, so a cached success
  would bypass host policy. Only client unary interceptors keep that option.
  [Open question 6](#open-questions) records how server-side successful
  short-circuits could return later.
- Recording every boundary failure directly would mark a call that an
  interceptor recovered as failed: OTel reports an `OK` outcome that carries a
  non-`Error` exception as `operation_failed`
  (`gems/connectrpc-otel/lib/connectrpc/otel/subscriber.rb:140-147`). Failures
  are therefore staged and recorded only when the call fails.

Two directions were settled before this design:

- Instrumentation subscribers stay observers. Metrics (`connectrpc/metrics`,
  `connectrpc-prometheus`, `connectrpc-yabeda`), OTel and Sentry keep using the
  lifecycle callbacks and do not move to interceptors.
- Interceptors bring no retry, hedging or OAuth framework.

All 0.x releases are experimental. This design adds experimental API; the
supported contract is settled later in
[#27](https://github.com/jimeh/connectrpc-ruby/issues/27).

## Current dispatch map

### Server

Every host reaches handlers through `Server`. No host calls a handler without
it. Handler execution has six call sites:

| Shape and protocol | Call site | Reaches |
| --- | --- | --- |
| Connect unary POST | `lib/connectrpc/server.rb:272` | `@invoker` |
| Connect unary GET | `lib/connectrpc/server.rb:206` | `@invoker` |
| Native gRPC unary | `lib/connectrpc/grpc_server.rb:89` | `@invoker` |
| gRPC-Web unary | `lib/connectrpc/grpc_web_server.rb:144` | `@invoker` |
| Connect streams and native gRPC streams | `invoke_stream_handler`, `lib/connectrpc/streaming/server_execution.rb:60-79` | handler |
| gRPC-Web server streaming | `lib/connectrpc/grpc_web_server.rb:146-148` | handler, called directly |

The default invoker is `Server#invoke_handler`
(`lib/connectrpc/server.rb:252-258`). The Rails gem supplies its own invoker,
which runs Rails' `process_action` chain, and its `connect` action calls the
handler itself (`gems/connectrpc-on-rails/lib/connectrpc/on_rails/controller.rb:9-16`).
A server chain that wraps `@invoker` once, at construction, covers all four
unary sites and Rails. A chain inside `invoke_handler` would miss Rails.

Hosts enter `Server` like this:

| Host | Entry | Shapes |
| --- | --- | --- |
| `connectrpc-rack` | `start_call` then `call` (`gems/connectrpc-rack/lib/connectrpc/rack.rb:30-32`) | Unary Connect POST/GET, unary gRPC-Web |
| `connectrpc-on-rails` | A new `Server` per request with the invoker block, then Rack's `transfer_response` (`controller.rb:43-53`) | Unary Connect, unary gRPC-Web |
| `connectrpc-async` Host | `ServerSession` starts the lifecycle, then `Application` calls `call`, `call_stream`, `call_grpc` or `call_grpc_web` in a per-request task (`gems/connectrpc-async/lib/connectrpc/async/application.rb:68`, `:154`) | All four, Connect, gRPC and gRPC-Web |
| `connectrpc-falcon` | The same `Application` and `ServerSession`, with Falcon workers and a local HTTP/1 resource | All four over H2; Connect unary, SS and CS over HTTP/1; unary and server-streaming gRPC-Web over both |
| `connectrpc-testing` | `Server#call` from an in-process `_Transport` (`gems/connectrpc-testing/lib/connectrpc/testing/transport.rb:27-33`) | Unary |
| `connectrpc-health` | An application `Server` with the generated handler map and no invoker | Check and List unary, Watch server streaming |

### Client

Generated clients hold `client:` and forward each RPC unchanged:
`@client.call(METHOD, request, **options)` and
`@client.server_stream(METHOD, request, **options, &)`, with the same pattern
for client and bidi streaming (`lib/connectrpc/generator/renderer.rb:79-97`,
`lib/connectrpc/generator/streaming_methods.rb:19-22`). A chain on
`ConnectRPC::Client` therefore applies to generated clients and to every
transport (NetHTTP, Async `Transport`, `HTTP1Transport`, `ThreadedTransport`,
the testing transport and custom transports) with no generated-code change.

`Client#call` has three paths: an uninstrumented `process_call`, an
instrumented wrapper around it (`lib/connectrpc/client.rb:96-128`), and unary
gRPC, which already runs through the streaming `stream_call` with a
single-request scope (`lib/connectrpc/client.rb:49-53`). Streaming calls run
through `stream_call`, `ClientScope` and the transport's `open_stream` or
`open_grpc_stream` (`lib/connectrpc/streaming/client.rb:51-80`).

## Comparison with connect-go and gRPC

| Concern | connect-go v1 (`connectrpc.com/connect`, v1.21.1) | connect-go v2 (`connectrpc.com/connect/v2`) | grpc-go | This design |
| --- | --- | --- | --- | --- |
| Shape | `Interceptor` with `WrapUnary(UnaryFunc) UnaryFunc`, `WrapStreamingClient`, `WrapStreamingHandler`; `UnaryInterceptorFunc` for unary only | `ClientInterceptor func(next ClientFunc) ClientFunc` and `ServerInterceptor func(next ServerFunc) ServerFunc`; every RPC is a stream | Separate unary and stream interceptors, separate client and server types | One object with four optional role-specific methods |
| Composition | Factories built once per client or handler | Built once in `NewClient` / `NewServer` | Built once per channel or server | Discovered once; a per-RPC guarded continuation |
| Order | "The first interceptor provided is the outermost layer of the onion" | "The first wraps the outermost call" | The first is the outermost | The first is the outermost |
| Server position | Unary interceptors run after unmarshaling | Before Connect reads or decodes the request | After decode (unary) | After bounded read and decode |
| Streams | Wrap the returned or passed conn | Wrap the returned or passed stream | Wrap `ClientStream` / `ServerStream` | `call.wrap_stream` around scoped endpoints |
| Calling `next` twice | Possible | Not addressed | Possible (retry interceptors) | Rejected |

Sources: the [connect-go v1 API](https://pkg.go.dev/connectrpc.com/connect),
the [connect-go v2 API](https://pkg.go.dev/connectrpc.com/connect/v2), the
[Connect interceptors guide](https://connectrpc.com/docs/go/interceptors/) and
the [gRPC interceptors guide](https://grpc.io/docs/guides/interceptors/).

Ruby differs in six places, each for a concrete reason:

1. **Role-specific methods instead of a shared `WrapUnary`.** connect-go v1
   uses one `WrapUnary` for both roles and distinguishes them with
   `Spec().IsClient`. In Ruby the two roles have different objects: the server
   has a `Context` with response metadata and cancellation listeners, and the
   client has none. Separate methods give each role a precise RBS type and let
   construction reject a server-only interceptor on a client.
2. **A per-RPC continuation instead of factory composition.** A Go
   interceptor holds `next` and could call it at any time. Ruby block scopes
   make the stack the natural boundary. Core can then check that the
   continuation runs once, on the owning fiber, while the interceptor runs, as
   #32 requires.
3. **No repeated continuation.** connect-go and grpc-go allow an interceptor
   to call `next` more than once, which is how their retry interceptors work.
   Here a second call raises `InterceptorStateError`. Exactly-once cleanup and
   one arbitration owner per call depend on one dispatch per call, and the
   settled direction excludes retry from interceptors. The Async H2 transport
   already owns the one safe replay (refused streams), below interceptors.
4. **Unary stays typed and post-decode.** connect-go v2 moved server
   interceptors before decode so authentication can reject without decoding.
   This design keeps connect-go v1's placement: Ruby handlers are typed
   `(request, context)` callables, typed validation is a primary use, and
   decode is already bounded by the request limits. The Rails gem already
   decodes before controller callbacks. Applications that must reject before
   decode use host middleware, which is also what the Connect guide recommends
   for v2 ("standard `net/http` middleware").
5. **Scoped stream wrappers.** Ruby streams are scoped block APIs, not
   returned conns. A client stream does not exist until the innermost layer
   opens it, after request headers are final, so a layer registers a wrapper
   instead of returning one. See [Streaming model](#streaming-model).
6. **Server-streaming interceptors run after the request is read.**
   connect-go v1's `WrapStreamingHandler` and grpc-go stream interceptors run
   before any message is read. Here core reads the single server-streaming
   request and checks there is exactly one before the chain, so the
   interceptor sees it as `call.request`, as a unary interceptor does. Client
   and bidi streaming still run the chain before any message is read. Early
   rejection of a server-streaming call therefore costs one bounded decode,
   as it does for unary.

## API

### Interceptor objects

An interceptor is any object with one or more of these public methods:

```ruby
def client_unary(call)  # call: ConnectRPC::ClientUnaryCall
  yield                 # -> ConnectRPC::Response
end

def client_stream(call) # call: ConnectRPC::ClientStreamCall
  yield                 # -> scope result
end

def server_unary(call)  # call: ConnectRPC::ServerUnaryCall
  yield                 # -> response message
end

def server_stream(call) # call: ConnectRPC::ServerStreamCall
  yield                 # -> scope result
end
```

Registration:

```ruby
client = ConnectRPC::Client.new(base_url:, binding:, interceptors: [Auth.new, Validate.new])
server = ConnectRPC::Server.new(services:, binding:, handlers:, interceptors: [Auth.new, Validate.new])
```

- `interceptors:` defaults to `[]`. Core copies and freezes the list.
- At construction, core discovers each element's methods for that role once,
  in the same way instrumentation discovers optional callbacks
  (`lib/connectrpc/instrumentation.rb:15-29`). Each RPC then runs only the
  layers that have a method for its role and kind.
- An element with no method for the role raises `ArgumentError`. A server-only
  interceptor passed to a client is therefore rejected at construction, not
  silently ignored.
- With no interceptors for a role and kind, dispatch takes today's path with
  no per-call allocation.
- Interceptor objects are shared by every concurrent RPC, like handlers. State
  that belongs to one RPC lives in its call object or stream wrapper.
  Application state shared across RPCs needs its own locking.

The common case needs one method body. An authentication interceptor that
covers all four client shapes can alias its methods, because `yield` returns
the scope result in both:

```ruby
class BearerAuth
  def initialize(tokens) = @tokens = tokens

  def client_unary(call)
    call.request_metadata["authorization"] = "Bearer #{@tokens.current}"
    yield
  end
  alias client_stream client_unary
end
```

A server-side check rejects without reaching the handler:

```ruby
class RequireTenant
  def server_unary(call)
    tenant = call.context.request_metadata["x-tenant"]
    raise ConnectRPC::Error.new(:unauthenticated, "Missing tenant") unless tenant

    yield
  end
  alias server_stream server_unary
end
```

### Call objects

Each RPC gets one call object per role, shared by every layer of that RPC.
Fields that can change are writable only while the call is open, that is,
until the innermost continuation starts dispatch. Afterward a write raises
`InterceptorStateError`, or `FrozenError` for frozen metadata.

| Class | Fields |
| --- | --- |
| `ClientUnaryCall` | `method` (unary `Method`), `protocol` (`:connect` or `:grpc`), `request` and `request=` (open only), `request_metadata` (`Metadata`, mutable while open), `deadline` (`Deadline` or nil), `cancellation` (read-only `_Cancellation`) |
| `ClientStreamCall` | `method` (streaming declaration), `protocol`, `request_metadata`, `deadline`, `cancellation`, `wrap_stream { \|stream\| ... }` (open only) |
| `ClientStreamCall::ServerStreaming` | A `ClientStreamCall` subclass for server streaming that adds `request` and `request=` (open only) |
| `ServerUnaryCall` | `method`, `protocol` (`:connect`, `:grpc` or `:grpc_web`), `request` and `request=` (open only), `context` (`Context`) |
| `ServerStreamCall` | `method`, `protocol`, `context` (`StreamingContext`), `wrap_stream { \|stream\| ... }` (open only) |
| `ServerStreamCall::ServerStreaming` | A `ServerStreamCall` subclass for server streaming that adds `request` and `request=` (open only) |

- Client and bidi streaming get direct `ClientStreamCall` and
  `ServerStreamCall` instances; server streaming gets the subclass. An
  interceptor that needs the request narrows with
  `case call when ConnectRPC::ServerStreamCall::ServerStreaming`, which Steep
  understands, so `request=` on a client-streaming call is a static error
  rather than a runtime surprise.
- `request=` replaces the message passed to inner layers and to dispatch.
  Core does not type-check the replacement; the binding validates it at encode
  on the client, and the handler receives it on the server.
- `method.cardinality` also tells a stream interceptor which shape it wraps.
- `ServerUnaryCall#protocol` is not derivable from the invoker's
  `(method, request, context)` arguments, so the four unary call sites pass
  the negotiated protocol to the private chain entry. The public invoker
  signature does not change.
- Call objects carry no transport, Rack or lifecycle objects. Interceptors
  never see `TransportRequest`, encoded bytes, Rack envs or instrumentation
  handles.

### Continuation rules

Core passes each layer a block that runs the next layer, or dispatch for the
innermost layer. The block is backed by a core-owned continuation that
enforces these rules and raises `InterceptorStateError` for a violation:

1. **Once.** A second call raises. Retries are not expressible.
2. **During the interceptor.** The continuation can run only while that
   layer's method is on the stack. Calling a captured block after the method
   returned raises, so an escaped continuation cannot dispatch late.
3. **On the owning fiber.** The continuation records `Fiber.current` when the
   layer starts and rejects any other fiber or thread. An interceptor cannot
   move dispatch into a new thread, an `Async` child task or a fiber of its
   own. Server processing therefore stays in the original processing task,
   with its lifecycle activation, fiber-local state and the Async package's
   exact-task peer accessor. Client dispatch stays on the caller's fiber,
   matching the stream session's thread affinity
   (`lib/connectrpc/streaming/session.rb:113-118`).
4. **Client unary may skip it.** A client unary interceptor that returns
   without yielding short-circuits successfully and must return a
   `ConnectRPC::Response`, for example from a local cache. A value that is not
   a `Response` raises `InterceptorStateError`.
5. **Everything else must use it or raise.** A server interceptor, or a
   stream interceptor on either role, that returns without yielding and
   without raising raises `InterceptorStateError`. On the server, the invoker
   is the host's handler-execution boundary and can carry host policy: the
   Rails invoker is the only path into Rails authentication, CSRF and
   controller callbacks. A successful server short-circuit would bypass that
   policy, which #32 forbids. A stream short-circuit would report success for
   a handler or application block that never ran.

`InterceptorStateError < StandardError` is a programming error. On the client
it propagates to the caller with its identity, like any other
interceptor-raised exception, so an accepted cancellation never masks misuse.
On the server it is recorded and becomes `internal`, like any other
unexpected handler exception.

## Ordering

The first interceptor listed is the outermost on both roles. It acts first on
the call and request and last on the response or error.

### Client

```text
Client#call / #server_stream / #client_stream / #bidi_stream
  1. argument validation: Method type, cancellation type, transport capability
  2. lifecycle start, begin_processing, activate (observers attach)
  3. Deadline and CallCancellation start
  4. caller metadata validated into Metadata
  5. interceptor chain, outermost first
     6. innermost continuation:
        a. request metadata frozen
        b. propagation carrier injected (#73 merge rule)
        c. lifecycle request metadata recorded as sent
        d. encode, compress, GET selection, transport, decode    (unary)
           or open, wrap stream, application block, finish scope (streams)
  7. arbitration commit after the outermost interceptor returns
  8. processing_finished, finish (observers detach)
```

- **Validation first.** `ArgumentError` for a wrong declaration type or
  cancellation type is raised before any interceptor or lifecycle starts, as
  today.
- **Instrumentation outside.** Processing duration includes interceptor time,
  so metrics' handling duration measures what the caller experiences. OTel and
  Sentry spans are active while interceptors run, so a token fetch inside an
  interceptor nests under the RPC span and logs carry its trace context. A
  short-circuit is recorded as an outcome with its `Error` code.
- **Deadline before interceptors.** The client deadline begins before the
  outermost interceptor, so interceptor time counts against the timeout. Core
  computes `connect-timeout-ms` and `grpc-timeout` at dispatch, as it does
  now, so the wire timeout reflects the remaining budget. Today the deadline
  starts inside `process_call` (`lib/connectrpc/client.rb:65`); PR A moves its
  creation before the chain without changing its semantics.
- **Injection after interceptors.** Instrumentation's staged carrier is merged
  into the final metadata at dispatch. Interceptors therefore cannot read or
  overwrite injected propagation headers. They can read the active trace
  context from the observer libraries. Interceptor-added metadata is "caller
  metadata" for #73's merge rule.
- **Lifecycle metadata.** The lifecycle records the validated caller metadata
  before the chain, then replaces it with the metadata actually sent at
  dispatch. A short-circuit keeps the caller metadata.
- **Messages.** `message_sent` and `message_received` still fire at encode
  and decode, not when an interceptor sees or replaces a message.
- **Commit at the outer boundary.** The call's terminal arbitration commits
  once, after the outermost interceptor returns or raises. A cancellation or
  expiry that is observed during an interceptor's post-processing wins, as it
  would during decode today.
- **Native gRPC unary owns a stream scope.** Unary gRPC runs through
  `stream_call` and `ClientScope` (`lib/connectrpc/client.rb:49-53`), so its
  lifecycle completion and commit belong to `ClientScope`, not to
  `process_call`. The `client_unary` chain runs inside `ClientScope#run` and
  outside `open_grpc_stream`, and the innermost continuation runs today's
  single-request scope and returns its `Response`. `ClientScope` therefore has
  to accept the arbiter created before the chain, and accept an interceptor's
  returned `Response` as the scope result. Today `ClientScope#execute` returns
  `@result` and commits only when `perform_stream_scope` set `@completed`
  (`lib/connectrpc/streaming/client_scope.rb:25-29`), so a skipped
  continuation would return nil and be reported as an aborted failure. PR A
  changes that: a short-circuit or fallback `Response` completes the scope,
  commits the arbiter and reports `OK` with `delivery: :unobserved`, because
  no exchange existed, matching what the Connect unary path reports.

### Server

```text
Host admission and host middleware (Rack, Rails routing, Async/Falcon session)
  1. Server#start_call, begin_processing, activate
  2. protocol negotiation, 405/415, CORS preflight
     (Rack and gRPC-Web answer CORS preflight, and native gRPC answers its
      405/415 preflight, before step 1)
  3. header validation, Context with deadline and cancellation
  4. bounded read, inflate, decode, cancellation check, message_received
     (streams: StreamingContext and session; server streaming also reads
      the single request and checks there is exactly one)
  5. interceptor chain, outermost first
     6. innermost continuation: @invoker (unary) or stream handler
  7. encode, compress, limit check, commit, message_sent (unary)
     or terminal response and trailers (streams)
  8. processing_finished
```

- Interceptors do not run for bare 405 or 415 responses, CORS preflight,
  undeclared methods, invalid timeouts, header validation failures, or read,
  inflate, decode and limit failures. Those responses come from core before
  any application code, as today.
- Interceptors run for a declared unary method even when it has no handler,
  because the invoker owns that check: a declared-but-unimplemented unary
  method returns the inner `unimplemented` error through the chain, and native
  gRPC unary with a custom invoker and no handler map entry keeps working. A
  streaming method without a handler is rejected before the chain, as
  `streaming_declaration` does today.
- The request deadline starts when headers are parsed, before decode, so
  interceptor time counts against it. Interceptors observe it through
  `context.deadline`, `cancelled?`, `check!` and `on_cancel`.
- Server processing duration already covers header validation, read, decode,
  handler and encode for unary calls; it now also covers interceptors. For
  streams, the window ends at the terminal observation
  (`lib/connectrpc/streaming/server_terminal.rb`), unchanged.
- Response encoding happens after the chain returns, so interceptors can add
  response metadata and trailers after `yield` on unary calls.

### Rails

The Rails gem's invoker is the innermost layer. Server interceptors therefore
run inside the controller instance's `process_action` override but before
Rails' own `process_action` chain: before `before_action` callbacks, CSRF
verification, authentication callbacks, `rescue_from` and the
`process_action.action_controller` notification.

- Host policy does not move. Rails authentication, CSRF and session handling
  still run for every call that succeeds, and an interceptor cannot bypass
  them: a server interceptor must yield or raise (continuation rule 5), so the
  only way to a successful response is through the invoker and Rails' full
  `process_action` chain. An interceptor can only reject earlier or add
  behavior around that chain.
- A rejecting interceptor skips the Rails callbacks, `rescue_from` and the
  Rails instrumentation event, the same as a core preflight failure today
  (`gems/connectrpc-on-rails/README.md:161-163`).
- Interceptors get no controller state, such as `current_user`. Policy that
  needs Rails state stays in Rails callbacks.
- Interceptor exceptions do not pass through `rescue_from` or
  `Rails.error.report`. Core records them for observers when the call fails,
  so OTel and Sentry still receive the original.
- An exception from inside the controller reaches interceptors as the
  normalized `Error` that the Rails invoker raises. The invoker calls
  `Context#record_failure` with the original first (`controller.rb:80-92`).
  Inside the chain that call is staged like every other boundary failure (see
  [Errors](#errors)), so observers still receive the original when the call
  fails.

## Unary semantics

### Short-circuits

An interceptor short-circuits by not yielding.

- **Rejection.** Raise a `ConnectRPC::Error`. On the client, the transport is
  never called, `message_sent` never fires, and the caller receives that
  `Error`. On the server, the handler never runs and the client receives the
  error's code, message, details, metadata and trailers. The response also
  carries response metadata and trailers that interceptors set on the context
  before raising, merged as `error_response` does today
  (`lib/connectrpc/server.rb:380-401`).
- **Successful short-circuit.** Only on the client: return a `Response`, for
  example from a local cache. Core freezes its metadata and trailers like any
  other result. A server interceptor that returns without yielding raises
  `InterceptorStateError` (continuation rule 5).

### Errors

- **Server mapping.** An `Error` raised anywhere in the chain becomes a wire
  error. Any other `StandardError` becomes a generic `internal` error with a
  safe message, exactly like a handler exception. `Exception`s that are not
  `StandardError` propagate with today's cleanup.
- **Recording.** Core stages the exception escaping each continuation
  boundary in a per-call slot before the outer interceptor sees it, keeping
  the first. `Context#record_failure` calls made inside the chain, such as the
  Rails invoker's, go to the same slot. When the call fails, core records the
  staged exception with `record_failure` before its own boundary failure, so
  lifecycle observers receive the original exception even after an
  interceptor rescued it and raised a translated `Error`. An interceptor that
  raises its own exception with no inner failure has that exception recorded.
  When an interceptor recovers and the call succeeds, core discards the slot,
  so the outcome is `OK` with no error. Recording it directly would make OTel
  report the successful call as `operation_failed`. This matches
  `docs/design.md`'s rule that `record_failure` retains the first *escaping*
  failure. Failures that core already records outside the chain, such as
  codec and stream session operation failures, keep today's direct recording.
- **Sentry.** Retaining the original does not change Sentry's capture rule.
  The subscriber captures only a non-`Error` original whose normalized code is
  `internal`, `unknown` or absent
  (`gems/connectrpc-sentry/lib/connectrpc/sentry/subscriber.rb:128-136`). A
  handler bug that an interceptor translates to `internal` is still captured;
  one translated to `invalid_argument` or `not_found` is not, as for any other
  deliberate `Error`.
- **Client identity.** The client never normalizes application exceptions, and
  that stays true. An interceptor-raised exception, including `ArgumentError`
  from writing invalid metadata, reaches the caller with the same identity and
  backtrace. Core distinguishes core-owned failures (transport, codec and
  arbitration errors raised by the innermost continuation) from
  interceptor-raised exceptions by identity, the same way a stream session
  distinguishes its own errors (`owned_error?`).
- **Translation and fallback.** A client interceptor may rescue a core error
  and raise a different exception, or return a fallback `Response`. A server
  interceptor may rescue a handler exception and raise an `Error`, or return a
  response message after the handler ran. The invoker has then run, so host
  policy has already applied. Arbitration still applies; see
  [Cancellation and deadlines](#cancellation-and-deadlines).
- **Nil responses.** A server unary interceptor that returns nil follows the
  handler-returned-nil path. On Connect unary the binding rejects it at encode
  and the call fails `internal`; native gRPC and gRPC-Web unary send through
  the stream session, which today maps nil to `invalid_argument`
  (`lib/connectrpc/streaming/session_messages.rb:113-115`). L6 settles that
  attribution, and PR A follows it.
- **What interceptors see.** Interceptors see the normalized `Error` that core
  or an inner layer raised. Its `cause` is whatever L1 settles; an interceptor
  must not rely on `cause` to reach the original. Originals are available only
  to lifecycle observers.

### Metadata

| Metadata | Client | Server |
| --- | --- | --- |
| Request | Mutable `Metadata` on the call while it is open; validated on write; frozen at dispatch | Read-only `context.request_metadata`, a snapshot taken before the chain |
| Response leading | Frozen snapshot in `Response#metadata` | Mutable `context.response_metadata` until headers are published |
| Response trailing | Frozen snapshot in `Response#trailers` | Mutable `context.response_trailers` until the terminal response |
| Error | `Error#metadata` and `#trailers` | Merged into the response, as today |

- **Client request.** `Metadata` already validates names and values and
  rejects protocol and framing headers (`lib/connectrpc/metadata.rb`). A later
  interceptor sees earlier interceptors' writes. Writes after dispatch raise
  `FrozenError`.
- **Client response.** Core freezes `Response#metadata` and `#trailers` when
  the innermost continuation returns, and again at the outermost boundary, so
  a replacement or cached `Response` built by an interceptor also reaches the
  caller frozen. To change what the application sees, an interceptor returns a
  new `Response`. Native gRPC and streaming responses are already frozen
  (`lib/connectrpc/grpc.rb:81`, `lib/connectrpc/streaming/session_io.rb:84`);
  Connect unary responses are mutable today, and freezing them is part of
  PR A.
- **Server request.** An authentication interceptor's check must not be undone
  by an inner layer that rewrites request metadata. Request metadata is
  therefore a read-only snapshot for interceptors and handlers on every
  protocol. Today it is mutable on Connect; L9 owns the request metadata
  freezing contract, and this design depends on it settling on a frozen
  snapshot taken before application code runs.
- **Server response.** For unary calls on every protocol, headers are built
  after the chain returns, so writes before and after `yield` both work. On
  native gRPC and gRPC-Web unary, core freezes response metadata when it
  publishes the head, which happens after the chain returns
  (`lib/connectrpc/grpc_server.rb:113-124`). For streams, see
  [Headers and trailers on streams](#headers-and-trailers-on-streams).

## Streaming model

### Scope and wrappers

`client_stream` and `server_stream` wrap the whole stream scope. Code after
`yield` runs after the scope has finished: after the application block and
`finish_stream_scope` on the client, and after the handler on the server. It
sees the scope's result or exception.

`call.wrap_stream { |stream| ... }` registers a wrapper for this layer while
the call is open. The block receives the inner endpoint and returns an object
that the next outer layer, and finally the application, receives instead. The
endpoint types are the existing scoped ones:

| Shape | Client endpoint | Server endpoint |
| --- | --- | --- |
| Server streaming | `ResponseReader` | `ResponseWriter` (request in `call.request`) |
| Client streaming | `RequestWriter` | `RequestReader` |
| Bidi | `ClientDuplex` | `ServerDuplex` |

```ruby
class ValidateMessages
  def initialize(validator) = @validator = validator

  def server_stream(call)
    case call
    when ConnectRPC::ServerStreamCall::ServerStreaming then @validator.check!(call.request)
    end
    call.wrap_stream { |stream| ValidatingStream.new(stream, @validator) }
    yield
  end
end

class ValidatingStream < SimpleDelegator
  def initialize(stream, validator)
    super(stream)
    @validator = validator
  end

  def send_message(message)
    @validator.check!(message)
    __getobj__.send_message(message)
  end
end
```

- **Order.** Core applies wrappers innermost first. The application block or
  handler receives `outer(inner(raw))`, and a `send_message` from the
  application passes through the outer wrapper first. This matches
  connect-go's "the first interceptor provided is the outermost wrapper for
  the StreamingClientConn or StreamingHandlerConn".
- **When.** On the server, the endpoint exists before the chain runs, so core
  builds the wrapped endpoint when the innermost continuation starts. On the
  client, request headers must be final before the stream opens, so layers
  register wrappers while the call is open, the innermost continuation opens
  the stream, and core applies the wrappers before it yields to the
  application block.
- **Per RPC.** Each `wrap_stream` block runs once per RPC and creates a fresh
  wrapper, so wrapper state is isolated between concurrent streams.
- **Delegation.** A wrapper should delegate every method it does not change.
  Core calls only the endpoint methods the application could call; it never
  relies on a wrapper for protocol work. When core applies a wrapper, it checks
  that the returned object responds to every public method of the endpoint it
  replaces, and raises `InterceptorStateError` otherwise, so a wrapper that
  drops `abandon` or `each` fails at wrap time instead of inside application
  code.
- **Typing.** `wrap_stream` takes a block typed `(untyped) -> untyped`, and
  application blocks and handlers keep their nominal endpoint types
  (`ResponseReader[R]` and so on). Wrappers are therefore duck-typed against
  those classes, backed by the runtime check above. Structural endpoint
  interfaces that would let wrappers be checked statically are
  [open question 7](#open-questions).
- **Wrapper failures.** A `wrap_stream` block or wrapper method that raises is
  an interceptor exception. It propagates through the chain like any other,
  and core cleanup still runs on the raw session.

### Cleanup and misuse

- **Core owns cleanup.** Abandonment, `close_send`, session close, terminal
  rendering and lifecycle completion run in core's existing `ensure` paths on
  the raw session and context, never through wrappers. A wrapper cannot skip,
  repeat or reorder them. Cleanup runs exactly once whether the application,
  a wrapper or an interceptor raised.
- **Escaped wrappers.** A wrapper retained after its scope delegates to a
  closed session, which already raises `StreamStateError`
  ("Stream scope is closed"). Thread and reactor affinity is enforced by the
  session (`lib/connectrpc/streaming/session.rb:113-118`) and by
  `StreamingContext#send_headers`, whichever wrapper the call passes through.
- **Repeated `close_send`.** `Session#close_send` already ignores a second
  call (`lib/connectrpc/streaming/session_messages.rb:39-47`), so a wrapper
  that calls it twice is harmless. The double call L5 describes is different:
  stream completion calls the exchange's `close_send` unconditionally
  (`lib/connectrpc/streaming/session_completion.rb:108-112`), even after an
  explicit `close_send` from the application or a wrapper. Wrappers make that
  sequence more common without adding a new exchange-level caller. L5's
  core-owned guard makes "wrapper `close_send`, then core completion" reach
  the exchange once.
- **Overlapping operations.** The session's per-direction guards still reject
  overlapping reads or writes, including a wrapper that issues an extra read.

### Results and identity

- **Result.** `yield` returns the scope result. For client streaming that is
  the `Response` on the client and the handler's response message on the
  server; for server streaming and bidi it is nil. The value an interceptor
  returns becomes the client-streaming result: the client returns it to the
  caller, which must be a `Response`, and the server sends it as the single
  response, with nil handled as L6 settles. For server streaming and bidi,
  core ignores interceptor return values, as it ignores handler return values
  today. The innermost server continuation therefore returns the
  client-streaming handler's response instead of sending it; core sends the
  value the outermost interceptor returns.
- **Client user exceptions.** `ClientScope` keeps an exception from the
  application block as an independent user exception and re-raises it over
  cancellation and cleanup errors
  (`lib/connectrpc/streaming/client_scope.rb:46-58`). With interceptors, the
  boundary that matters is the outermost interceptor:
  - An exception that escapes the outermost interceptor and that the session
    does not own is an independent user exception. If an interceptor
    translated the application's exception, the translation wins.
  - An interceptor cannot turn a failed scope into success. If the
    application block raised and an interceptor returns normally, core still
    raises the original application exception, because cleanup already
    abandoned the stream.
  - Session-owned errors keep today's arbitration and delivery rules.
- **Server.** Stream errors from interceptors follow the handler's path:
  `capture_stream_error` records them, maps them to a safe `Error` and renders
  the terminal (`lib/connectrpc/streaming/server.rb:113-120`).

### Headers and trailers on streams

- Server response metadata is mutable until headers are published:
  explicitly by `send_headers`, or implicitly by the first message or the
  terminal response. Writes after publication raise, because core deeply
  freezes leading metadata at publication. An interceptor that needs to add
  headers must do it before `yield`, or from a wrapper before the first
  `send_message`.
- Trailers stay mutable until terminal serialization, so interceptors can add
  trailers after `yield`, including after a handler error.
- Client server-streaming and bidi wrappers read response headers and
  trailers through the endpoint's `headers` and `trailers`, which return the
  session's frozen snapshots. A client-streaming endpoint (`RequestWriter`)
  has no response metadata methods (`lib/connectrpc/streaming/streams.rb:80-88`);
  a client-streaming interceptor reads them from the `Response` that `yield`
  returns.

## Cancellation and deadlines

- **One arbiter per call.** Each call keeps exactly one terminal arbitration
  owner: `CallCancellation` on the client and the `Context` on the server.
  Interceptors receive only read-only views: `call.cancellation` on the client
  and the `Context` methods on the server (`cancelled?`, `cancellation_reason`,
  `check!`, `on_cancel`). The client view has no completion methods at all.
  The client view is the same read-only `_Cancellation` view that L2
  introduces for transports. On the server, `call.context` is the same
  `Context` that handlers receive. Its `commit!` and `close` are public at
  runtime (`lib/connectrpc/deadline.rb:84-85`) but absent from its RBS, so
  calling them is outside the typed contract and unsupported for interceptors,
  as it already is for handlers.
- **Accepted reasons never flip.** Once the arbiter accepts expiry or
  cancellation, the call ends with that reason:
  - On the server, the terminal error is the accepted reason, whatever an
    interceptor raised or returned, as `terminal_error` does for handlers today
    (`lib/connectrpc/server.rb:369-378`). Observers still receive the original
    exception.
  - On the client, a core-owned failure is replaced by the accepted reason, as
    today. An interceptor-raised exception is an independent user exception: it
    reaches the caller with its identity, and the arbiter still records the
    accepted terminal for observers. A successful return after an accepted
    reason becomes that reason's `Error`.
- **No deadline changes.** Interceptors cannot extend or shorten the deadline;
  it is fixed when the call starts. Shortening is an
  [open question](#open-questions).
- **Cooperative blocking.** Core cannot interrupt an interceptor. An
  interceptor that does I/O, such as fetching a token, should observe
  `call.deadline`, `call.cancellation` or `context.on_cancel` itself. The
  NetHTTP watchdog only covers its own socket, and Async peer `RST_STREAM`
  only signals the server context.
- **Shared sources.** A caller's `CancellationSource` can be shared across
  calls. An interceptor never completes it, and cancelling it still affects
  each call through that call's own arbiter.

## Host coverage

| Host | Unary | Streaming | Change needed |
| --- | --- | --- | --- |
| Core `Client`, any transport | PR A | PR B | None outside core |
| Generated clients | Through `Client` | Through `Client` | None |
| `ThreadedTransport` | Connect and native gRPC unary on the caller thread | Server streaming on the caller thread | None; interceptors and wrappers run on the caller thread, where core already encodes and the stream session lives |
| `connectrpc-rack` | PR A | Not supported by the host | None |
| `connectrpc-on-rails` | PR C | Not supported by the host | Private `connect_interceptors` hook |
| `connectrpc-async` Host | PR A | PR B | None; interceptors run in the per-request processing task |
| `connectrpc-falcon` | PR A | PR B | None |
| `connectrpc-testing` | PR A, both chains in process | Not supported by the transport | None |
| `connectrpc-health` | Check and List, PR A | Watch, PR B | None; the application's `Server` carries the interceptors |
| Custom transports and bindings | Unchanged SPI | Unchanged SPI | None; interceptors run above encode |
| Custom invokers | The innermost unary layer | Not applicable | None |

- **Rails hook.** Rails builds a `Server` per request from private hooks
  (`controller.rb:43-49`). PR C adds `connect_interceptors`, defaulting to `[]`,
  next to `connect_instrumentation`. The existing hooks are split:
  `connect_protocol`, `connect_status_codec` and `connect_cors` are
  `supported`, while hooks that hand core extension objects, such as
  `connect_instrumentation`, `connect_handlers` and `connect_limits`, are
  `spi` (`typecheck/surface.json`). `connect_interceptors` hands core
  extension objects too, so it is `spi`. Because it is read per request, it
  should return a frozen list built once.
- **Invoker.** The invoker contract `(method, request, context)` does not
  change. Interceptors wrap it, which also gives streaming a policy seam
  without extending the invoker to streams.
- **Health Watch.** Watch is long-lived. An interceptor's `server_stream`
  frame, and anything it holds, lasts as long as the Watch stream.
- **Host facts.** Server interceptors do not get Rack envs or peer addresses.
  Within the Async and Falcon hosts they run in the exact processing task, so
  the existing task-scoped client certificate accessor works there. A portable
  peer field is an [open question](#open-questions).

## Static typing

RBS ships with the implementing PRs, following the [RBS guidance](../rbs.md):

```rbs
module ConnectRPC
  interface _ClientUnaryInterceptor
    def client_unary: (ClientUnaryCall call) { () -> Response[untyped] } -> Response[untyped]
  end

  interface _ClientStreamInterceptor
    def client_stream: (ClientStreamCall call) { () -> untyped } -> untyped
  end

  interface _ServerUnaryInterceptor
    def server_unary: (ServerUnaryCall call) { () -> untyped } -> untyped
  end

  interface _ServerStreamInterceptor
    def server_stream: (ServerStreamCall call) { () -> untyped } -> untyped
  end

  type client_interceptor = _ClientUnaryInterceptor | _ClientStreamInterceptor
  type server_interceptor = _ServerUnaryInterceptor | _ServerStreamInterceptor
end
```

- `Client#initialize` gains `?interceptors: Array[client_interceptor]`, and
  `Server#initialize` gains `?interceptors: Array[server_interceptor]`.
- Message types are `untyped` because one interceptor serves many methods,
  like the existing `_Invoker`. The block types are precise: a continuation
  takes no arguments, and a client unary interceptor must return a `Response`.
- Only stream calls define `wrap_stream`. Only `ClientUnaryCall`,
  `ServerUnaryCall` and the two `ServerStreaming` subclasses define `request`
  and `request=`; the stream interfaces take the base class, and an
  interceptor narrows to the subclass with `case`/`when`.
- **Registration is checked per member, not per method.** A union of
  interfaces accepts an object that satisfies any one member, so an object
  with a valid `client_stream` and a `client_unary` that returns a `String`
  still type-checks at `Client.new`. The precise check comes from the
  interceptor's own signature: a class whose RBS declares
  `include ConnectRPC::_ClientUnaryInterceptor` is checked against that
  interface when Steep checks the class. The runtime check on rule 4 covers
  unsigned interceptors.
- Inventory classification: the `interceptors:` keywords and call classes are
  `supported`; the four interfaces are `spi`; continuation internals are
  `internal`.
- Static negatives in `typecheck/api/negative/` with exact diagnostics in
  `negatives.json`:
  - a server-only interceptor passed to `Client.new`;
  - a signed class including `_ClientUnaryInterceptor` whose `client_unary`
    returns a non-`Response`, including a class that also defines a valid
    `client_stream`;
  - a block continuation called with an argument, from a signed interceptor
    class;
  - `wrap_stream` called on a unary call;
  - `request=` called on a `ClientStreamCall` without narrowing.

  A matching positive consumer narrows to
  `ClientStreamCall::ServerStreaming` and assigns `request=`.

## Dependencies on #26 and the lifecycle metadata work

| Group | What it changes | How interceptors depend on it | Order |
| --- | --- | --- | --- |
| L2: unary transport receives the mutable cancellation owner | Gives transports a read-only `_Cancellation` view instead of `CallCancellation` with `commit!` and `close` | Interceptors need the same read-only view on `ClientUnaryCall` and `ClientStreamCall`. Building it twice would leave two read-only shapes. | Before PR A |
| L1: normalization retains rescued causes | Decides which normalized errors suppress `cause` | Interceptors see normalized errors. The contract already forbids relying on `cause`, so this is a soft dependency: PR A can proceed first if its specs assert no `cause` behavior. | Preferably before PR A |
| L3: binding `ArgumentError` classified as malformed response metadata | Fixes the client's classification boundary | Client interceptors see the misclassified code. Interceptor metadata writes raise `ArgumentError` outside `read_response` and are unaffected, but the boundary should be settled first. | Before PR A |
| L6: nil server streaming response maps to `invalid_argument` | Settles server and client attribution for a nil response | Every session-backed send maps nil to `invalid_argument` today, including native gRPC and gRPC-Web unary, while Connect unary fails `internal` at encode. A server unary interceptor that returns nil takes these paths in PR A, and a client-streaming interceptor in PR B. | Before PR A |
| L5: streaming completion can call `close_send` twice | Chooses an ownership guard or an idempotency contract | Completion calls the exchange's `close_send` after any explicit one. Wrappers make an explicit call more common, so PR B's evidence needs the guard. This design prefers a core-owned guard over a contract on callers. | Before PR B |
| L8: early native failures replace request metadata with an empty snapshot | Preserves the original request carrier on server native early-failure paths | Interceptors never run on those paths, and the client recording in [Ordering](#client) is separate. | Independent |
| L9: frozen native gRPC unary metadata | Reconciles the `Context` metadata freezing contract | Server interceptors need request metadata to be a frozen snapshot on every protocol. Unary response metadata must stay mutable until the chain returns, which holds today on every protocol. | Before PR A |
| #73: caller baggage lost on injection | Sets the merge rule for injected propagation headers | Injection happens after interceptors, so interceptor-added `baggage` is caller metadata under that rule. | Before PR A, or PR A adopts whatever rule has merged |
| L11: OTel classification casing | OTel attribute convention | None | Independent |

The interceptor design also constrains these groups:

- L2's read-only view must be the type interceptors receive, so it should be a
  small public object implementing `_Cancellation` rather than a private
  transport-only shape.
- L5 should guard the exchange-level `close_send` inside core's session or
  exchange ownership, so that completion after an explicit `close_send` from
  the application or a wrapper reaches the exchange once, instead of relying on
  every caller calling it once.
- L6 should give nil server responses one attribution across Connect unary,
  session-backed unary and client streaming, so interceptor returns need no
  protocol-specific rule.
- L9 should settle on a frozen server request metadata snapshot taken before
  application code runs. If L9 chooses mutable request metadata instead, this
  design's read-only rule needs revisiting before PR A.

## Non-goals

- Retry, hedging, backoff or OAuth frameworks. The continuation rule rejects
  repeated dispatch.
- Moving metrics, OTel or Sentry to interceptors, or giving subscribers a way
  to mutate calls.
- Server interceptors before decode, or interceptors over bytes or
  `TransportRequest`. Host middleware and custom transports own those layers.
- Per-call client interceptors as call options.
- Access to Rails controller state, or running interceptors inside the Rails
  controller lifecycle.
- A streaming invoker.
- Changes to generated code or to the transport, binding or invoker SPIs.
- Changing the deadline from an interceptor.

## Failure paths and risks

| Risk | Effect | Mitigation |
| --- | --- | --- |
| An interceptor blocks on I/O and ignores cancellation | The call outlives its deadline | Document cooperative checks; expose deadline and cancellation views; spec an interceptor that honors `on_cancel` |
| A Rails interceptor runs before authentication | Unauthenticated requests reach interceptor code | Document the order; keep authentication in Rails callbacks or host middleware; spec that a rejection skips callbacks and that Rails policy still runs on success |
| A server interceptor returns success without yielding | Host policy in the invoker, such as Rails authentication and CSRF, is skipped | Continuation rule 5 raises `InterceptorStateError`; Rails spec proves the attempt fails and runs no handler |
| An interceptor translates an exception | Observers could lose the original | Stage at every continuation boundary and record on failure; specs with real OTel and Sentry subscribers for capture-eligible and ineligible translations |
| An interceptor recovers from a failure | A successful call could be reported as failed | Discard staged failures on success; OTel-backed spec that a recovered call has an `OK` outcome with no error |
| A continuation escapes or runs twice | Late or duplicate dispatch, broken cleanup | Core-owned single-use guard with stack and fiber checks; misuse specs |
| A wrapper holds a session after its scope | Late I/O on a closed stream | The session already raises `StreamStateError`; spec escaped wrapper use |
| Interceptor state leaks between concurrent RPCs | Cross-talk between streams | Per-RPC call objects and wrappers; concurrent stream spec on one connection |
| Per-call overhead | Extra allocations on every RPC | Skip the chain when no interceptor has a method for the role and kind; record allocation counts in PR A without claiming a baseline (#35 owns baselines) |
| Freezing client response metadata | Existing callers that mutate `Response#metadata` raise `FrozenError` | 0.x is experimental; release notes call out the change; spec the frozen snapshot |
| `ClientScope` identity change | A translated exception could be replaced by the original | Explicit rule and specs for translation, swallowing and session-owned errors |

## Delivery and verification

Each implementation PR goes through the maintainer's delivery workflow, with
independent Codex and Claude review and CodeRabbit. Every PR runs the focused
specs, `mise run check`, `mise run rbs:check`, `mise run rbs:package` (installed
signature discovery, which neither `verify` nor `rbs:check` includes), and
`mise run verify` followed by `mise run rack:verify`, as AGENTS.md requires.
Core has no Rack host, so evidence that needs a host lives in the owning
package's bundle and runs that package's verify task:

| PR | Package bundles and tasks |
| --- | --- |
| A | `rack:verify` (Connect over NetHTTP and Rack), `async:verify` (native gRPC and gRPC-Web unary), `testing:verify`, `otel:verify` and `sentry:verify` (observer evidence) |
| B | `async:verify`, `falcon:verify`, `health:verify`, `otel:verify`, `sentry:verify` |
| C | `verify:rails` with `RAILS_LANE=8.0` and `RAILS_LANE=8.1`, `testing:verify` |

No PR changes protocol wire behavior.

### PR A: core unary interceptors

Scope:

- `interceptors:` on `Client.new` and `Server.new`, discovery and validation.
- `ClientUnaryCall`, `ServerUnaryCall`, the continuation guard and
  `InterceptorStateError`.
- Client chain for Connect POST, Connect GET and native gRPC unary, with the
  deadline and arbiter moved before the chain, injection and lifecycle
  metadata at dispatch, commit at the outer boundary, and frozen response
  metadata. For native gRPC unary, `ClientScope` accepts the pre-created
  arbiter and an interceptor-returned `Response`.
- Server chain built once around `@invoker`, covering the four unary sites,
  with the negotiated protocol passed to the private chain entry.
- Staged failure recording at continuation boundaries, including
  `Context#record_failure` inside the chain.
- RBS, inventory entries, consumer checks and static negatives.
- Guides: [design](../design.md) replaces "Interceptors ... remain future
  work", plus the [unary](../unary.md), [gRPC](../grpc.md) and
  [gRPC-Web](../grpc-web.md) guides and the [support](../support.md) inventory.

Evidence:

- Composition order on both roles, recorded by three interceptors, over real
  localhost NetHTTP against `connectrpc-rack` for Connect POST and GET, and
  over the Async H2 transport and Host for native gRPC and gRPC-Web unary.
- Short-circuit rejection: a recording transport sees no request on the
  client; on the server the handler never runs, and the wire error carries the
  code, message, metadata and trailers.
- Client successful short-circuit and fallback on Connect and on native gRPC
  unary: the caller receives the interceptor's `Response` with frozen
  metadata, and the lifecycle reports `OK` with `delivery: :unobserved`.
- A server interceptor that returns without yielding fails `internal` with
  `InterceptorStateError` recorded, and the handler never runs.
- Construction: an element with no method for the role raises
  `ArgumentError`.
- Handler failure: the wire error is `internal`, and a real subscriber's
  `Outcome#error` is the handler's original exception after an interceptor
  translated it.
- Recovery: an interceptor that rescues a handler exception and returns a
  message produces an `OK` outcome with no error, and the OTel span is not
  marked failed.
- Sentry: a handler bug translated to `internal` is captured; one translated
  to `invalid_argument` is not.
- Metadata: writes to client request metadata after dispatch raise; server
  request metadata cannot be mutated by an interceptor; server response
  metadata and trailers written after `yield` reach the wire; a client unary
  interceptor returning a non-`Response` raises `InterceptorStateError`.
- Cancellation, named per role: on the client, a source cancelled inside an
  interceptor before `yield` gives `canceled` with no transport call, expiry
  during post-processing gives `deadline_exceeded` after a successful
  transport response, and an interceptor-raised exception keeps its identity
  under accepted cancellation. On the server, cancellation accepted while an
  interceptor runs gives the accepted reason on the wire, with the original
  exception still recorded.
- Misuse, on both roles: a second `yield`, a captured block called after
  return, and a block called from another thread and from an `Async` child
  task each raise `InterceptorStateError`.
- No-interceptor behavior: the existing suite passes, with any spec changed
  by the newly frozen Connect unary response metadata or the earlier deadline
  start named in the PR.
- Targeted perturbations: reversing chain order fails the order spec, and
  recording boundary failures directly fails the recovery spec, each at its
  assertion.

### PR B: core streaming interceptors

Scope:

- `ClientStreamCall`, `ServerStreamCall`, their `ServerStreaming`
  subclasses and `wrap_stream`, with the wrap-time method check.
- Client chain in `stream_call` for server, client and bidi streaming on
  Connect and gRPC, inside `ClientScope` and outside `open_stream`.
- Server chain at `invoke_stream_handler` and at the gRPC-Web server-streaming
  call site.
- Result passing for client streaming, the `ClientScope` identity rules and
  header publication rules.
- RBS and negatives for stream calls; the [streaming guide](../streaming.md).

Evidence:

- Wrapper and interceptor order for all three shapes over real Async h2c for
  Connect and native gRPC, Falcon HTTP/1 for Connect server and client
  streaming, Async gRPC-Web server streaming, and `ThreadedTransport` server
  streaming.
- Escaped wrapper use raises `StreamStateError`; a wrapper `close_send`
  followed by core completion reaches a strict exchange's `close_send` once
  (needs L5). A wrapper missing an endpoint method fails at wrap time.
- Exactly-once cleanup: with counting doubles at the exchange and lifecycle
  boundaries, an interceptor raising before `yield`, an interceptor raising
  after `yield`, a raising `wrap_stream` block and a raising wrapper method
  each produce one abandon or terminal, one `context.close` and one
  `lifecycle.finish`. A stream interceptor that returns without yielding
  raises `InterceptorStateError` with the same single cleanup.
- Original processing task: on the Async Host over mTLS, the task-scoped
  client certificate accessor returns the same certificate inside the
  interceptor, the wrapper and the handler, and a continuation called from an
  `Async` child task is rejected.
- Concurrent isolation: two concurrent bidi streams on one Async connection,
  each with per-RPC wrapper counters, distinct request metadata and distinct
  call objects, synchronized on observable message progress, never on sleeps.
  Each stream asserts it saw only its own metadata, call identity and counts.
- Headers and trailers: a server interceptor writing response metadata after
  publication raises; trailers added after `yield`, including after a handler
  error, reach the terminal.
- Cancellation: a real peer `RST_STREAM(CANCEL)` while an interceptor is
  blocked before `yield` and while the handler runs; client abandonment
  through a wrapper.
- Identity: an application exception translated by an interceptor reaches the
  caller as the translation; a swallowed application exception still fails
  the call with the original.
- Health Watch through a server interceptor on the Async Host.
- Static: the narrowing positive consumer and the `request=` negative.

### PR C: Rails hook, host evidence and documentation

Scope:

- Private `connect_interceptors` hook in `connectrpc-on-rails`, RBS and
  inventory.
- README sections in the Rails, Rack, Async, Falcon, testing and health gems
  describing coverage and order.
- One runnable interceptor example in `examples/`, if it fits the existing
  two-terminal layout.

Evidence:

- A real controller on both Rails lanes: an interceptor runs before
  `before_action`; a rejection skips callbacks, `rescue_from` and the Rails
  instrumentation event; a successful call still runs CSRF and authentication
  callbacks; an interceptor that returns without yielding fails with no
  handler call and no successful response; a controller exception reaches
  `Rails.error.report` once and the interceptor sees the normalized `Error`.
- `connectrpc-testing` round trip with both chains.

## Testing strategy

Proportionate to the risk in each PR:

- **Real callers.** Order, success, rejection, failure and cancellation run
  through real clients and real hosts: NetHTTP over localhost, the Async H2
  transport and Host, Falcon HTTP/1, `ThreadedTransport` and the testing
  transport. Recording transports are used only to prove that dispatch did not
  happen.
- **Observable synchronization.** Cancellation and concurrency specs wait on
  wire progress, handler entry signals or bounded polling of the asserted
  condition. No fixed sleeps.
- **Observers.** Instrumentation specs use real subscribers, and the Sentry
  and OTel packages' SDK-backed bundles check that originals survive
  interceptor translation and that durations include interceptor time.
- **Static.** `rbs:check` covers consumer calls and the exact negative
  diagnostics listed in [Static typing](#static-typing).
- **Perturbation.** One per behavior cluster where inspection cannot rule out
  a false positive: chain order, staged failure recording, the single-use
  guard and exactly-once stream cleanup.
- **Confirmation.** Each PR confirms from runner output that the new specs
  ran, by name or count.

This PR is documentation only. It is verified by link and command review.

## Documentation in this PR

Only this file. The guides change with the implementing PRs, so they never
describe interceptors before the code exists.

## Open questions

1. **Deadline shortening.** connect-go interceptors can derive a shorter
   context deadline. Should a Ruby interceptor be able to shorten, but never
   extend, the call deadline before dispatch? It needs a rule for the wire
   timeout header and for which arbiter owns the shorter expiry.
2. **Portable peer facts.** Should `ServerUnaryCall` and `ServerStreamCall`
   expose peer facts such as a remote address, as connect-go v2's
   `CallInfo.PeerAddr` does? Rack and Rails can supply less than the native
   hosts.
3. **Per-call client interceptors.** gRPC Java attaches interceptors per call.
   Is there demand beyond what per-client lists and per-call metadata already
   cover?
4. **Function shorthand.** connect-go v1 has `UnaryInterceptorFunc`. Is a
   one-method object enough in Ruby, or is a lambda shorthand worth its RBS
   cost?
5. **Pre-decode server hook.** If applications ask for core-level rejection
   before decode, a separate `server_admit(call)` hook that sees only metadata
   could follow connect-go v2 without changing the typed unary model.
6. **Server-side successful short-circuits.** Server caching interceptors are
   ruled out because the invoker can carry host policy. They could return if
   an invoker declares that it carries none, for example the default
   `invoke_handler`, while the Rails invoker keeps rule 5. Is there demand?
7. **Typed stream wrappers.** Structural RBS interfaces for the six endpoint
   classes would let `wrap_stream` and wrappers be checked statically instead
   of by the wrap-time method check. Are they worth their signature and
   inventory cost?
