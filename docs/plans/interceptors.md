# Typed client and server interceptors

Status: implemented. PR A ([#89](https://github.com/jimeh/connectrpc-ruby/pull/89))
delivered core unary interceptors, PR B
([#94](https://github.com/jimeh/connectrpc-ruby/pull/94)) core streaming
interceptors, and PR C the Rails `connect_interceptors` hook, host
documentation and the runnable example. The design below is kept as approved.
Base: `54d55fb` (lifecycle metadata, #81), after release 0.1.2.
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
| Continuation | A no-argument `yield`, guarded by core: once only, on the owning fiber, and only while the interceptor runs. Misuse raises `ConnectRPC::InterceptorStateError`. Changes go through a per-RPC call object that is open until dispatch. Only client unary interceptors may return a result without yielding, as a cached short-circuit before dispatch. No layer on either role can turn a failed continuation into success; it may only translate the exception. |
| Server placement | After the bounded read and decode, as in connect-go v1. Unary and server-streaming interceptors see the typed request. Rejection before decode stays with host middleware. |
| Instrumentation | Interceptors run inside the instrumentation activation. Measured processing durations include interceptor time, and observer spans are active while interceptors run. Subscribers stay observers. |
| Streaming model | The stream methods wrap the whole scope. `call.wrap_stream { \|stream\| ... }` decorates the stream endpoint. Core applies wrappers innermost first, so the application sees the outermost wrapper. Core cleanup always uses the raw session. |
| Errors | A short-circuit is a raised `ConnectRPC::Error`. Core records the exception escaping each continuation boundary, first one wins, so observers keep the original. Because no layer can recover, a failure that escapes a continuation always fails the call. Interceptor-raised exceptions are independent user exceptions and keep their identity on the client. |
| Metadata | Client request metadata is mutable until dispatch, then frozen. Response metadata and trailers are frozen snapshots on the client. Server request metadata is the handler's mutable `Context#request_metadata`, following Jim's L9 choice in #81; server response metadata is mutable until headers are published and trailers until the terminal response. Interceptors cannot change the deadline. |
| Hosts | No host-specific code except a private Rails `connect_interceptors` hook. Rails interceptors run outside the controller lifecycle. |
| Delivery | Prerequisites from #26 land first. Then three PRs: core unary, core streaming, then the Rails hook with host evidence and documentation. |

Design review on 2026-11-16 narrowed these choices without changing their
direction, and Jim accepted both narrowings:

- Jim's planning choice let any unary interceptor return a result without
  yielding. On the server, the invoker is the only path into host policy: on
  Rails it runs authentication, CSRF, `before_action` and `after_action`
  callbacks, and turns their rejections into a `ConnectRPC::Error`
  (`gems/connectrpc-on-rails/lib/connectrpc/on_rails/controller.rb:80-92`). A
  server layer that returned success without yielding, or that rescued that
  error after yielding, would bypass host policy. The design therefore keeps
  one invariant: a successful server response requires a successful invoker
  or stream handler. [Deferred question 6](#deferred-questions) records how
  server-side caching or fallback could return later.
- An earlier revision also let a client unary interceptor return a fallback
  `Response` after its continuation failed. Each attempt to make that
  observable correctly failed review. The client records failures directly
  from the transport, codec, `process_call` and `ClientScope`, so a fallback
  carried those errors into an `OK` outcome, which OTel reports as
  `operation_failed`
  (`gems/connectrpc-otel/lib/connectrpc/otel/subscriber.rb:140-147`). Staging
  those recordings fixed processing, but a native gRPC fallback after an
  aborted exchange still ends as an ERROR span, because OTel and Sentry fail
  any aborted delivery (`gems/connectrpc-otel/README.md:155`). A Connect
  fallback would also report the failed exchange's HTTP status and metadata
  next to `OK`. Client fallbacks are therefore out of scope. No layer on
  either role can turn a failed continuation into success, which removes the
  staging mechanism entirely. Applications can still rescue around
  `client.call` and supply a fallback there, which also fits the settled
  direction that resilience policy stays outside interceptors.
  [Deferred question 8](#deferred-questions) records what client fallbacks
  would need.

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
| Connect unary POST | `lib/connectrpc/server.rb:271` | `@invoker` |
| Connect unary GET | `lib/connectrpc/server.rb:205` | `@invoker` |
| Native gRPC unary | `lib/connectrpc/grpc_server.rb:89` | `@invoker` |
| gRPC-Web unary | `lib/connectrpc/grpc_web_server.rb:144` | `@invoker` |
| Connect streams and native gRPC streams | `invoke_stream_handler`, `lib/connectrpc/streaming/server_execution.rb:60-79` | handler |
| gRPC-Web server streaming | `lib/connectrpc/grpc_web_server.rb:146-148` | handler, called directly |

The default invoker is `Server#invoke_handler`
(`lib/connectrpc/server.rb:251-257`). The Rails gem supplies its own invoker,
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

Ruby differs in seven places, each for a concrete reason:

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
7. **No recovery and no server short-circuit.** connect-go and grpc-go
   interceptors can return a response without calling `next`, or after
   `next` failed, which is how server-side caching and client or server
   fallback interceptors work there. Here only a client unary interceptor can
   return without yielding, and no layer can turn a failed continuation into
   success (continuation rules 5 and 6). On the server, the invoker can carry
   host policy that a short-circuit or fallback would bypass. On the client,
   a fallback cannot be observed consistently with the recorded failure and
   the delivery facts. Deferred questions 6 and 8 record how each could
   return.

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
  no per-call allocation. Response freezing (see [Metadata](#metadata)) is
  not part of the chain and applies on that path too.
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
4. **Client unary may skip it.** A client unary interceptor may return a
   `ConnectRPC::Response` without yielding, for example from a local cache.
   Nothing has been dispatched or recorded at that point. A value that is not
   a `Response`, or a `Response` whose `metadata` or `trailers` is not a
   `Metadata` (the RBS `_PartialResponse` shape), raises
   `InterceptorStateError`. A client-streaming interceptor that returns a
   value other than a complete `Response` raises it too. When the
   continuation failed, rule 6 applies first: the return value is discarded
   whatever its type.
5. **Everything else must use it.** A server interceptor, or a stream
   interceptor on either role, that returns without yielding and without
   raising raises `InterceptorStateError`. On the server, the invoker is the
   host's handler-execution boundary and can carry host policy: the Rails
   invoker is the only path into Rails authentication, CSRF and controller
   callbacks. A successful server short-circuit would bypass that policy,
   which #32 forbids. A stream short-circuit would report success for a
   handler or application block that never ran.
6. **No recovery.** When the continuation of any layer, on either role and
   in any shape, raised, that layer may raise a different exception, which
   translates the failure, but it cannot turn the failure into success. If it
   returns normally, core discards the return value and re-raises the
   continuation's exception, which keeps its identity. On the server this is
   what preserves host policy: a Rails `before_action`,
   CSRF or `after_action` rejection arrives as a `ConnectRPC::Error` from the
   invoker, and no interceptor can replace it with a response. A successful
   server response therefore always means the invoker or stream handler
   returned successfully. On the client it means a failure that escaped a
   continuation never sits on a successful outcome.

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
  single-request scope and returns its `Response`. `ClientScope#start`
  already creates the deadline and arbiter before its block runs
  (`lib/connectrpc/streaming/client_scope.rb:70-75`), so they precede the
  chain without change. `ClientScope` must, however, accept a short-circuit
  `Response`, returned without yielding, as the scope result. Today
  `ClientScope#execute` returns `@result` and commits only when
  `perform_stream_scope` set `@completed`
  (`lib/connectrpc/streaming/client_scope.rb:25-29`), and delivery is
  `:completed` only when the scope completed (`:81-89`), so a skipped
  continuation would return nil and be reported as an aborted failure. In PR
  A, `ClientScope` changes in three ways, and keeps today's delivery and
  abandonment rules for every exchange that opened:
  - **Result.** The scope result is the value the outermost interceptor
    returns, not `@result`, so both a short-circuit `Response` and a
    replacement `Response` returned after a successful continuation reach
    the caller. A short-circuit completes the scope and commits the arbiter.
  - **Interceptor exceptions.** An exception that escapes the outermost
    interceptor and is independent under
    [failure provenance](#failure-provenance) becomes the scope's
    independent error, like an application block exception today. `execute`
    then skips `commit!` (`client_scope.rb:30-33`), and `finish` re-raises it
    by identity, so an accepted cancellation does not replace it.
  - **Delivery before dispatch.** When an interceptor short-circuits or
    raises before the innermost continuation opened an exchange, the scope
    reports `delivery: :unobserved`, as the Connect unary path does, instead
    of today's `:aborted` (`:81-89`). How delivery is derived in the other
    interceptor cases, from exchange facts rather than from who raised, is
    [settled in PR A and PR B](#settled-in-pr-a-and-pr-b).

  The same three rules apply to `client_stream` chains, which run inside the
  same `ClientScope`.

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
  them. A server interceptor must yield (continuation rule 5), and it cannot
  turn the invoker's failure into success (rule 6). A callback rejection,
  including a `before_action` that renders 401, a CSRF failure or an
  `after_action` that sets a 403, reaches interceptors as the invoker's
  `ConnectRPC::Error`, and the client still receives a failure. The only way
  to a successful response is through a successful run of Rails' full
  `process_action` chain. An interceptor can only reject earlier, translate a
  failure, or add behavior around that chain.
- A rejecting interceptor skips the Rails callbacks, `rescue_from` and the
  Rails instrumentation event, the same as a core preflight failure today
  (`gems/connectrpc-on-rails/README.md:161-163`).
- Interceptors get no controller state, such as `current_user`. Policy that
  needs Rails state stays in Rails callbacks.
- Interceptor exceptions do not pass through `rescue_from` or
  `Rails.error.report`. Core records them for observers, so OTel and Sentry
  still receive the original.
- An exception from inside the controller reaches interceptors as the
  normalized `Error` that the Rails invoker raises. The invoker calls
  `Context#record_failure` with the original first (`controller.rb:80-92`),
  and recording keeps the first failure. Rule 6 means that failure always
  fails the call, so observers receive the original.

## Unary semantics

### Short-circuits

An interceptor short-circuits by not yielding.

- **Rejection.** Raise a `ConnectRPC::Error`. On the client, the transport is
  never called, `message_sent` never fires, and the caller receives that
  `Error`. On the server, the handler never runs and the client receives the
  error's code, message, details, metadata and trailers. The response also
  carries response metadata and trailers that interceptors set on the context
  before raising, merged as `error_response` does today
  (`lib/connectrpc/server.rb:383-404`).
- **Successful short-circuit.** Only on the client: return a `Response`, for
  example from a local cache. Core freezes it like any other result (see
  [Metadata](#metadata)). A server interceptor that returns without yielding
  raises `InterceptorStateError` (continuation rule 5).

### Errors

- **Server mapping.** An `Error` raised anywhere in the chain becomes a wire
  error. Any other `StandardError` becomes a generic `internal` error with a
  safe message, exactly like a handler exception. `Exception`s that are not
  `StandardError` propagate with today's cleanup.
- **Recording.** Core records the exception escaping each continuation
  boundary with `record_failure` before the outer interceptor sees it.
  `record_failure` keeps the first failure, so lifecycle observers receive the
  original exception even after an interceptor rescued it and raised a
  translated `Error`. An interceptor that raises its own exception with no
  inner failure has that exception recorded. Rule 6 means any failure that
  escapes a continuation fails the call, so these boundary recordings never
  attach to a successful outcome. That is narrower than "a successful outcome
  never carries an error": a stream operation failure that the application or
  a wrapper catches inside the scope is recorded by the session immediately
  (`lib/connectrpc/streaming/session.rb:132-139`) and can sit on an `OK`
  outcome today. Interceptors keep that behavior; see
  [Settled in PR A and PR B](#settled-in-pr-a-and-pr-b). `Context#record_failure`
  is for framework invokers, not interceptors: an interceptor that calls it
  and then succeeds puts an error on an `OK` outcome. The client keeps recording
  directly from the transport (`lib/connectrpc/transports/net_http.rb:138-143`),
  the codec paths (`lib/connectrpc/client.rb:259-266`), `process_call` and
  `ClientScope`, and the transport SPI does not change.
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
  backtrace. [Failure provenance](#failure-provenance) defines which
  exceptions count as interceptor-raised.

- **Translation.** Any interceptor may rescue the continuation's exception
  and raise a different one, typically an `Error`, but no interceptor can
  return a result after a failed continuation (rule 6). After a successful
  continuation a client interceptor may return a replacement `Response`, and
  a server interceptor may return a replacement response message. An
  application that wants a fallback rescues around `client.call`.
  Arbitration still applies; see
  [Cancellation and deadlines](#cancellation-and-deadlines).
- **Nil responses.** A server unary interceptor that returns nil follows the
  handler-returned-nil path. On Connect unary the binding rejects it at encode
  and the call fails `internal`; native gRPC and gRPC-Web unary send through
  the stream session, which today maps nil to `invalid_argument`
  (`lib/connectrpc/streaming/session_messages.rb:113-115`). L6 settles that
  attribution, and PR A follows it.
- **What interceptors see.** Interceptors see whatever the inner layer
  raised. Under the default server invoker and in server stream chains, that
  is the handler's own exception, which core normalizes to `internal` only
  after the chain (`lib/connectrpc/server.rb:142-145`,
  `lib/connectrpc/streaming/server.rb:116-123`). The Rails invoker hands them
  a normalized `Error`. On the client, core failures arrive as `Error`s, for
  example a decode `CodecError` arrives as `Error(:internal)`
  (`lib/connectrpc/client.rb:264-266`), and client stream application
  exceptions arrive unchanged. When an interceptor sees a normalized `Error`,
  its `cause` is whatever L1 settles, and an interceptor must not rely on
  `cause` to reach the original; lifecycle observers receive it.
- **Translation hides an accepted cancellation.** The client commits once at
  the outer boundary, so a core failure that an accepted cancellation would
  replace reaches interceptors unreplaced. If an interceptor translates it,
  the translation is independent under
  [failure provenance](#failure-provenance) and reaches the caller instead of
  `canceled`. Error-mapping interceptors should check `call.cancellation`
  before translating, and leave the exception unchanged when a reason was
  accepted.

### Failure provenance

On the client, arbitration depends on where an exception came from. One rule
covers every client path (Connect unary, native gRPC unary and the three
stream shapes), and classifies by origin, not by which object owns a
failure:

- **Core-owned.** The exact exception object that escaped the innermost
  continuation, and that today's code classifies as core-owned, keeps that
  classification and today's arbitration unchanged. This covers transport
  failures, including `open_stream` and `open_grpc_stream` failures raised
  before any stream session exists; codec, metadata and status failures;
  deadline expiry while building a request (`streaming/client.rb:99`);
  completion errors such as `StreamNotConsumedError` and "Expected exactly
  one response" (`client_execution.rb:69-77`); the replacement error from
  `recover_send_failure` (`client_execution.rb:22-25`); and arbitration
  errors. An accepted cancellation or expiry still replaces these, as today.
- **Independent, from the application.** An exception that today's
  `ClientScope` already treats as an independent user exception, because the
  application block raised it and the session does not own it
  (`client_execution.rb:49-54`), stays independent.
- **Independent, from an interceptor or wrapper.** Any other exception that
  escapes the outermost interceptor is independent: one an interceptor or
  `wrap_stream` block raised itself, or a translation of an inner failure.
  Wrapper methods run inside the application block, so their exceptions are
  already classified there, as today.

Core implements this by remembering the exception that escaped the innermost
continuation and its classification, then comparing identity at the
outermost boundary. Rule 6 re-raises the continuation's exception unchanged,
so a normal return after a failure keeps the inner classification.
`InterceptorStateError` raised by core for interceptor misuse is independent,
so cancellation never masks misuse.

### Metadata

| Metadata | Client | Server |
| --- | --- | --- |
| Request | Mutable `Metadata` on the call while it is open; validated on write; frozen at dispatch | Mutable `context.request_metadata`, shared with the handler; stream reader `headers` stay frozen snapshots |
| Response leading | Frozen snapshot in `Response#metadata` | Mutable `context.response_metadata` until headers are published |
| Response trailing | Frozen snapshot in `Response#trailers` | Mutable `context.response_trailers` until the terminal response |
| Error | `Error#metadata` and `#trailers` | Merged into the response, as today |

- **Client request.** `Metadata` already validates names and values and
  rejects protocol and framing headers (`lib/connectrpc/metadata.rb`). A later
  interceptor sees earlier interceptors' writes. Writes after dispatch raise
  `FrozenError`.
- **Client response.** `Response` is a `Struct`
  (`lib/connectrpc/protocol.rb:10`), and freezing it does not freeze the
  `Metadata` objects it holds (`Metadata#freeze` is separate,
  `lib/connectrpc/metadata.rb:65-68`). Core freezes the `Response`
  itself, so `response.metadata = ...` raises, and freezes its metadata and
  trailers. It does this whenever a layer returns a `Response`, before the
  next outer layer receives it: when the innermost continuation returns, when
  a client unary interceptor short-circuits, and when any layer returns a
  replacement. A replacement or cached `Response` built by an inner
  interceptor therefore reaches outer interceptors and the caller frozen, and
  no outer layer can alter an object an inner layer shares across calls. The message object is neither
  copied nor frozen, as today; core cannot deep-freeze binding-owned messages.
  An interceptor that serves one cached `Response` to many callers therefore
  shares its message between them, and should build a fresh `Response` with a
  copied message per call when callers may mutate it. To change what the
  application sees, an interceptor returns a new `Response`. Native gRPC and
  streaming response metadata is mostly frozen already
  (`lib/connectrpc/grpc.rb:81`, `lib/connectrpc/streaming/session_io.rb:85`),
  but Connect unary metadata, every `Response` struct and the client-streaming
  trailers fallback (`Metadata.new` in `client_execution.rb:84`) are mutable
  today. Freezing applies to every client response, with or without
  interceptors: PR A freezes unary responses on Connect and native gRPC, and
  PR B freezes the client-streaming `Response`.
- **Server request.** L9 settled in
  [#81](https://github.com/jimeh/connectrpc-ruby/pull/81), by Jim's choice:
  every server `Context` keeps mutable request metadata on every protocol,
  and the stream session keeps its own frozen snapshot for reader and duplex
  `headers`. Interceptors receive that same `Context`, so they can read
  request metadata and also add derived values for inner layers and the
  handler, such as a verified principal. Interceptors and handlers run in
  one process with no trust boundary between them, so a check reads the
  value at check time; an inner layer that rewrites request metadata after
  an outer check is application code changing its own input. The lifecycle
  records the request carrier once, when processing begins (#81's L8 fix),
  so interceptor writes do not change `Outcome#request_metadata`.
- **Server response.** For unary calls on every protocol, headers are built
  after the chain returns, so writes before and after `yield` both work. On
  native gRPC and gRPC-Web unary, core freezes response metadata when it
  publishes the head, which happens after the chain returns
  (`lib/connectrpc/grpc_server.rb:111-122`). For streams, see
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

This example validates the server-streaming request and outgoing messages
sent through `send_message`. It does not validate incoming messages on any
readable endpoint, such as a server `RequestReader` or either side of a bidi
stream; a read-side wrapper must override both
`receive` and `each`, as **Delegation** below explains.

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
  code. Delegating does not route through the wrapper: `ResponseReader#each`
  calls `receive` on the reader itself
  (`lib/connectrpc/streaming/streams.rb:67-77`), so a wrapper that overrides
  only `receive` and delegates `each` is bypassed by every read made through
  `each`. A read-side wrapper must override both `receive` and `each`, for
  example by implementing `each` as a loop over its own `receive`.
- **Typing.** `wrap_stream` takes a block typed `(untyped) -> untyped`, and
  application blocks and handlers keep their nominal endpoint types
  (`ResponseReader[R]` and so on). Wrappers are therefore duck-typed against
  those classes, backed by the runtime check above. Structural endpoint
  interfaces that would let wrappers be checked statically are
  [deferred question 7](#deferred-questions).
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
  - [Failure provenance](#failure-provenance) decides whether the exception
    that escapes the outermost interceptor is independent. If an interceptor
    translated the application's exception, the translation is independent
    and wins.
  - An interceptor cannot turn a failed scope into success (rule 6). If the
    application block or an inner layer raised and an interceptor returns
    normally, core still raises that exception, because cleanup already
    abandoned the stream.
  - Core-owned errors, including session-owned ones and transport failures
    raised before the session exists, keep today's arbitration and delivery
    rules.
- **Server.** Stream errors from interceptors follow the handler's path:
  `capture_stream_error` records them, maps them to a safe `Error` and renders
  the terminal (`lib/connectrpc/streaming/server.rb:116-123`). A
  `server_stream` interceptor may translate a handler exception by raising a
  different one, but it cannot produce an `OK` terminal after the handler or
  an inner layer raised (rule 6): if it returns normally, core renders the
  terminal from the continuation's exception, which is an inner layer's
  translation when there is one. Messages the handler already sent stay
  sent. Because stream failures always fail the call, the direct recording at
  each boundary keeps the original for observers.

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
  cancellation, it never accepts anything else, and no successful result
  can follow:
  - On the server, the terminal error is the accepted reason, whatever an
    interceptor raised or returned, as `terminal_error` does for handlers today
    (`lib/connectrpc/server.rb:372-381`). Observers still receive the original
    exception.
  - On the client, a core-owned failure is replaced by the accepted reason, as
    today, and a successful return after an accepted reason becomes that
    reason's `Error`. An independent exception reaches the caller with its
    identity, as an independent application exception does on streams today.
- **What client observers receive.** There is no separate channel for the
  arbiter's terminal. The outcome's code and normalized error come from the
  exception that reaches the caller, as streams report today
  (`client_scope.rb:92-101`) and Connect unary reports from its escaping
  exception (`client.rb:122`): the accepted reason's code when it replaced a
  core-owned failure, the independent exception's own `Error` code, or nil
  for a non-`Error` exception. `Outcome#error` stays the first recorded
  failure. PR A applies the same rule to Connect unary, which today derives
  its code from the escaping exception already. One override exists today:
  an abandoned stream scope reports `canceled` whatever reached the caller
  (`client_scope.rb:98-99`). Its precedence over interceptor exceptions is
  [settled in PR B](#settled-in-pr-a-and-pr-b).
- **No deadline changes.** Interceptors cannot extend or shorten the deadline;
  it is fixed when the call starts. Shortening is
  [deferred question 1](#deferred-questions).
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
  peer field is [deferred question 2](#deferred-questions).

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
  - a server-only interceptor passed to `Client.new`, and a client-only
    interceptor passed to `Server.new`;
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
| L8: early native failures replace request metadata with an empty snapshot | Preserves the original request carrier on server native early-failure paths | Interceptors never run on those paths, and the client recording in [Ordering](#client-1) is separate. | Independent |
| L9: frozen native gRPC unary metadata | Settled in #81: request metadata stays mutable on every server `Context`; sessions keep their own frozen snapshot | Interceptors share the handler's mutable `Context`. Unary response metadata must stay mutable until the chain returns, which holds today on every protocol. | Done in #81 (`54d55fb`) |
| #73: caller baggage lost on injection | Settled in #81: injected keys replace caller values, except `baggage`, which carries the caller's values followed by the injected ones | Injection happens after interceptors, so interceptor-added `baggage` is caller metadata under that rule and survives injection. | Done in #81 (`54d55fb`) |
| L11: OTel classification casing | OTel attribute convention | None | Independent |

The interceptor design also constrains these groups:

- L2's read-only view must be the type interceptors receive, so it should be a
  small public object implementing `_Cancellation` rather than a private
  transport-only shape.
- Every built-in transport wraps its work in `CallCancellation.with_request`
  (for example `lib/connectrpc/transports/net_http.rb:127`), which passes a
  request through unchanged only when its cancellation is a
  `CallCancellation` and otherwise creates a second arbiter that commits when
  the transport returns (`lib/connectrpc/cancellation.rb:127-148`). L2 must
  make `with_request` recognize the core-owned view and not wrap it, or every
  call gets a second arbiter that commits before decode and before
  interceptor post-processing, breaking "one arbiter per call" and "commit at
  the outer boundary".
- L5 should guard the exchange-level `close_send` inside core's session or
  exchange ownership, so that completion after an explicit `close_send` from
  the application or a wrapper reaches the exchange once, instead of relying on
  every caller calling it once.
- L6 should give nil server responses one attribution across Connect unary,
  session-backed unary and client streaming, so interceptor returns need no
  protocol-specific rule.
- L9's outcome in #81 (mutable request metadata on every server `Context`)
  is what this design assumes; a later change to freeze it would need to
  decide whether interceptors may still add request metadata for inner
  layers.

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
| A server interceptor returns success without yielding, or rescues the invoker's failure and returns a response | Host policy in the invoker, such as Rails authentication, CSRF and `after_action` checks, is bypassed | Rules 5 and 6: no server success without a successful invoker; Rails specs for each attempt |
| An interceptor translates an exception | Observers could lose the original | Record at every continuation boundary, first one wins; specs with real OTel and Sentry subscribers for capture-eligible and ineligible translations |
| An application wants a client fallback after a failure | Rule 6 rejects it in an interceptor | Document rescuing around `client.call`; deferred question 8 records what an interceptor fallback would need |
| A continuation escapes or runs twice | Late or duplicate dispatch, broken cleanup | Core-owned single-use guard with stack and fiber checks; misuse specs |
| A wrapper holds a session after its scope | Late I/O on a closed stream | The session already raises `StreamStateError`; spec escaped wrapper use |
| Interceptor state leaks between concurrent RPCs | Cross-talk between streams | Per-RPC call objects and wrappers; concurrent stream spec on one connection |
| Per-call overhead | Extra allocations on every RPC | Skip the chain when no interceptor has a method for the role and kind; record allocation counts in PR A without claiming a baseline (#35 owns baselines) |
| Freezing client responses | Existing callers that mutate a `Response` or its metadata raise `FrozenError` | 0.x is experimental; release notes call out the change; spec the frozen snapshot |
| A cached `Response` is shared between callers | One caller's message mutation is visible to others | Document that core does not copy messages; recommend a fresh `Response` and copied message per call |
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
  metadata at dispatch, commit at the outer boundary, and frozen responses.
  For native gRPC unary, `ClientScope` accepts a short-circuit `Response`
  as the scope result.
- Server chain built once around `@invoker`, covering the four unary sites,
  with the negotiated protocol passed to the private chain entry.
- Direct failure recording at continuation boundaries.
- Rule 6 for client and server unary layers.
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
- Client successful short-circuit on Connect and on native gRPC unary: the
  caller receives the interceptor's `Response` frozen, including the struct,
  and the lifecycle reports `OK` with `delivery: :unobserved`. A rejection
  raised before `yield` also reports `:unobserved` on both protocols.
- Replacement results: after a successful continuation, a client interceptor's
  replacement `Response` reaches the caller on Connect and on native gRPC, and
  a server interceptor's replacement message reaches the wire. An outer
  client interceptor receives an inner interceptor's short-circuit or
  replacement `Response` already frozen. A unary
  interceptor that raises after a successful continuation fails the call with
  that exception on both roles.
- Frozen responses from real calls, with and without interceptors: the
  `Response` struct, its metadata and its trailers are frozen on Connect and
  native gRPC.
- Rule 6 on the client: an interceptor that rescues a NetHTTP connection
  failure, the `Error(:internal)` that wraps a response decode `CodecError`,
  or a native gRPC remote error and
  returns a `Response` still raises the continuation's exception, and the
  outcome keeps the original failure.
- A server interceptor that returns without yielding fails `internal` with
  `InterceptorStateError` recorded, and the handler never runs. A server
  interceptor that rescues a handler exception and returns a message still
  fails with the continuation's exception.
- Two-layer translation: an inner interceptor translates a handler or
  transport exception to `Error` A, the outer one rescues A and returns
  normally, and the call fails with A, not with the handler's original, while
  `Outcome#error` is the original.
- Construction: an element with no method for the role raises
  `ArgumentError`.
- Handler failure: the wire error is `internal`, and a real subscriber's
  `Outcome#error` is the handler's original exception after an interceptor
  translated it.
- Sentry: a handler bug translated to `internal` is captured; one translated
  to `invalid_argument` is not.
- Metadata: writes to client request metadata after dispatch raise; a server
  interceptor's request metadata write is visible to the handler on Connect,
  native gRPC and gRPC-Web, and `Outcome#request_metadata` is unchanged by it;
  server response
  metadata and trailers written after `yield` reach the wire; a client unary
  interceptor returning a non-`Response` raises `InterceptorStateError`.
- Cancellation, named per role and path: on the client, a source cancelled
  inside an interceptor before `yield` gives `canceled` with no transport
  call, and expiry during post-processing gives `deadline_exceeded` after a
  successful transport response. Under an accepted cancellation, on Connect
  unary and on native gRPC unary, an interceptor-raised exception reaches the
  caller by identity and the outcome reports that exception's own code, while
  a core-owned transport failure is replaced by `canceled` and reported as
  `canceled`. A client short-circuit returned after the arbiter accepted a
  cancellation raises the accepted reason. On the server, cancellation
  accepted while an interceptor runs gives the accepted reason on the wire,
  with the original exception still recorded.
- Instrumentation timing without sleeps: an interceptor records its own
  monotonic start and end around `yield`, and the outcome's processing
  duration is at least that interval; inside the interceptor, the OTel
  current span is the RPC span.
- Misuse, on both roles: a second `yield`, a captured block called after
  return, and a block called from another thread and from an `Async` child
  task each raise `InterceptorStateError`.
- No-interceptor behavior: the existing suite passes, with any spec changed
  by the newly frozen Connect unary response metadata or the earlier deadline
  start named in the PR.
- Targeted perturbations, each failing at its assertion: reversing chain
  order fails the order spec; allowing a return after a failed continuation
  fails the client and server rule 6 specs; re-raising the handler's original
  instead of the continuation's exception fails the two-layer translation
  spec; classifying an interceptor exception as core-owned fails the
  identity-under-cancellation spec; and removing the single-use check fails
  the second-`yield` misuse spec.
- Static: a client-only interceptor passed to `Server.new` is rejected, as
  the server counterpart of the `Client.new` negative.

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
- Read-side wrappers: a validating wrapper that overrides `receive` and
  `each` rejects an invalid message whether the handler or application reads
  with `receive` or with `each`. The readable endpoints are tested: the server
  `RequestReader` on client streaming, both roles on bidi, and the client
  `ResponseReader` on server streaming. The client `RequestWriter` on client
  streaming has no read methods.
- Escaped wrapper use raises `StreamStateError`; a wrapper `close_send`
  followed by core completion reaches a strict exchange's `close_send` once
  (needs L5). A wrapper missing an endpoint method fails at wrap time.
- Exactly-once cleanup, with counting doubles at the exchange and lifecycle
  boundaries, for four raise points: an interceptor before `yield`, an
  interceptor after `yield`, a `wrap_stream` block and a wrapper method. A
  stream interceptor that returns without yielding raises
  `InterceptorStateError` with the same cleanup. Expectations per role:
  - Server: one terminal, one `context.close`, one session close and one
    `processing_finished`.
  - Client, raise before `yield`: no `open_stream`, no abandon and one
    `lifecycle.finish`, and the call's registration on a shared
    `CancellationSource` is released: cancelling the source afterward invokes
    none of that call's listeners.
  - Client, raise after the stream opened: one abandon or completion, one
    session close and one `lifecycle.finish`, with the same registration
    release check.
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
  the call with the original. Under an accepted cancellation, an
  interceptor-raised exception reaches the caller by identity and the outcome
  reports its own code.
- Pre-session cancellation regression: with a `client_stream` interceptor
  present, a transport whose `open_stream` cancels the shared source and then
  raises `unavailable` still produces `canceled`, as it does today without
  interceptors.
- Client-streaming result: a replacement `Response` returned after a
  successful continuation reaches the caller frozen, including trailers from
  the `Metadata.new` fallback. On the server, a `server_stream` interceptor
  that rescues a handler exception and returns normally still renders the
  continuation's terminal, including an inner layer's translation in a
  two-layer chain.
- Observers: with the real OTel and Sentry bundles, a stream handler bug
  translated to `internal` keeps the original as `Outcome#error` and is
  captured; one translated to `invalid_argument` is not captured.
- Health Watch through a server interceptor on the Async Host.
- Static: the narrowing positive consumer and the `request=` negative.
- Targeted perturbation: skipping the raw-session abandon on an interceptor
  raise after the stream opened fails the exactly-once cleanup spec at its
  assertion.

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
  handler call and no successful response; an interceptor that rescues the
  invoker's `Error` and returns a message still returns the Rails rejection,
  for a `before_action` that renders 401, a CSRF failure and an
  `after_action` that sets 403; a controller exception reaches
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
  diagnostics listed in [Static typing](#static-typing). Each negative is its
  own file under `typecheck/api/negative/`, with any RBS it needs beside it,
  and produces exactly one diagnostic, as `typecheck/negative_check.rb`
  requires.
- **Perturbation.** One per behavior cluster where inspection cannot rule out
  a false positive, as listed in each PR's evidence: chain order, rule 6,
  two-layer translation, failure provenance and the single-use guard in
  PR A, and exactly-once stream cleanup in PR B.
- **Confirmation.** Each PR confirms from runner output that the new specs
  ran, by name or count.

This PR is documentation only. It is verified by link and command review.

## Settled in PR A and PR B

The last capped review of the error component found these questions. The
design does not settle them; the named PR settles each one in its own
contract and proves it with the listed evidence, without changing the
invariants above (rules 4-6, failure provenance, and accepted reasons never
flipping).

| Question | PR | Recommended direction | Required evidence |
| --- | --- | --- | --- |
| Abandonment precedence. `ClientScope` reports `canceled` for any abandoned scope, including a `StreamNotConsumedError` that reaches the caller and an explicit `abandon` followed by a normal return (`client_scope.rb:40`, `:77-79`, `:98-99`). Which wins when an interceptor raises or translates in an abandoned scope? | B | An independent exception escaping the outermost interceptor sets the outcome code; abandonment still sets `canceled` when no interceptor exception escapes, as today | Explicit abandonment, unconsumed completion, a translated `StreamNotConsumedError` and an interceptor raising after `abandon`, each with and without accepted cancellation, asserting the caller result or error, normalized error, outcome code and delivery |
| Delivery for interceptor failures after an exchange. Today's `delivery_completed?` (`client_scope.rb:83-89`) reports `:aborted` when an interceptor raises after a completed exchange, and OTel and Sentry treat that as a real abort | A (gRPC unary), B (streams) | Derive delivery from exchange facts: `:unobserved` when no exchange opened, whoever raised; `:completed` when the exchange completed and only interceptor code failed; `:aborted` only for a real abort | Delivery asserted in the PR A raise-after-`yield` spec, the replacement-then-raise spec and the invalid-replacement spec, and in the PR B pre-`yield` and post-`yield` cleanup specs |
| Operation diagnostics caught inside a scope. A session operation failure that the application or a wrapper catches is recorded immediately and can sit on an `OK` outcome today (`spec/streaming_spec.rb:324`) | B | Keep today's behavior and document it: only failures that escape a continuation are guaranteed to fail the call | A wrapper that catches a rejected `send_message` and continues: the call succeeds, and the outcome matches today's no-interceptor behavior |
| Where the `wrap_stream` factory runs relative to `yield_stream_endpoint`, which decides whether its exceptions count as application exceptions for `implicit_abandonment?` | B | Run it before the application block, outside `yield_stream_endpoint`, and classify its exceptions as independent interceptor exceptions | A raising factory: caller identity, outcome code, `abandoned` state and single cleanup |
| A second `yield` raises `InterceptorStateError` after a successful first one, and the interceptor swallows it | A | Rule 6 applies: core re-raises the `InterceptorStateError` | Misuse spec with the swallow |
| Identity comparison under accepted cancellation needs a non-trivial chain | A, B | Run the "core-owned failure replaced by `canceled`" spec with a pass-through interceptor and with one that does `rescue => e; raise e`, and add the translated-core-failure case from [Errors](#errors) | Both variants report `canceled`; the translation reports the translation |

## Documentation in this PR

Only this file. The guides change with the implementing PRs, so they never
describe interceptors before the code exists.

## Deferred questions

Jim accepted the recommendation for each of these on 2026-11-16. None is part
of the three implementation PRs; each needs its own proposal if demand
appears.

1. **Deadline shortening.** connect-go interceptors can derive a shorter
   context deadline. A Ruby interceptor could shorten, but never extend, the
   call deadline before dispatch, which needs a rule for the wire timeout
   header and for which arbiter owns the shorter expiry. Recommendation:
   defer; the deadline stays fixed when the call starts.
2. **Portable peer facts.** `ServerUnaryCall` and `ServerStreamCall` could
   expose peer facts such as a remote address, as connect-go v2's
   `CallInfo.PeerAddr` does, but Rack and Rails can supply less than the
   native hosts. Recommendation: defer; the Async task-scoped certificate
   accessor covers native mTLS authorization today.
3. **Per-call client interceptors.** gRPC Java attaches interceptors per call.
   Recommendation: defer; per-client lists and per-call metadata cover the
   known uses.
4. **Function shorthand.** connect-go v1 has `UnaryInterceptorFunc`.
   Recommendation: no lambda shorthand; a one-method object is short in Ruby
   and keeps one RBS shape.
5. **Pre-decode server hook.** A separate `server_admit(call)` hook that sees
   only metadata could follow connect-go v2 without changing the typed unary
   model. Recommendation: defer; host middleware covers pre-decode rejection.
6. **Server-side short-circuits and fallbacks.** Server caching and fallback
   interceptors are ruled out by rules 5 and 6 because the invoker can carry
   host policy. They could return if an invoker declares that it carries
   none, for example the default `invoke_handler`, while the Rails invoker
   keeps both rules. Recommendation: defer until there is demand.
7. **Typed stream wrappers.** Structural RBS interfaces for the six endpoint
   classes would let `wrap_stream` and wrappers be checked statically instead
   of by the wrap-time method check. Recommendation: defer; the runtime check
   and nominal application types are enough for the first implementation.
8. **Client fallbacks.** A client unary interceptor that returns a `Response`
   after a failed continuation would need three things: staging every
   lifecycle-handle recording during the chain and clearing it on inner
   recovery, an explicit rule for the delivery and HTTP facts of the failed
   exchange (OTel and Sentry fail any aborted delivery), and a rule for a
   cancellation accepted after the fallback. Recommendation: defer;
   applications rescue around `client.call`.
