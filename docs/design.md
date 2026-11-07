# Design and implemented profile

The [supported 0.x contract](support.md) defines the target compatibility
boundary and the remaining adoption gates.

The gem implements typed Connect RPCs and native HTTP/2 gRPC in all four RPC shapes.
Falcon also serves Connect HTTP/1.1 server/client streaming. The explicit Async
HTTP1Transport supports Connect unary and server/client streaming; standalone
Async Host remains H2-only. The streaming guide records
the response-first discard limits and cooperative 500 ms total final-close
policy. Shared protocol code is independent of HTTP hosting and concrete
protobuf classes. The public require is `connectrpc`, and the Ruby namespace is
`ConnectRPC`. The API is experimental.

## Shared boundaries

`Method` holds validated service/method names, request/response binding types,
`cardinality: :unary`, `idempotency: :unknown` (or `:no_side_effects` / `:idempotent`),
and a case-sensitive path. Only `:no_side_effects` admits GET. `Service` validates its method
set. Streaming uses invariant sibling declarations, keeping unary Method
assignability intact. Generated service glue reuses these objects.
The [streaming guide](streaming.md) owns its scoped APIs, publication/completion
boundaries, finite transport SPI and absolute recovery/tail policies.

`Client` owns request encoding, protocol headers, response validation, errors,
metadata, byte limits and the deadline. Base URL path prefixes are retained.
A binding provides `encode(message, type:, codec:)` and
`decode(bytes, type:, codec:)`. Custom bindings raise `CodecError` for invalid
messages. The optional Google binding delegates binary and
[protobuf JSON](https://protobuf.dev/programming-guides/json/) to native
protobuf APIs. It ignores unknown JSON fields. Scalar wrapper roots use a
standard Any envelope internally to work around protobuf 4.36's direct-root
decoder limitation; the backend still validates the protobuf values.

A transport provides `call(request)` with these host-neutral value objects:

| Object | Fields |
| --- | --- |
| `TransportRequest` | `url` (URI), `http_method`, `headers` (lower-case keys, string arrays), `body` (bytes), `response_limit`, `deadline`, `lifecycle`, `cancellation` |
| `TransportResponse` | `status` (integer), `headers` (repeated string arrays), `body` (bounded bytes) |
| `Response` | `message` (binding type), `metadata`, `trailers` (Metadata objects) |

`TransportRequest.deadline` is a `Deadline` or `nil`. Custom transports must
enforce a supplied deadline and read-only `cancellation` SPI during their owned
I/O and bound the body incrementally.
An absent deadline means no aggregate RPC expiry or wire timeout. Client validation checks their output again but
cannot undo an oversized allocation or interrupt their work.

`Server` owns negotiation, bounded input, decoding, invocation, serialization,
and safe errors. `Rack` only translates request/response HTTP state. It wraps
an optional downstream app and routes `PATH_INFO`; unrelated service paths
fall through, while unknown methods within registered services fail explicitly.
Neither core nor handlers need Rack envs. The optional `connectrpc-rack` package
owns `connectrpc/rack` exclusively and loads core through `require "connectrpc"`.
Applications add `gem "connectrpc-rack", "~> 0.1.0"` while keeping the require
and `ConnectRPC::Rack` API unchanged. Core has no middleware file or runtime
dependencies; requiring middleware without its package raises an ordinary
`LoadError`. Google loads only through `connectrpc/bindings/google_protobuf`;
its missing/incompatible dependency diagnostic remains useful. The middleware
retains its own Rack version activation diagnostic.

A default invoker calls the registered plain handler with `(request, context)`.
An explicit `invoker` callable receives `(method, request, context)` and can
supply framework execution later. Every invocation has a fresh `Context` with
request metadata, response leading/trailing metadata, and optional deadline.
Handlers are reusable; application-owned shared state needs its own locking.

## Wire behavior

The [native gRPC guide](grpc.md) describes protobuf-only native HTTP/2 execution,
protocol configuration and status/trailer policies. The behavior below describes
the default Connect profile; portable adapters support Connect and explicitly enabled gRPC-Web unary.
The [gRPC-Web guide](grpc-web.md) owns its independent media, CORS, bounds and terminal rules.

The implemented [Connect profile](https://connectrpc.com/docs/protocol/) is
unary POST and safe unary GET, identity or opt-in gzip encoding, and `application/proto` or `application/json`.
JSON also accepts `charset=utf-8`. Clients send protocol version `1`; servers
accept an absent version and reject any other supplied value. A 200 response
must match the request codec. Non-application response types produce `unknown`;
mismatched application codecs and unsupported/unadvertised response encodings produce
`internal`. JSON errors are only parsed for an appropriate JSON content type.
Single identity/gzip content codings are case-insensitive; empty, repeated,
stacked and unknown content codings are rejected. Identity-only servers continue
to reject gzip. Client response preferences use distinct symbol arrays and
normalize identity to the last fallback. Server gzip capability negotiates
supported nonidentity accept tokens before identity, ignoring unknown/quality
value tokens. Absent accept inherits request coding; empty accept selects identity.
Repeated accept fields combine in field order. Gzip-selected empty serialization
produces a valid nonempty member; zero-length HTTP content bypasses inflation
after header validation. Every concatenated member validates CRC/ISIZE/completion;
truncation and trailing data fail under an aggregate decoded cap.

Other verbs get bare 405 with `Allow: GET, POST` for safe methods, otherwise `POST`; unsupported types, including
Connect streaming and gRPC variants on portable adapters, get bare 415 with `Accept-Post`.
Unsupported request encoding returns `unimplemented` listing supported codings. Invalid
version, timeout, or request decoding returns `invalid_argument`. Limits return
`resource_exhausted`. Unexpected application exceptions return generic
`internal`; deliberate `Error` messages are application-controlled safe text.
Standard Connect-to-HTTP and fallback HTTP-to-Connect tables are used. Null, absent and unrecognized wire codes use the HTTP fallback while preserving
valid message and details. Malformed bodies, code types or details use the
HTTP fallback without decoding those error fields. Error details preserve a type name and binary bytes, with
unpadded base64 on output and validated padded/unpadded input.

`Metadata` stores lower-case keys and repeated printable ASCII values and
rejects protocol/framing overrides and header injection. Binary `-bin` helpers
handle base64 and comma coalescing. Trailing metadata becomes `trailer-`
response fields. Other fields are never blindly comma-split. The middleware
emits separate arrays under `Rack::Lint`, and NetHTTP preserves raw repeated
response fields. NetHTTP joins outgoing request arrays with comma+space.
rackup/WEBrick additionally coalesces incoming and response fields; interop
asserts those observed values explicitly. Received fields outside the metadata
name/printable-ASCII contract are omitted during extraction, including an
entire field with any invalid repeated value. Valid adjacent leading/trailing
fields survive. Protocol validation is separate, and outbound metadata remains
strictly validated.

GET uses the existing codec, compression, deadline and metadata paths. The
client opts in with `use_get: true`; `get_url_limit: 8192` bounds the complete
absolute URI before transport dispatch, without fallback. Canonical controls
are `connect=v1`, optional `base64=1`, optional `compression=gzip`, `encoding`,
and `message`. Binary/gzip use unpadded URL-safe base64; JSON identity uses
UTF-8 form encoding. Input accepts padded URL-safe base64, arbitrary parameter
order, unknown/repeated unknown fields and binary unknown values. Form decoding
happens once: `+` is space, `%2B` is plus, and `;` is literal data.

The bounded parser validates escapes everywhere and rejects duplicate known
names after name decoding as a local ambiguity policy. Only text message values
require UTF-8. Query syntax errors can precede codec rejection. Missing or
unsupported query encoding gets bare 415. Content-Length syntax follows, then
positive declared/actual content gets bare 415 after at most one requested
byte. Query compression, version, message presence and deferred timeout errors
follow in that order. Missing/empty compression means identity; missing/empty
connect version is accepted. Other supplied versions must be `v1`. Query
controls supersede irrelevant POST protocol headers. Valid deadlines start
before parsing and are checked cooperatively throughout processing.

Unsafe GET gets bare 405 before query/header/input work. An unknown owned
suffix can negotiate GET and then return `unimplemented`; it never invokes a
handler or supplies a fabricated safe Method to observation. Every GET response,
including bare errors, merges Vary after application/error metadata, preserving
case-insensitive tokens and wildcard `*`. Cache-Control belongs to the app.
Native NetHTTP/Async requests have no body or synthetic content headers.

`Server.call(query: "")` keeps path and raw query separate. The raw-query default
is `3 * (4 * ((validated_request_wire_limit + 2) / 3)) + 1024`, including fully
percent-escaped padded base64 and bounded parameter overhead. It is independent
of decoded query payload and inflated message caps. Parsing retains only five
controls, validates ignored values without storing them, and checks expansion
before appending. Incoming URLs and native serialization already exist before
these checks; no total allocation bound is claimed.

GET request byte observation measures the percent/base64-decoded transport
payload before inflation. Unknown measurements stay nil; only measured empty
payloads are zero. No URL, query or payload enters RPC observation. Host/proxy
logs and generic HTTP tracing remain application-owned. Ruby's official native
protobuf encoder has no deterministic flag; same-object within-process encoding
and gzip are tested, but map encodings vary across processes. No semantic or
cross-process canonical cache-key promise is made.

## Bounds and lifecycle

Default serialized/decompressed request/response limits are 4 MiB each.
Independent wire limits default to the corresponding message limits, preserving
both bounds for existing callers. `TransportRequest.response_limit` is the
encoded response read cap. Declared lengths are validated against wire bytes and
oversized lengths rejected before reading. Unknown-length input stops at the
first excess byte. Identity satisfies both caps. Outgoing serialization is checked
before compression; compressed output, including framing/footer, must fit its
wire cap without fallback. Explicitly larger wire limits permit small-message
expansion without increasing message bounds.

Gzip uses default Zlib gzip-only streams with bounded input and block output,
checking size before every append. At most one 16 KiB native output callback can
exceed retained payload, plus native state. Every member/chunk checks cooperative
expiry; streams close on every outcome. Wire and decoded buffers coexist, and
native protobuf/JSON serialization can allocate before its output check. Custom
transports may allocate before returning; core revalidation cannot undo that.
There is no total allocation, hard CPU or independent header-size guarantee.

Server errors remain identity and use the smaller response cap, including the
safe replacement and empty-429 fallback. Valid gzip peer errors are decoded
before JSON parsing. Malformed gzip or JSON on non-200 uses HTTP fallback with
valid metadata/trailers; malformed gzip 200 is internal. Header rejection stays
internal, local size excess stays resource_exhausted, and actual expiry stays
deadline_exceeded. No generic error-body rescue changes those classifications.

The default timeout is 30 seconds. Explicit `timeout: nil` means no logical
client deadline, timeout header or expiry watchdog. Native operation waits still
use the exact `9_999_999.999` second cap, returning `unavailable` on timeout.
Sustained progress can outlast this per-operation cap. For finite positive
timeouts, one monotonic client deadline begins before encoding. Remaining timeout milliseconds are
rounded up and clamped to ten digits without extending actual expiry. NetHTTP
recomputes the header at dispatch and checks the budget around connection,
codec and body progress. Each call owns one connection; a finite deadline also owns one watchdog. The
watchdog closes the owned raw socket on expiry, including during header
trickle, stalled reads and blocked writes, and is joined on every exit.
NetHTTP depends on the pinned stdlib BufferedIO socket layout for that close.
Request preparation finishes before connecting, with an expiry check afterward.
The connection checks expiry and recomputes the wire timeout immediately before
dispatch, and rejects Net::HTTP's implicit reconnect of a closed socket. This
keeps every dispatched byte on the one socket owned by the watchdog.
TCP/TLS/read/write timeout arguments and watchdog waits are capped at exactly
`9_999_999.999` seconds, the Connect wire maximum, for safe native time conversion.
Individual native operations are independently bounded: an operation timeout
before logical expiry returns `unavailable`, while actual expiry returns
`deadline_exceeded`. Logical expiry remains unchanged; the watchdog re-evaluates
it after each capped wait and only closes the socket at actual expiry. Local
huge-budget calls verify conversions on the exercised plain TCP paths, not an
actual 115-day duration or runtime TLS behavior.

DNS/native resolution may outlast the budget. Net::HTTP's internal connection
implementation can itself use `Timeout.timeout` on Ruby 3.4; ConnectRPC never
calls it or asynchronously interrupts application code. TLS handshake timeout
uses the remaining budget after TCP/DNS. Peer/hostname verification stays on.
There is no pooling, proxy-env routing, retries, redirect following, automatic
decompression. NetHTTP constructs requests with the actual explicit accept
advertisement so the default HTTP gem never transparently inflates or rewrites
content encoding/length; transports carry opaque encoded bytes.

Server timeout validation creates a per-call monotonic deadline. Checks run
around input, decoding, handler execution and encoding. `cancelled?` combines explicit cancellation and expiry;
`cancellation_reason` identifies `:canceled` or `:deadline_exceeded`, and handlers
can call `check!` cooperatively. No header means no server
expiry. Synchronous Rack cannot interrupt blocked input, arbitrary handlers or
codec work, or detect every disconnect. No hard server cancellation is claimed.

## Explicit cancellation ownership

`CancellationSource` alone initiates permanent cancellation. Its read-only
`CancellationToken` can be shared across calls. `cancel` returns true only on
its first transition. Registration and invocation claims are synchronized;
callbacks run outside locks in the cancelling thread. Already-cancelled
registration invokes before returning. `unregister` returns true only for a
pending registration, suppresses even snapshotted pending callbacks and releases
references. Running/finished/unregistered listeners return false without waiting.
Listeners must be short and nonblocking; `StandardError` does not stop siblings.
Internal resource retirement separately guards callbacks that already started.

Client and Context each own distinct terminal arbitration. While open, they
accept an already-expired real deadline first, then directly observed source
cancellation, then a result/error. Accepted reasons never flip. A source's
transition requests cancellation; its timestamp does not determine the call's
serialized winner. Completion directly rechecks source state, including after
decoding or exceptional codec/transport paths. HTTP response receipt alone
cannot complete the client. Instrumentation and callbacks run outside state
locks, and only the executing fibers complete instrumentation. Original failures
remain available to lifecycle observers; normalized canceled/deadline/transport
errors have fixed safe messages and no sensitive rescued cause.

`TransportRequest.deadline` remains the actual Deadline or nil.
`TransportRequest.cancellation` implements `_Cancellation`: `cancelled?`,
`reason`, `check!`, and `on_cancel`. Transports only observe this call-owned SPI;
they cannot complete the client. Server accepts the same read-only seam and
creates its own processing arbitration. Testing forwards it without completing
the client's state. Context exposes its combined reason, with no token reader.

NetHTTP installs socket ownership before startup, attaches raw TCP at the TLS
hook or after plain startup, and closes a newly exposed socket immediately if
cancellation already won. TLS/read/write/header waits are interruptible; native
DNS/TCP before exposure remains cooperative. Per-call retirement prevents late
callbacks from accessing completed/recycled resources. Nil deadlines have no
watchdog or wire timeout; the exact independent native cap remains
`9_999_999.999`. Retry/reconnect guards and TLS verification remain enabled.

Async foreign-thread listeners only resolve a tagged Promise. Its owner reactor
arbitrates, stops private operation/setup tasks and performs bounded cleanup.
Promise resolution order does not decide the RPC outcome. Healthy stream reset
preserves the shared pool and siblings; a stuck deferred frame retains the one
100 ms cleanup budget and raw-first whole-connection fallback.

Native HTTP/2 creates a request cancellation source before processing,
independently of instrumentation. Only peer RST_STREAM with CANCEL (0x8) signals
it, before upstream input teardown can resume reads. Exceptional input paths
record their original failure and check cancellation before normalization.
Processing detaches this source at completion; delivery observation continues
independently. NO_ERROR/other resets, END_STREAM, half-close, EOF, connection loss
and host/fatal-owner abort add no explicit cancellation guarantee. Portable
Rack/Rails and native Falcon HTTP/1 add no token injection or peer detection.

## Evidence and future direction

Specs exercise real custom bindings/transports/invokers, ProtoJSON mapping,
Rack::Lint success and errors, real localhost HTTP, byte limits and lifecycle.
The pinned connect-go Client and Handler verify both cross-language directions
and codecs against one shared descriptor fixture, including details,
host-visible metadata and a request limit. The same Go client scenarios also
run against the Go server as a reference baseline, with raw repeated Go fields
instead of WEBrick's coalesced values. There are 18 fixture cases: six reference
baseline cases and twelve cross-language cases. This is a unary subset, not
full conformance. The separate [official conformance harness](../conformance/README.md)
requires 96 client and 90 server identity passes, including two GET successes
in each direction. A separate gzip inventory requires 88 client and 82 server
passes. All four caller-cancellation cases pass unwaived with actual request
writes and socket cleanup, for 184 client and 172 server passes in total.
Separate Go gzip scenarios
exercise both directions/codecs, negotiation, errors and bounds with wire
observations. The pinned Go client rewrites accept preferences, so independent
raw Go HTTP requests additionally prove gzip-request/identity-response behavior.
The pinned Go server prefers request gzip over explicit identity acceptance;
Ruby clients reject that unadvertised response. There is no full-conformance claim.

Wire protocol, HTTP transport, and message binding remain separate choices.
gRPC/gRPC-Web, interceptors, a production server,
and Faraday are future work. Unsupported modes/options are rejected
rather than represented as buffered streaming. The optional [connectrpc-rails gem](../gems/connectrpc-rails/README.md) uses
the invoker boundary to run native controller callbacks, authentication,
`rescue_from` and instrumentation after bounded core decode. It depends on `connectrpc-rack` for Rack translation and keeps hosting
dependencies out of core. It preserves native
CSRF/session policy, reports unexpected errors through Rails.error and keeps
generated files non-reloadable outside Zeitwerk paths. A mounted Rack app alone
does not supply the Rails controller lifecycle.
See the [Rails controller guide](https://guides.rubyonrails.org/action_controller_overview.html).

The optional [connectrpc-async package](../gems/connectrpc-async/README.md)
selects Async HTTP/2 for standalone unary/streaming hosting and a matching pooled transport.
It uses the existing transport/server seams, generated definitions and Google
binding without changing core Context or wire contracts. The application owns
the reactor and close lifecycle. Private, version-bounded wrappers own writes,
stream cancellation, request tasks and wire-complete drain; no frame/HPACK
engine or global monkeypatch is added. Real h2c and verified TLS `h2` are
required. Full TLS snapshots enable required client authentication, same-listener
credential reload and older-generation retirement. The exact-task client leaf
accessor is the optional authorization seam; peer-presented chains are not
claimed to be verified chains. See the package guide for session isolation,
cleanup's 100ms allowance and deliberate whole-connection failure for a stuck
frame. Native streaming uses the same ingress and cleanup boundaries. No production daemon claim is made.

Connect streaming carries end-stream metadata in its body; future gRPC has
different [trailer requirements](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md).
The Async backend preserves leading repeated fields and leading `trailer-*`
metadata while ignoring actual trailing HEADERS after backend validation.
It reconstructs lengths normalized by the backend; malformed lengths and some
singleton repeats can fail before core. Conservative frame/window settings do
not provide a hard process-memory/header-size guarantee. Independent connect-go
unary h2c/TLS/mTLS results are separate from core's identity and gzip conformance
profiles. Combined gzip specs exercise opaque bytes and wire/decompressed bounds
over the actual Async host and transport. NetHTTP remains HTTP/1.1; each future
transport needs its own verified capability scope.

The reusable `ConnectRPC::Async::Application` translates native Protocol::HTTP
requests independently of hosting. `TLSConfiguration` owns complete versioned
provider results and immutable worker-local compilation; `ServerSession` owns
raw-accept generation capture, setup/admission, exact-task identity and reviewed
HTTP/2 resource cleanup. The strict standalone Host composes that session.
The optional [Falcon package](../gems/connectrpc-falcon/README.md) composes it with
Falcon's process workers, raw endpoints and HTTP server/run lifecycle. Its local
HTTP/1 resource uses upstream parse/write hooks and preserves wire completion
and callback boundaries during retirement. A private response-body EOF marker
pairs with the pinned writer's next successful final flush to establish one
absolute 500 ms final-close expiry before callbacks. Successfully delivered
retiring Connect responses half-close raw TCP writes, retain the original
100 ms / 256 KiB bounded discard, then wait only the remaining total budget
without further reads. Admission/socket ownership joins original processing and
retirement; owner shutdown can shorten either phase. These cooperative timers
can overshoot under scheduler delay and do not guarantee peer receipt. The
independent Web rejected-input policy is unchanged. Only explicit Rack fallback
receives Falcon's generic encoding/cache middleware. Native Connect streaming hosts
select explicit core shape capabilities. See the package guide for source failure, expiry, worker
stop and unsupported socket-ownership transfer policies.

The explicit optional `connectrpc/generator` entry point implements the standard
protoc request/response protocol through official `google/protobuf/plugin_pb`.
The extensionless executable reads raw protobuf to EOF, with no conformance
length framing. Malformed protobuf, missing/incompatible Google and CLI arguments
fail on stderr with nonzero status and empty stdout. Unsupported descriptors
return `CodeGeneratorResponse.error`, no partial files and exit zero. Unexpected
implementation exceptions remain visible. Only `FEATURE_PROTO3_OPTIONAL` is
advertised; editions are unsupported.

Each service-bearing target emits `_connect.rb`, requiring core, its official
`_pb.rb` and any additionally referenced message files. Imported streaming
services are not targets. Generated modules
contain frozen RPC declaration constants, `SERVICE`, a `Client` initialized with
`client:`, and `.handlers` with required callable RPC keywords. Unary wrappers forward `call(METHOD, request, **options)` unchanged. Streaming
wrappers forward the matching scoped operation, its required block and all options.
Partial handlers/custom invocation remain core Server features. No framework,
transport configuration or binding auto-require is added.
Optional [RBS signatures](rbs.md) preserve these runtime boundaries and require
`rbs=true` for service signature generation.
Concrete types come from the official generated descriptor pool, preserving
nested classes, imports, proto2/proto3 presence and `ruby_package` mappings.

Naming follows protoc 36.2's
[RubifyConstant and PackageToModule](https://github.com/protocolbuffers/protobuf/blob/v36.2/src/google/protobuf/compiler/ruby/ruby_generator.cc):
message/service names preserve underscores and use `PB_` for a nonalphabetic
initial character; package underscores delimit capitalized words. An explicit
`ruby_package` containing `::` is used component-wise; otherwise dot/package
conversion applies. RPC names use the gRPC Ruby acronym and lower/digit-to-upper
underscore rule. Keyword/inherited wrapper names (including private copy/object
hooks and implicit conversions), leading-underscore RPCs,
`BEGIN`/`END`, reserved constants, unsafe paths, unresolved/non-message types,
duplicates and namespace/output collisions fail before emitting files.
All runtime constant references are absolute. Namespace checks index the request's
package/message/enum graph and target services, including Ruby aliases and
class/module conflicts. Fixed runtime protection applies to package ancestors,
official message/enum assignments and target service declarations in the loaded
import/type closure. Compatible `Google`/`Google::Protobuf` package modules may
be shared, including equivalent `ruby_package` mappings. Direct replacement of
those modules, reserved roots such as `ConnectRPC`/`File`, and concrete protobuf
runtime names such as `Google::Protobuf::DescriptorPool` are rejected. Known
official protobuf types are accepted only under their canonical file, proto name
and Ruby namespace, preserving WKT imports without allowing application types to
replace them. The fixed policy follows the pinned protobuf sources, rather than
reflecting the generator process's constants. A service cannot merge with
another package. No global registry spans generator invocations.
Generated files guard directly defined `SERVICE`, `Client` and RPC constants;
normal `require` is idempotent, while deliberate `load`/redefinition fails clearly.
Application constants outside the request still need compatible namespaces.

A Rust descriptor-driven
backend using [prost-reflect](https://docs.rs/prost-reflect/latest/prost_reflect/)
remains an option; cross-backend message-class compatibility is not promised.
No custom protobuf parser is implemented. See the
[Ruby generated-code guide](https://protobuf.dev/reference/ruby/ruby-generated/)
and [Rack specification](https://github.com/rack/rack/blob/v3.2.7/SPEC.rdoc).

See the optional [in-process testing package](../gems/connectrpc-testing/README.md) for real encoded client/server round trips without sockets.

## RPC lifecycle observation

`Instrumentation.new(subscribers:, on_error: nil)` freezes a copied subscriber
list. `start(call)` returns a per-call observer. Optional callbacks are fixed:
`attach` returns a local activation token; `detach(token)` unwinds successful
attachments in reverse registration order; `processing_finished(outcome)`
receives an immutable final processing snapshot; `finished(completion)` receives
one terminal processing/delivery observation. Core owns one application yield.
Each callback/reporter StandardError is isolated. Fatal exceptions retain Ruby
semantics. Observers must make attach/detach exception-safe: the dispatcher
cannot repair private context state mutated before a failing attach returns a
token. Retained observers hold no globally active core context.

Call facts contain role, Connect protocol, recognized immutable Method or nil,
client endpoint host/port and a propagation carrier, never message objects.
The client starts before timeout/metadata validation and applies validated staged
injection transactionally to copied metadata. Server extraction uses defensive
read-only headers without moving validation ahead of 405. Outcomes distinguish
original exceptions, normalized/replacement exceptions, emitted Connect code,
HTTP status, monotonic processing duration and measured encoded byte counts.
Unknown byte counts are nil. Metadata snapshots retain repeated arrays.

`Server#start_call(path:, headers:, http_method: "POST")` is the explicit host
SPI. Hosts pass the actual method so Connect GET selection precedes subscriber
start, regardless of ignored POST content headers. It resolves a known method
and starts observers without reading input or validating protocol. The default
preserves POST behavior for existing callers; subclass overrides must accept
and forward the new keyword.
`Server#call(..., lifecycle: handle)` consumes that Server/path-bound handle once
and leaves terminal ownership to the host. `handle.activate { ... }` scopes
context to the executing task; `handle.finish(delivery:, error: nil,
decision: :processed, http_status: nil)` terminates idempotently. Ordinary
Server#call owns start/processing/unobserved completion. Optional
`TransportRequest.lifecycle` lets a transport activate independent RPC tasks.
`Context#record_failure(error)` retains the first original escaping failure
before framework normalization. It does not itself complete a call.

Processing state (`not_started`, `running`, `finished`), admission/host decision
and delivery observation are independent. A host abort while processing runs
is latched until processing cleanup, with processing callback before terminal
callback. Terminal state is committed before observers run, preventing reentry.
Admission-only calls have no fabricated processing result. Portable Rack
response-finished callbacks take precedence over body close and have their own
label; they never claim native full-write completion. A consumer that neither
closes a body nor runs promised callbacks can leave observation unfinished.
Native Async/Falcon own explicit request exchanges before admission, independent
handler activation and write/reset/cleanup evidence. Connection setup before an
RPC exists remains outside this scope. See the
[OTel guide](../gems/connectrpc-otel/README.md) for trace semantics, privacy,
HTTP instrumentation nesting and synchronous processor costs.

## Explicit HTTP/1.1 client ownership

The Async package's independent `HTTP1Transport` uses exclusive reactor-owned
leases for Connect unary, safe GET and SS/CS. Reservations precede yielding
TCP/TLS setup; retiring resources count toward the cap until raw close and task
joins finish. Idle reuse requires normal writer completion, full HTTP EOF,
successful cleanup and native persistent/idle state. A nonblocking acquisition
probe retires FIN or unsolicited data before dispatch. No failed call is replayed.

A private H1 stream separates response reads from output flushing and caps native
body fragments at 16 KiB. Strict HTTP/1.1 status syntax preserves three-digit
extension statuses for core fallback. Framing validation precedes native header
mutation; leading metadata is snapshotted before chunk trailers. Core retains
terminal decoding and send/error arbitration. Complete unary non-200 responses
stop unfinished uploads destructively and return to core; HTTP 200 requires
writer success. Cleanup failures cannot turn into successful completion.

TLS offers only `http/1.1`, accepts nil or H1 ALPN, and rejects other negotiation
before dispatch. Hostname/peer verification and mTLS remain enabled. The shared
private TLS helper retains H2's existing default. H1 changes no server runtime
policy, H2 connection ownership, or Web rejection behavior.
