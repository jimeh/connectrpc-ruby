# Public API signatures

Every package ships its own gem-discovered signatures and dependency manifest.
Core also generates unary service signatures. RBS
and Steep are development tools; neither is required by the runtime. Default
Connect generation produces exactly the same Ruby files as before. Only the
plugin parameter `rbs=true` adds `<stem>_connect.rbs` for each service-bearing
input. Unknown, duplicate, invalid and trailing parameters fail without files.

## Generate messages and services

Use protoc 36.2, google-protobuf 4.36.2, RBS 4.2.0 and Steep 2.1.0 for the
tested upstream message-signature path. Create `generated/`, then run the
installed plugin alongside the official Ruby and RBS generators:

```sh
protoc -I proto --ruby_out=generated --rbs_out=generated \
  --connectrpc-ruby_out=rbs=true:generated example.proto
```

Use schema-relative input names with `-I`. Also generate
`--rbs_out` for imported message schemas. If your schema imports Timestamp or
Empty, include `google/protobuf/timestamp.proto` and
`google/protobuf/empty.proto` as additional compiler inputs. Their official
Ruby implementations may come from google-protobuf; their signatures must enter
the RBS environment too. The compiler's standard include directory supplies
these schemas. Other imports need their normal `-I` directories.

Buf 1.75.0 supports the official RBS builtin. An equivalent v2 template is:

```yaml
version: v2
plugins:
  - protoc_builtin: ruby
    out: generated
  - protoc_builtin: rbs
    out: generated
  - local: protoc-gen-connectrpc-ruby
    out: generated
    opt: rbs=true
```

Buf generates local inputs by default. Generate imported/WKT signatures with
protoc as above or include their schema closure in your generation inputs.
Connect emits service signatures only. It never generates or rewrites message
fields, constructors or official `_pb.rbs` output.

## Check an application

Select the small protobuf bridge explicitly in your Steepfile. It lives outside
the gem's automatically discovered `sig/`, so `library "connectrpc"` introduces
no Google declarations. With the gem installed, use a path relative to your
Steepfile for the bridge (Steep signature patterns use project-relative paths):

```ruby
require "pathname"
bridge = Pathname(Gem::Specification.find_by_name("connectrpc").full_gem_path)
  .join("rbs/google_protobuf_4_36")
  .relative_path_from(Pathname(__dir__)).to_s

target :app do
  library "connectrpc"
  signature "generated", "sig", bridge
  check "app"
  configure_code_diagnostics(Steep::Diagnostic::Ruby.strict)
end
```

Run `bundle exec steep check` with your application's development bundle.
Strict diagnostics are required: Steep's defaults classify some incorrect
request constraints and assignments as hints, which do not fail a normal check.
The repository's `typecheck/consumers/` has executable positive and separately
checked negative examples, including concrete callable objects and inline lambdas.

The bridge is deliberately limited to the shapes emitted by protoc 36.2:
`AbstractMessage`, the empty `_MessageClass[T]` compatibility marker,
`RepeatedField` indexing/writing/appending, `Map` indexing/writing, enum
lookup/resolve, and descriptor-pool lookup/message-class/enum-module access.
Upstream literal lookup overloads retain specific message/enum tokens; arbitrary
lookup results stay opaque because the runtime can also return other descriptors.
It promises no collection iteration, Enumerable or Enumerator behavior. The
marker does not promise codec methods. The binding signature erases custom
runtime tokens at its encode/decode boundary instead of inventing a generic
decode association.

Official field signatures are best effort, as their generated header states.
They do not validate numeric ranges, serialization, every protobuf runtime API,
or arbitrary custom bindings. Do not combine this bridge with the old
collection google-protobuf 3.22 signatures, whose collection shapes differ.
Applications supplying protobuf runtime signatures must omit the bridge.

## Keep application-owned message signatures

Existing application message signatures work without the bridge. Alternatively,
minimal nominal declarations check RPC identities, and need no custom
`initialize` declaration:

```rbs
module Example
  class Request
  end
  class Reply
  end
end
```

Declare every referenced message class, including imported/nested types, in
your application's `sig/`. A separate field extension remains application-owned:

```rbs
class Example::Request
  attr_accessor text: String
end
```

Use these declarations instead of duplicate official message signatures. Nominal
identities alone do not check field or constructor arguments. The repository's
`typecheck/nominal/` checks this alternative with a separate field extension.

## Associations and typed construction

Generated method constants describe `Method[Request, Reply]`; generated client
methods accept Request and return `Response[Reply]`. Both Method and Response
are invariant. Handlers receive `(Request, Context)` and return the reply
message, rather than a Response envelope. Their argument signatures accept a
concrete proc or a signed callable object. The proc arm lets Steep infer inline
lambda request/context types. Returned handler-map values retain `call`;
heterogeneous maps do not encode a particular key's method association. The
core Server registration map erases those associations only after generated
`.handlers` has checked each callable.

Method's `request_type` and `response_type` are arbitrary binding tokens typed
as Object. A manual `Method.new` cannot infer instance types from those tokens.
Supply an explicit application assertion after verifying your binding contract:

```ruby
# @type var method: ConnectRPC::Method[Example::Request, Example::Reply]
method = ConnectRPC::Method.new(service: "example.API", name: "Call",
                               request_type: Example::Request,
                               response_type: Example::Reply)
```

This annotation asserts the association; it does not prove that a custom
binding honors it. Generated constants establish it from schema information.

Response, TransportRequest and TransportResponse remain mutable Ruby Structs.
Fully initialized `new` and class `[]` calls return precise records. Response's
message type is inferred, and Client still returns `Response[Reply]` with
non-null Metadata and trailers. Omitted or nil fields produce a partial-record
interface with nullable readers; a supplied Response message retains its own
type. Keyword arguments and positional keyword Hashes are both accepted.

```ruby
complete = ConnectRPC::Response.new(message: reply,
  metadata: ConnectRPC::Metadata.new, trailers: ConnectRPC::Metadata.new)
complete[:metadata].to_h
partial = ConnectRPC::Response[message: reply]
partial.metadata&.to_h
partial[:metadata] = nil
# complete[:metadata] = nil # Rejected: precise records retain their field types.
```

Field readers, writers and literal Symbol/String/integer indexing preserve each
field's type, including negative indices. Common read-only Struct operations
(`to_h`, `members`, `size`, `length`, `to_a`, `values`, `each`, `each_pair`,
`values_at`, `deconstruct`, `deconstruct_keys`) are declared. Unrestricted
reflection, such as `send` or `instance_variable_set` that invalidates precise
fields, is an application escape boundary. Arbitrary computed member names,
recursive `dig`, block-form `to_h` and custom Struct subclass behavior are outside
this declared surface. These signatures do not change Ruby's runtime mutation.

## Unary GET options

`Method#idempotency` is the immutable `:unknown | :no_side_effects | :idempotent`
enum. Generated constants preserve standard protobuf method options. Core
Client accepts `use_get: bool` and `get_url_limit: Integer`; Server accepts
`get_query_limit: Integer?` and `call(query: String)`. Runtime constructors still
validate booleans and positive integer bounds. Rails `connect_limits` also
accepts `get_query_limit`. Generated wrappers keep their existing call options.
The strict implementation target includes the private GET parser and builder;
manual/generated positive callers and exact negative diagnostics cover these
options alongside installed package discovery.

## Package APIs and extension contracts

Use the installed package names in a Steepfile:

```ruby
target :app do
  library "connectrpc"
  library "connectrpc-rack" # Only the integrations in your application bundle.
  library "connectrpc-testing"
  library "connectrpc-rails"
  library "connectrpc-async"
  library "connectrpc-falcon"
  library "connectrpc-otel"
  check "app"
  configure_code_diagnostics(Steep::Diagnostic::Ruby.strict)
end
```

Each optional manifest resolves only its dependency closure. Core declares no
Google runtime or optional hosting/tracing namespace and has no runtime tool
dependencies. Its GoogleProtobuf binding class signature is available without
the explicit message-runtime bridge; the binding itself still requires the
optional runtime require.

| Owner | Supported API and extension SPI |
| --- | --- |
| Core | Method, non-generic Service, Client/Server options and calls, records, Metadata, CancellationSource/Token/Registration, Deadline/Context, Error/ErrorDetail, explicit NetHTTP and Google binding entry points |
| Core SPI | `_Cancellation`, `_Binding`, `_Transport`, `_Input`, `_Handler`, `_Invoker`, callable clocks/error handlers, `_Subscriber`, four separate optional observer callback capabilities, host lifecycle Handle |
| Rack | Middleware constructor/call and shared `_App`, `_Body`, `_CallableBody`, `_Stream`, env and response contracts |
| Testing | In-process Transport constructor/call, reusable as the core transport |
| Rails | Controller inclusion, `connect`, documented private service/binding/handlers/limits/instrumentation overrides and nullable request/context/method readers |
| Async | Application/native HTTP capabilities, Host, Transport, ServerSession/accepted handles, TLSConfiguration, complete static/provider record shapes, status/error callbacks, rotation/drain/identity |
| Falcon | Endpoint's supported operations, Environment overrides and Server/native application/TLS lifecycle |
| OTel | Subscriber configuration and narrow tracer-provider/tracer/span/propagator capabilities; SDK and context implementation remain application owned |

Constructors include their actual options and return types. Type checks reject
wrong field, callback and configuration types. Runtime checks still enforce
names, byte limits, finite numeric ranges, required TLS key presence, TLS/wire
validity, reactor affinity, admission and lifecycle ordering. RBS records with
nullable fields do not prove those keys were explicitly present. There is no
claim of fully type-safe RPC execution.

Instrumentation subscribers return an opaque object or nil. They may implement
any subset of attach, detach, processing_finished and finished. The separate
`_ObserverAttach`, `_ObserverDetach`, `_ObserverProcessingFinished` and
`_ObserverFinished` interfaces let applications deliberately check a capability;
returning an arbitrary object from `start` does not automatically validate all
its optional callbacks. Core resolves methods once into private typed callback
records at one reflection boundary. Call, Outcome and Completion are shallowly
immutable Data values. In particular, the client Call's carrier remains mutable
for validated propagation injection. Handle exposes `activate`, `finish`,
`record_failure`, `host_rejected!` and `host_rejected?` for hosting. Construction,
processing bookkeeping and Instrumentation.start/safely/carrier are internal.

Optional class signatures describe ConnectRPC-owned supported operations. They
do not promise full upstream framework inheritance or describe all Rack/Rails,
Async/Falcon or OpenTelemetry APIs. No broad external namespace declarations are
shipped through normal discovery. Falcon checks fallback status, scalar/repeated
headers and enumerable/callable response bodies using small local structural
capabilities. These also accept `ConnectRPC::Rack::_App`; the Falcon closure does
not require the `connectrpc-rack` gem. Async owns shared native HTTP/TLS
capabilities reused by Falcon.

## Source inventory and implementation checks

The reviewed `typecheck/surface.json` records definitions, attributes, visibility,
constructor parameters/defaults and Struct/Data members across all maintained
library source. Each entry is explicitly supported, extension SPI or internal.
`rbs:inventory` checks the syntax tree against that inventory and requires shipped
declarations for supported members, including record readers/writers. A new
source method, changed option/visibility/member or omitted signature fails.
Review source and docs before updating an inventory entry; Ruby-public helpers
are not automatically compatibility promises. This is a practical source drift
check, not semantic proof of arbitrary Ruby metaprogramming.

Protocol, Compression, MessageIO, Method.validate_name!, private callback records,
core processing helpers, socket internals, framework glue internals and generator
implementation helpers stay outside the supported API. Generator invocation and
schema-generated service APIs retain their existing contract and parity checks.

`typecheck/Steepfile` checks actual core source and private state under
`Steep::Diagnostic::Ruby.strict`: method, metadata, error, deadline, protocol,
client, server, instrumentation, compression message_io, cancellation and get_query, plus entry/version.
`rbs:source` validates the resolved source list and Ruby syntax before checking.
Its implementation-only declarations live outside every gem's `sig/` directory.
Erasure is limited to custom bindings/heterogeneous messages, external JSON and
optional observer discovery. The internal Numeric arithmetic declaration fills
a missing upstream base signature; custom numeric arithmetic remains a runtime
requirement. Narrow assertions record established phase, slice and format facts.

NetHTTP socket/watchdog implementation, Google codec internals, generator
internals and optional backend/framework implementation checking are later stages.
Their supported public signatures and consumer checks are included here.

Callable-body stream capabilities include the standard Rack read, write, `<<`,
flush, close, half-close and closed-state operations. Native HTTP header
capabilities return an Enumerator of name/value tuples. Struct `each_pair`
yields one name/value tuple, which a block may explicitly destructure.

Custom OpenTelemetry providers must supply the finite span-context capabilities
used by the default W3C propagator: valid state, raw trace/span ids, sampled
flags and serialized tracestate. These declarations introduce no SDK or upstream
namespace; static types do not prove valid id lengths or sampling policy.

### Opt-in framework compatibility

Rails inheritance and Falcon's configuration DSL need declarations for upstream
classes that currently ship no RBS. Each integration packages a finite bridge
outside its automatically loaded `sig/`:

| Package | Explicit signature directory | Tested upstream versions |
| --- | --- | --- |
| `connectrpc-rails` | `rbs/action_controller_8` | actionpack 8.0.5.1 and 8.1.4 |
| `connectrpc-falcon` | `rbs/async_service_0_25` | Falcon 0.57.0, async-service 0.25.0 |

Add only the selected bridge paths to an application's Steep target, using the
same gem-relative `Pathname` pattern as the Google bridge above. Omit these
bridges when the application already supplies declarations for those upstream
classes. Normal library discovery introduces neither `ActionController` nor
`Async::Service` declarations.

The Rails bridge covers nominal `ActionController::API` and `Base` inheritance
and `before_action` with named Symbol callbacks. It does not type the rest of
Rails or its callback option/block forms. The Falcon bridge covers
`Loader#service(name)` with a builder block, `include`, String `url`, Integer
`count`, block `prepare!`, and value/block forms of `connectrpc_server`,
`rack_app`, `tls`, `tls_provider` and `server_options`. Deferred option blocks
have the ConnectRPC Environment capabilities. No general `method_missing`
acceptance or complete upstream inheritance is declared. The repository checks
actual Rails subclasses and a real Falcon-loaded service configuration with
these declarations and runtime counterparts. The Falcon fixture accepts the real loader as a typed method argument, letting
Steep infer the builder block's self type without a file-level self override.

## Repository checks

```sh
mise run rbs:setup
mise run rbs:check
mise run rbs:source    # Focused strict implementation leaf.
mise run rbs:api       # Focused supported API callers.
mise run rbs:inventory # Source/signature drift.
mise run rbs:proof     # Independent capture red/green probes.
mise run rbs:package
MISE_RUBY_VERSION=4.0.9 mise run rbs:check
MISE_RUBY_VERSION=4.0.9 mise run rbs:package
```

The independent checksummed `typecheck/Gemfile.lock` pins only development tools.
Bundles, Bundler state, compiler caches and local evidence live under
`vendor/rbs/`. The package check installs into a fresh GEM_HOME and exercises
normal installed signature/executable discovery outside the checkout. It clears
checkout Ruby and Bundler load paths. Both Ruby lanes run these feature gates in
CI. `rbs:check` includes strict source, inventory and all API consumers as well as
the original generated positive/17 negative and nominal/bridge cases.
`rbs:package` uses the existing frozen optional bundle setups for offline runtime
archives, builds every owned signature package and checks each library through
normal installed discovery. It checks composed environments in both load orders,
real Rails subclass inclusion, Rack/Testing calls, native session/TLS operations,
Falcon endpoint integration and API-only OTel without loading an SDK. The original
package guards and generated installed callers remain. Core, optional and tool
bundles stay independent; this feature adds no dependency or pin.

`rbs:proof` captures independent source inodes without Git metadata, proves wrong
return/data/callback diagnostics and source-addition/signature-omission drift,
restores every captured byte, and reruns green. It never mutates delivery source.
The captured executable alone uses 0755 for existing archive guards; source modes
remain unchanged. Logs and capture bindings live under ignored `vendor/rbs/`.

## Cancellation types

Client and generated unary methods accept `cancellation: CancellationToken?`.
Pass `CancellationSource.new.token`; the source itself is rejected. Tokens expose
only observation and listener registration. `CancellationRegistration#unregister`
returns bool. Token reasons are `:canceled | nil`; Context's combined
`cancellation_reason` also admits `:deadline_exceeded`.

`TransportRequest` appends `cancellation: _Cancellation?` after `lifecycle`;
negative indices shift accordingly (`-1` is cancellation, `-2` is lifecycle).
Complete and partial Structs retain their precise field contracts and aggregate
unions. `_Cancellation` is the read-only transport/host extension SPI, with
`cancelled?`, `reason`, `check!` and `on_cancel`. Server/Context accept that SPI
and own separate processing arbitration. Deadline remains an actual Deadline or
nil. Runtime cancellation, reactor and blocking-I/O limits still apply.
