# Supported 0.x profile and compatibility

Use this guide to choose a protocol, host and runtime combination, and to
understand which APIs and wire behavior the first supported 0.x profile will
preserve. It records the target contract agreed in
[#27](https://github.com/jimeh/connectrpc-ruby/issues/27).

**The profile has not been adopted by a release.** Current artifacts remain
experimental and are not declared supported or production ready. Adoption
requires the [remaining audit dispositions and release qualification](#adoption-gates).
This document does not authorize publication, release preparation, deployment
or a 1.x promise.

For a specific integration, start with the [environment](#qualified-environment)
and [protocol and host matrix](#protocol-and-host-matrix). The
[API boundaries](#application-apis-extension-spis-and-internals) and
[compatibility policy](#compatibility-and-package-versions) define what an
adopting release will preserve. The [evidence map](#what-the-evidence-establishes)
explains what has been exercised and where qualification is limited.

The source baseline is `073f95a`, including the
[H1 client](https://github.com/jimeh/connectrpc-ruby/pull/44) and
[Falcon cache repair](https://github.com/jimeh/connectrpc-ruby/pull/47).
Reading source for this guide adds no runtime qualification. The
[#26 disposition ledger](https://github.com/jimeh/connectrpc-ruby/issues/26)
distinguishes source inspection and historical probes from current runtime
evidence; a passing selected profile does not close that audit.

## Compatibility and package versions

Once a release adopts this profile, patch releases preserve the documented
application API, extension SPI and wire defaults within its 0.x minor series.
This includes documented options, generated caller contracts, error behavior,
limits, timing and resource policies, and observable tracing conventions.

Breaking changes belong in minor releases. Their migration notes must explain
the affected calls, configuration, wire behavior and generated code. A minor
release may require regeneration when its migration notes say so. Generated
code has no blanket compatibility guarantee across minor releases.

Deprecations and replacements must be described in the relevant documentation
and migration notes. No fixed deprecation period, maintenance duration, response
SLA or 1.x compatibility guarantee is promised. These rules do not turn internal
Ruby methods into extension points.

### Match the ConnectRPC package versions

Support requires the **same version** of `connectrpc` and every optional
ConnectRPC gem used by the application: `connectrpc-rack`, `connectrpc-rails`,
`connectrpc-async`, `connectrpc-falcon`, `connectrpc-testing` and `connectrpc-otel`.
Only the packages actually used need to be installed.

Existing gemspec ranges such as `~> 0.1.0` allow patch versions to differ.
An installable combination is not necessarily supported. This policy adds no
runtime version guard and does not change dependency ranges. Native packages
also use private core implementation, so their compatibility cannot be inferred
from the public SPI alone.

## Qualified environment

The initial target uses **Ubuntu 24.04 on x86_64**, **MRI 3.4.12 or 4.0.9**, and
**Bundler 4.0.23** with the owning frozen, checksummed dependency bundles. Both
MRI versions apply to the rows below unless a row says otherwise. The
[CI workflow](../.github/workflows/ci.yml) defines the executed combinations;
[Mise tasks](../mise.toml) define their local equivalents.

| Integration | Locked baseline | Scope of the evidence |
| --- | --- | --- |
| Core, Rack, Testing | [Root](../Gemfile.lock), [Rack](../gems/connectrpc-rack/Gemfile.lock) and [Testing](../gems/connectrpc-testing/Gemfile.lock): google-protobuf 4.36.2 and Rack 3.2.7 where used | Portable Rack socket checks use rackup/WEBrick |
| Native Async | [Async lock](../gems/connectrpc-async/Gemfile.lock): async 2.46.0, async-http 0.105.0, protocol-http2 0.29.2 | Requires an application-owned Async reactor and thread |
| Falcon | [Falcon lock](../gems/connectrpc-falcon/Gemfile.lock): Falcon 0.57.0 and its locked native stack | Native RPC and explicit Rack/Rails fallback have separate execution contracts |
| Rails supported profile | [Rails 8.1 lock](../gems/connectrpc-rails/gemfiles/rails_8.1.gemfile.lock): Rails/Action Pack 8.1.4 | Native unary controller execution on both MRI versions |
| Rails compatibility evidence | [Rails 8.0 lock](../gems/connectrpc-rails/gemfiles/rails_8.0.gemfile.lock): Rails/Action Pack 8.0.5.1 | MRI 3.4.12 only; retained CI lane outside the supported Rails profile |
| OpenTelemetry | [OTel lock](../gems/connectrpc-otel/Gemfile.lock): API 1.11.1, SDK 1.13.1, Rack instrumentation 0.31.2, NetHTTP instrumentation 0.29.2 | Tested coexistence includes native Rails 8.1.4. Other SDK/instrumentation combinations need their own evidence |

The official browser fixture runs against both MRI server versions. Its
[artifact pins](../browser/artifacts.json) and [JS lock](../browser/bun.lock)
select grpc-web 2.1.1, google-protobuf 4.0.3, Playwright 1.64.1, Chromium
156.0.8078.4, Node 24.22.1 and Bun 1.4.4. Its protocol and TLS limitations are
listed [below](#grpc-web).

The Ruby `>= 3.4` gemspec floor and broader dependency ranges allow installation;
they do not qualify every matching version. Likewise, lockfile entries for
other platforms do not prove runtime compatibility. macOS, Windows, ARM, JRuby,
TruffleRuby, Puma, every Rack server, and every combination of allowed framework
and dependency versions are outside this initial supported environment. This
does not assert they fail. The Rails 8.0 lane does not extend upstream maintenance.

## Protocol and host matrix

U means unary, SS server streaming, CS client streaming, and bidi bidirectional
streaming. H1 means HTTP/1.1 and H2 means HTTP/2 in these tables. All entries
are target profile capabilities, subject to the adoption gates.

### Connect

Connect clients default to protobuf (`:proto`) and identity encoding. ProtoJSON
requires `codec: :json`; servers accept both codecs. Gzip, client GET and optional
integrations require explicit configuration. Gzip negotiation and independent
wire and decoded limits remain those in the [core guide](../README.md) and
[streaming guide](streaming.md).

Every unary entry below supports POST and eligible GET. GET is opt-in on the
client and restricted to methods declared `:no_side_effects`. It never enables
replay or fallback. Server routing and authentication remain application owned.
Host-neutral and Testing entries operate on encoded HTTP state without owning
a socket.

| Endpoint | RPC shapes | HTTP and security evidence | Owning evidence |
| --- | --- | --- | --- |
| Host-neutral `Server#call` | U | Host-supplied HTTP state and bounded input | Core specs, `rbs:check` API callers and installed consumers |
| Default NetHTTP client | U | H1 TCP; verified TLS in focused specs and installed consumers, outside the plaintext official profile. No client-certificate (mTLS) or custom trust configuration | `test:conformance`, `test:interop`, `test:interop:get`, core TLS/cancellation specs and `async:package` |
| Rack middleware | U | Portable unary translation; official socket profile is H1 TCP via rackup/WEBrick. No portable TLS/mTLS qualification | `test:conformance`, `test:interop`, `rack:test`, `rack:package` |
| Native Rails controller | U | Portable unary execution through explicit routes. No separate Rails TLS/mTLS qualification | `test:rails`, `check:package:rails`, `generation:check:rails`; no exact Rails conformance inventory |
| Testing transport | U | In process; no HTTP socket or security negotiation | `testing:test`, `testing:package`; not independent interoperability evidence |
| Async Host | U/SS/CS/bidi | H2 prior-knowledge h2c, verified TLS `h2`, mTLS | `async:interop`, `async:interop:streaming`, `async:conformance`, native specs and installed callers |
| Falcon native server | U/SS/CS on H1; U/SS/CS/bidi on H2 | H1 TCP/TLS/mTLS and H2 h2c/TLS/mTLS | `falcon:interop`, `falcon:interop:streaming`, `falcon:conformance`, `falcon:conformance:http1`, native specs and installed callers |
| Async `Transport` | U/SS/CS/bidi | H2 h2c, verified TLS `h2`, mTLS | `async:interop`, `async:interop:streaming`, `async:conformance`, native specs and installed callers |
| Async `HTTP1Transport` | U/SS/CS | H1 TCP, verified TLS/mTLS; accepts only H1 ALPN or absent ALPN | `async:conformance:http1`, `async:interop:http1`, H1 specs and installed generated callers |

The standalone Async Host remains strict H2. Selecting `HTTP1Transport` for a
client does not add H1 to that Host.

Native clients require an application-owned Async reactor. Construction, I/O
and close run on its reactor and thread; foreign-thread cancellation only
signals that owner. There is no ordinary-thread persistent/streaming facade.
Each H1 connection has one exclusive lease through EOF and cleanup, so long
streams can occupy all leases. Native handlers do not acquire Rails callbacks
or security policy merely because Falcon also hosts a Rails fallback.

### Native gRPC

Native gRPC is an explicitly enabled protocol, separate from Connect.
Server `:auto` means Connect plus native gRPC only. Both native gRPC and
[gRPC-Web](#grpc-web) require protobuf and a status codec.

| Endpoints | RPC shapes | HTTP, codecs and compression | Owning evidence |
| --- | --- | --- | --- |
| Async Transport, Async Host, Falcon native server | U/SS/CS/bidi | H2 h2c, verified TLS and mTLS; protobuf, identity or opt-in per-message gzip; no GET | `async:conformance:grpc`, `falcon:conformance:grpc`, `async:interop:grpc`, `falcon:interop:grpc` and installed generated callers |

NetHTTP, Rack, Rails, Testing, HTTP1Transport and Falcon H1 do not provide
native gRPC.

### gRPC-Web

Web must be enabled explicitly and has no Ruby client mode. Every row below
uses protobuf and identity encoding only. Binary and text refer to Web's wire
representations, not different message codecs.

| Server or adapter | RPC shapes and wire formats | HTTP and security evidence | Owning evidence |
| --- | --- | --- | --- |
| Async native server | U/SS binary and text | H2 h2c/TLS; mTLS identity in focused SDK-backed specs | `async:conformance:web` for binary; `browser:verify` for browser text/binary behavior; `otel:test` for identity and ownership |
| Falcon native server | U/SS binary and text | H1 TCP/TLS and H2 h2c/TLS; mTLS identity in focused SDK-backed specs | `falcon:conformance:web`, `browser:verify`, independent binary Go control and `otel:test` |
| Rack through rackup/WEBrick | U binary/text | Portable H1 TCP evidence; no qualified TLS/mTLS profile | `rack:conformance:web` is exact binary evidence; `rack:test` covers binary/text translation |
| Native Rails controller | U binary/text | Explicit POST/OPTIONS routes and private protocol/status/CORS hooks | `browser:verify`, Rails and OTel specs; no exact Rails Web conformance ledger |
| Testing transport | Manual encoded U binary/text | In process only | `testing:test`, `rbs:package` manual encoded requests; no browser/network claim |

Official grpc-web browser clients exercise binary/text unary and incremental
**text** SS on native hosts. Binary SS wire interoperability is a separate
connect-go check. Binary XHR buffers responses, so binary SS wire support does
not promise incremental browser delivery.

The browser fixture deliberately ignores fixture TLS errors. It does not
qualify verified TLS or browser client-certificate selection; native wire and
SDK mTLS checks are separate. See the [Web guide](grpc-web.md) for explicit
CORS, Rails CSRF/origin policy and bounds. Web CS/bidi, JSON, per-message
compression and whole-body compression are outside the profile.

### Unsupported combinations

Portable adapters reject streaming before reading input or invoking handlers;
incapable clients reject it before codecs or I/O. There is no H1 bidi,
HTTP/1.0 Connect streaming, HTTP/3, implicit protocol fallback, RPC replay,
new backend or new ordinary-thread integration in this contract. Faraday is
outside the profile.

## What the evidence establishes

Each capability row names its evidence. These checks answer different questions:

| Evidence | What it establishes | What it does not establish |
| --- | --- | --- |
| Official conformance profiles and ledgers | Exact scheduled cases, with complete execution accounting | Exhaustive conformance for every library capability or host configuration |
| Independent Go peers | Cross-implementation behavior for the exercised calls | Coverage of unexercised protocol or configuration axes |
| Browser checks | Behavior of the pinned official browser client against the fixtures | A text axis in the official conformance runner, or verified browser TLS |
| Installed typed consumers | Packaging, signature discovery and generated caller contracts | Independent protocol interoperability |

The official runner is connectconformance 1.0.6. Its embedded reference is
distinct from the repository's [connect-go 1.21.1 peer](../interop/go.mod) and
[grpc-go 1.85.0 peer](../interop/grpc/go.mod), built with Go 1.27.3. Report
independent peers, official profiles, local specs and browser checks separately.

### Read the selected profiles narrowly

The [conformance guide](../conformance/README.md), selected YAML profiles,
`cases*.json` inventories and owning ledgers define the exact cases. Their
coverage differs by endpoint:

- `test:conformance` selects portable Connect U/GET over plaintext H1.
  `profile-native-{identity,gzip}.yaml` selects H2 Connect streaming, not native
  unary/GET. Native unary/GET evidence comes from independent peers, focused
  specs and installed callers, not that streaming ledger.
- `profile-http1-{identity,gzip}.yaml` selects Falcon Connect SS/CS.
  `profile-native-http1-client-{identity,gzip}.yaml` separately selects native H1
  client U/GET/SS/CS, including TLS/client certificates. Server and client
  inventories are not interchangeable.
- `profile-grpc-{identity,gzip}.yaml` selects H2 gRPC in all four shapes.
  `profile-web-{async,falcon,rack}.yaml` selects binary Web with host-specific
  HTTP/shape axes and no client-certificate axis. The official runner has no Web
  text axis. Rails browser/spec coverage is not a Rails conformance ledger.

`rbs:check` checks supported callers, exact negative diagnostics, source and
inventory. `rbs:package` checks normal installed signature discovery and real
generated consumers in both package composition orders. Package tasks use fresh
external GEM_HOME environments.

[CI](ci.md) assigns these tasks to frozen bundles and both Ruby lanes where
specified. Use `mise tasks` for actual task names. No passing count substitutes
for an executed profile, a ledger's complete accounting or a missing test axis.

## Application APIs, extension SPIs and internals

The owning gem's shipped `sig/` and manifest, the [RBS guide](rbs.md), and
[`typecheck/surface.json`](../typecheck/surface.json) define the compatibility
boundary. The inventory uses three classifications:

| Classification | Meaning for callers |
| --- | --- |
| `supported` | Application APIs covered by the patch compatibility policy |
| `spi` | Deliberate extension interfaces, covered by that policy within their documented ownership and runtime constraints |
| `internal` | Implementation details with no patch extension guarantee |

Ruby public visibility alone does not establish a compatibility promise.
RBS describes callable shapes; the protocol and package guides describe runtime
ownership and constraints.

Supported APIs include method and service declarations, core and generated
client operations, Server configuration, metadata, errors, contexts,
cancellation sources/tokens, scoped readers/writers/duplex endpoints, and the
optional packages' documented constructors, lifecycle and configuration
operations. Generated service factories, typed handlers and installed generated
client contracts are included. Optional protobuf and framework bridges remain
explicit opt-ins with their documented finite coverage.

The extension SPIs cover these boundaries. Binding, cancellation and lifecycle
interfaces are shared with streaming and gRPC execution.

- Shared interfaces and unary execution: `_Binding`, `_Transport`, `_Input`, invokers and
  `_Cancellation`.
- Streaming and gRPC: `_StreamingTransport`, `_StreamExchange`, streaming I/O,
  `_GRPCTransport`, `_GRPCStreamExchange` and `_GRPCStatusCodec`.
- Server hosting and observation: native publication/output interfaces,
  including `_GRPCWebOutput`, host lifecycle handles and subscriber/observer
  callbacks.

Rails' private configuration hooks are also deliberate extension points. The
inventory classifies the Web protocol/status/CORS hooks as `supported`, and the
service/binding/handlers/limits/instrumentation hooks and request/context/method
readers as `spi`.

Use each interface within its documented bounded reads, completion, reactor
affinity and original-processor ownership rules. An SPI does not grant access
to private objects its implementation happens to expose. Protocol, compression
and parser helpers, cancellation arbitration, private lifecycle bookkeeping,
native socket/lease/session helpers and generator implementation are internal
unless explicitly classified otherwise. Their observable supported behavior
still falls under patch compatibility.

The [strict source list](../typecheck/source.rb) is finite. Consumer coverage
does not mean all optional implementation is checked.

## Deliberate policies and limitations

### Native client completion

Native Connect streaming clients require HTTP EOF and transport completion
within one absolute second after their terminal envelope, even with a nil RPC
deadline. Extra bytes fail, and slow valid EOF may be rejected. This is the
library's interoperability policy, not a Connect requirement. It is neither a
gRPC timer nor a NetHTTP unary timeout. The
[streaming guide](streaming.md#completion-policy) defines recovery and cleanup
budgets.

Native gRPC requires actual final H2 HEADERS and EOF. It deliberately rejects
malformed/duplicate rich details, details on OK and mismatched rich codes with
safe internal errors. The [gRPC guide](grpc.md) explains this policy, which is
stricter than grpc-go.

### Falcon Connect H1 retirement

Falcon remains response-first when original Connect H1 input is incomplete:

1. Incomplete input disables connection reuse. Original processing finishes,
   for any RPC outcome, and Falcon delivers the full response.
2. Retirement has one cooperative **500 ms total budget from the actual final
   HTTP flush**. It includes the unchanged absolute 100 ms / 256 KiB discard
   allowance, with full 64 KiB read reservations.
3. After discard stops, the remaining budget permits no more reads or pipeline
   reuse. Admission and socket ownership last through retirement and original
   processing join.

Cooperative timers can overshoot and cannot preempt arbitrary native or
application work. [Falcon Web cleanup](grpc-web.md#bounds-and-host-spi) is
separate: original input cleanup gates its staged terminal and incomplete-upload
rejection head. Web does not inherit Connect's response-first grace.

### Delivery, cancellation and metadata

Processing, delivery, cleanup and admission are independent facts. Full native
write and cleanup do not prove peer receipt or TLS close_notify. Only actual H2
CANCEL signals peer cancellation to Context. H1 EOF/disconnect, other resets and
shutdown do not. A stuck H2 frame may fail its connection and siblings during
bounded cleanup. Retention and byte limits do not bound total RSS, parser/HPACK
allocation, codec work or application CPU.

Rack env normalization loses the distinction between underscore and hyphen
request metadata names. NetHTTP and rackup/WEBrick also coalesce some repeated
fields. Native metadata acceptance is not narrowed to compensate.

OTel's default unknown-method redaction omits the convention's
`rpc.method_original` attribute. This is a documented privacy choice, not full
semantic-convention compliance. SDK/exporter ownership and blocking remain
application responsibilities.

## Adoption gates

[#26](https://github.com/jimeh/connectrpc-ruby/issues/26) owns audit dispositions.
The findings below retain their audit IDs so readers can follow each repair or
decision. A merged fix closes its specific defect, not the audit as a whole.

### Resolved or disclosed

- #39 resolved H1/M2 and the core part of M1; #40 resolved L17's archive modes.
- [#47](https://github.com/jimeh/connectrpc-ruby/pull/47), merged as `073f95a`,
  resolved M3. Falcon fallback caching defaults to `cache: false`, with explicit
  opt-in through `ConnectRPC::Falcon::Server.new` or `server_options`. Native RPC
  stays outside fallback cache middleware. The [Falcon guide](../gems/connectrpc-falcon/README.md#run-a-service)
  retains the opt-in cache semantics and application responsibilities.
- L12 is disclosed above as a Rack adapter limitation. This guide corrects
  L18's current documentation drift without declaring the audit complete.

### Still open

- **Async Host accept failures (M4).** Runtime diagnosis at `073f95a` on both
  pinned Rubies confirms that a controlled accept `EMFILE` silently fails the
  listener task. The session and socket stay live and admitted RPCs continue,
  but fresh typed RPCs time out without acceptance or dispatch. Calling `start`
  again does not recover the listener. First-accept and configured-persistent
  fault cases also reproduce; normal blocked-accept close passes. Repair policy
  remains open: bounded transient retry with visible terminal failure, or
  immediate visible host failure. The terminal observation mechanism is also
  undecided. This Host diagnosis does not explain historical H2 client failures.
- **Custom observer detach unwind.** A fatal or nonlocal exit in one detach can
  skip earlier detaches. Require a repair or explicit support decision with
  focused core/SDK evidence. The bundled OTel detach does not yield; historical
  evidence does not prove cross-RPC contamination. The proposed repair remains
  a gate until completed and qualified.
- **Normalization and SPIs (L1/L3/L6, L2/L5).** Settle cause/error attribution,
  the exposed mutable unary cancellation owner versus the read-only SPI intent,
  and duplicate exchange `close_send` ownership. Contradictory intent is not a
  satisfied guarantee.
- **gRPC bounds and upload cancellation (L4/L7, L10).** Bound rejected-media
  draining and surplus-message decoding. Verify the current H2 unary
  cancellation reset on wire; unchanged source and historical probes do not
  establish its current result.
- **Native lifecycle metadata and OTel (L8/L9/L11, host remnants of M1).**
  Reconcile early metadata loss, frozen gRPC unary request metadata, local OTel
  classification casing and any reachable ambient-exception path with current
  evidence and explicit contracts.
- **Generator, tooling and packaging (L13-L16).** Resolve or explicitly
  disposition generator name collisions, unmatched native selectors, bundle-task
  isolation and direct gemspec-caller behavior. Canonical builds and existing
  green CI do not prove those other paths.

### Before a release adopts the profile

1. Reconcile every accepted material finding in #26. Record accepted fixes or
   explicit residual-limit decisions, with current regression and qualification
   evidence for affected capability rows. This guide does not pre-accept the
   remaining defects as supported behavior.
2. Qualify the adopting revision with its declared CI, installed-caller and
   protocol gates.
3. Identify the release and migration notes that adopt this contract.

Release automation ([#36](https://github.com/jimeh/connectrpc-ruby/issues/36))
is described in [releasing](releasing.md). Merging a release PR is the separate
release decision.
