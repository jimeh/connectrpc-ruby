# Supported 0.x profile and compatibility

This is the agreed target support contract for the first supported 0.x profile,
as selected in [#27](https://github.com/jimeh/connectrpc-ruby/issues/27).
It does not declare the current artifacts supported or production ready. The
project remains experimental until the [adoption gates](#adoption-gates) are met
and a release explicitly adopts this profile. Documenting the contract does not
publish packages or authorize release preparation, deployment or a 1.x promise.

The capability and evidence map below was checked against source at `1b4870e`,
after [#44](https://github.com/jimeh/connectrpc-ruby/pull/44). The existing
[main CI run](https://github.com/jimeh/connectrpc-ruby/actions/runs/42419273239)
passed its declared jobs. This document adds no new runtime qualification.
Historical audit probes and source inspection are distinguished from current
runtime evidence in [#26](https://github.com/jimeh/connectrpc-ruby/issues/26).

## Compatibility and package versions

Once a release adopts this profile, patch releases preserve the documented
application API, extension SPI and wire defaults within its 0.x minor series.
This includes documented options, generated caller contracts, error behavior,
limits, timing/resource policies and observable tracing conventions. Breaking
changes belong in minor releases with migration notes explaining the affected
calls, configuration, wire behavior and generated code. A minor release may
require regeneration when its migration notes say so. There is no blanket
cross-minor generated-code guarantee.

Deprecations and replacements must be described in the relevant documentation
and migration notes. No fixed deprecation period, maintenance duration, response
SLA or 1.x compatibility guarantee is promised. These rules do not turn internal
Ruby methods into extension points.

Support requires the **same version** of `connectrpc` and every optional
ConnectRPC gem in the application: `connectrpc-rack`, `connectrpc-rails`,
`connectrpc-async`, `connectrpc-falcon`, `connectrpc-testing` and `connectrpc-otel`.
Only the packages actually used need to be installed. Existing gemspec ranges
such as `~> 0.1.0` permit patch skew; dependency resolution alone does not qualify
that combination. This is a support policy, with no new runtime version guard
or dependency-range enforcement. Native packages also compose private core
implementation and must not infer compatibility from the public SPI alone.

## Qualified environment

The initial target uses Ubuntu 24.04, x86_64, MRI 3.4.12 or 4.0.9, Bundler
4.0.23 and the owning frozen, checksummed dependency bundles. The
[CI workflow](../.github/workflows/ci.yml) defines the executed combinations;
[Mise tasks](../mise.toml) define their local equivalents. The Ruby `>= 3.4`
gemspec floor and broader dependency ranges describe allowed installation,
not qualification of all matching versions.

| Integration | Exact baseline | Support scope |
| --- | --- | --- |
| Core, Rack, Testing | [Root](../Gemfile.lock), [Rack](../gems/connectrpc-rack/Gemfile.lock) and [Testing](../gems/connectrpc-testing/Gemfile.lock) locks; google-protobuf 4.36.2, Rack 3.2.7 where used | Both MRI lanes; socket evidence for portable Rack uses rackup/WEBrick |
| Native Async | [Async lock](../gems/connectrpc-async/Gemfile.lock): async 2.46.0, async-http 0.105.0, protocol-http2 0.29.2 | Both MRI lanes; application-owned Async reactor and thread |
| Falcon | [Falcon lock](../gems/connectrpc-falcon/Gemfile.lock): Falcon 0.57.0 and its locked native stack | Both MRI lanes; native server and explicit Rack/Rails fallback have separate execution contracts |
| Rails supported profile | [Rails 8.1 lock](../gems/connectrpc-rails/gemfiles/rails_8.1.gemfile.lock): Rails/Action Pack 8.1.4 | MRI 3.4.12 and 4.0.9, native unary controller execution |
| Rails compatibility evidence | [Rails 8.0 lock](../gems/connectrpc-rails/gemfiles/rails_8.0.gemfile.lock): Rails/Action Pack 8.0.5.1 | MRI 3.4.12 only; retained CI lane, outside the supported Rails profile |
| OpenTelemetry | [OTel lock](../gems/connectrpc-otel/Gemfile.lock): API 1.11.1, SDK 1.13.1; Rack instrumentation 0.31.2, NetHTTP instrumentation 0.29.2 | Both MRI lanes; tested coexistence includes native Rails 8.1.4. Other SDK/instrumentation combinations require their own evidence |
| Official browser fixture | [Browser pins](../browser/artifacts.json), [JS lock](../browser/bun.lock): grpc-web 2.1.1, google-protobuf 4.0.3, Playwright 1.64.1, Chromium 156.0.8078.4, Node 24.22.1, Bun 1.4.4 | Browser fixture on both MRI server lanes, with the protocol limits below |

Lockfile platforms for other architectures or operating systems are not runtime
qualification. macOS, Windows, ARM, JRuby, TruffleRuby, Puma, every Rack server,
every Rails/dependency combination and the dependency Cartesian product are
outside this initial supported environment. This does not assert they fail.
The Rails 8.0 evidence lane does not extend upstream maintenance.

## Protocol and host matrix

U means unary, SS server streaming, CS client streaming, and bidi bidirectional
streaming. All entries are target profile capabilities, subject to the adoption
gates. HTTP/1 streaming means HTTP/1.1. The standalone Async Host remains strict
HTTP/2; the explicit `HTTP1Transport` does not add H1 to that Host.

Connect clients default to protobuf (`:proto`) and identity; ProtoJSON uses an
explicit `codec: :json`. Servers accept both codecs. Gzip, client GET and
optional integrations require explicit configuration. Gzip negotiation
and independent wire/decoded limits remain those in the [core guide](../README.md)
and [streaming guide](streaming.md). GET is unary only, opt-in on the client,
and restricted to methods declared `:no_side_effects`; it never enables replay
or fallback. Server routing/authentication remains application owned.

| Connect endpoint | U | SS | CS | bidi | HTTP/security evidence | Owning evidence |
| --- | --- | --- | --- | --- | --- | --- |
| Host-neutral `Server#call` | Encoded POST/eligible GET | No | No | No | Host-supplied HTTP state and bounded input, no socket ownership | Core specs, `rbs:check` API callers and installed consumers |
| Default NetHTTP client | POST/eligible GET | No | No | No | H1.1 TCP; verified TLS in focused specs/installed consumers, outside the plaintext official profile. No client-certificate (mTLS) or custom trust configuration | `test:conformance`, `test:interop`, `test:interop:get`, core TLS/cancellation specs and `async:package` |
| Rack middleware | POST/eligible GET | No | No | No | Portable unary translation; official socket profile is H1.1 TCP via rackup/WEBrick, with no portable TLS/mTLS qualification | `test:conformance`, `test:interop`, `rack:test`, `rack:package` |
| Native Rails controller | POST/eligible GET | No | No | No | Portable unary execution through explicit routes; no separate Rails TLS/mTLS qualification | `test:rails`, `check:package:rails`, `generation:check:rails`; no exact Rails conformance inventory |
| Testing transport | Encoded POST/eligible GET | No | No | No | In process, no HTTP socket or security negotiation | `testing:test`, `testing:package`; not independent interoperability evidence |
| Async Host | POST/eligible GET | Yes | Yes | Yes | H2 prior-knowledge h2c, verified TLS `h2`, mTLS | `async:interop`, `async:interop:streaming`, `async:conformance`, native specs and installed callers |
| Falcon native server | POST/eligible GET | Yes | Yes | H2 only | H2 h2c/TLS/mTLS; H1.1 TCP/TLS/mTLS for U/SS/CS | `falcon:interop`, `falcon:interop:streaming`, `falcon:conformance`, `falcon:conformance:http1`, native specs and installed callers |
| Async `Transport` | POST/eligible GET | Yes | Yes | Yes | H2 h2c, verified TLS `h2`, mTLS | `async:interop`, `async:interop:streaming`, `async:conformance`, native specs and installed callers |
| Async `HTTP1Transport` | POST/eligible GET | Yes | Yes | No | H1.1 TCP, verified TLS/mTLS; only H1 ALPN or absent ALPN accepted | `async:conformance:http1`, `async:interop:http1`, H1 specs and installed generated callers |

Native clients require an application-owned Async reactor. Construction, I/O
and close run on its reactor/thread; foreign-thread cancellation only signals
that owner. There is no ordinary-thread persistent/streaming facade. Each H1
connection has one exclusive lease through EOF and cleanup. Long streams can
occupy all leases. Native handlers do not acquire Rails callbacks or
security policy merely because Falcon also hosts a Rails fallback.

Native gRPC and gRPC-Web are explicit protocols, separate from Connect. `:auto`
means Connect plus native gRPC only. Both gRPC families require protobuf and a
status codec. Web has no Ruby client mode.

| Protocol/endpoint | Shapes | HTTP and security | Codec/compression | Owning evidence |
| --- | --- | --- | --- | --- |
| Native gRPC: Async Transport, Async Host, Falcon native server | U/SS/CS/bidi | H2 h2c, verified TLS and mTLS | Protobuf; identity or opt-in per-message gzip; no GET | `async:conformance:grpc`, `falcon:conformance:grpc`, `async:interop:grpc`, `falcon:interop:grpc` and installed generated callers |
| Web: Async native server | U/SS binary and text wire | H2 h2c/TLS; mTLS identity in focused SDK-backed specs | Protobuf, identity only | `async:conformance:web` for binary; `browser:verify` for browser text/binary behavior; `otel:test` for identity/ownership |
| Web: Falcon native server | U/SS binary and text wire | H1.1 TCP/TLS and H2 h2c/TLS; mTLS identity in focused SDK-backed specs | Protobuf, identity only | `falcon:conformance:web`, `browser:verify`, independent binary Go control and `otel:test` |
| Web: Rack through rackup/WEBrick | U binary/text | Portable H1 TCP evidence, no qualified TLS/mTLS profile | Protobuf, identity only | `rack:conformance:web` is exact binary evidence; `rack:test` covers binary/text translation |
| Web: native Rails controller | U binary/text | Explicit POST/OPTIONS routes, private protocol/status/CORS hooks | Protobuf, identity only | `browser:verify`, Rails and OTel specs; no exact Rails Web conformance ledger |
| Web: Testing transport | Manual encoded U binary/text | In process only | Protobuf, identity only | `testing:test`, `rbs:package` manual encoded requests; no browser/network claim |

Official grpc-web browser clients exercise binary/text unary and incremental
**text** SS on native hosts. Binary SS wire interoperability is a separate
connect-go check; binary XHR buffers and is not an incremental-browser promise.
The browser fixture deliberately ignores fixture TLS errors, so it does not
qualify verified TLS or browser client-certificate selection. Native wire/SDK
mTLS checks are separate. See [Web](grpc-web.md) for explicit CORS, Rails
CSRF/origin policy and bounds. No Web CS/bidi, JSON, per-message compression or
whole-body compression is included.

NetHTTP, Rack, Rails, Testing, HTTP1Transport and Falcon H1 do not provide native
gRPC. Portable adapters reject streaming before input/handlers; incapable
clients reject before codecs/I/O. There is no H1 bidi, HTTP/1.0 Connect streaming,
HTTP/3, implicit protocol fallback, RPC replay, new backend or new ordinary-thread
integration in this contract. Faraday is outside the profile.

## What the evidence establishes

The [conformance guide](../conformance/README.md), selected YAML profiles,
`cases*.json` inventories and owning ledgers define exact scheduled cases.
Passing them is not exhaustive conformance for every library capability or
host/configuration. In particular:

- `test:conformance` selects portable Connect U/GET over plaintext H1. Native
  `profile-native-{identity,gzip}.yaml` selects H2 Connect streaming, not native
  unary/GET. Native unary/GET evidence comes from independent peers, focused
  specs and installed callers, not that streaming ledger.
- `profile-http1-{identity,gzip}.yaml` selects Falcon Connect SS/CS. The separate
  `profile-native-http1-client-{identity,gzip}.yaml` selects native H1 client
  U/GET/SS/CS, including TLS/client certificates. Server and client inventories
  are not interchangeable.
- `profile-grpc-{identity,gzip}.yaml` selects H2 gRPC all four shapes.
  `profile-web-{async,falcon,rack}.yaml` selects binary Web with host-specific
  HTTP/shape axes and no client-certificate axis. The official runner has no
  Web text axis. Rails browser/spec coverage is not a Rails conformance ledger.
- The official runner is connectconformance 1.0.6. Its embedded reference is
  distinct from the repository's [connect-go 1.21.1 peer](../interop/go.mod) and
  [grpc-go 1.85.0 peer](../interop/grpc/go.mod), built with Go 1.27.3. Independent
  peers, official profiles, local specs and browser checks establish different
  properties and must be reported separately.
- `rbs:check` checks supported callers, exact negative diagnostics, source and
  inventory; `rbs:package` checks normal installed signature discovery and real
  generated consumers in both package composition orders. Package tasks use
  fresh external GEM_HOME environments. Installed callers establish packaging
  and caller contracts, not independent protocol interoperability.

[CI](ci.md) assigns these tasks to frozen bundles and both Ruby lanes where
specified. Use `mise tasks` for actual task names. No passing count substitutes
for an executed profile, a ledger's complete accounting or a missing test axis.

## Application APIs, extension SPIs and internals

The owning gem's shipped `sig/` and manifest, the [RBS guide](rbs.md), and
[`typecheck/surface.json`](../typecheck/surface.json) define the boundary.
Inventory classifications are `supported`, `spi` and `internal`; Ruby public
visibility alone is insufficient. The RBS interfaces describe callable shapes,
while protocol/package guides define runtime ownership and constraints.

Supported application APIs include method/service declarations, core and
generated client operations, Server configuration, metadata/errors/contexts,
cancellation sources/tokens, scoped readers/writers/duplex endpoints, and the
optional packages' documented constructors/lifecycle/configuration operations.
Generated service factories, typed handlers and installed generated-client
contracts are included. Optional protobuf/framework bridges remain explicit
opt-ins with their documented finite coverage.

Extension SPIs include `_Binding`, unary `_Transport`/`_Input`/invokers,
`_Cancellation`, `_StreamingTransport`/`_StreamExchange` and streaming I/O,
`_GRPCTransport`/`_GRPCStreamExchange`/`_GRPCStatusCodec`, native server
publication/output interfaces including `_GRPCWebOutput`, host lifecycle handles,
and subscriber/observer callbacks. Rails' private configuration hooks are
deliberate extension points: the inventory classifies the Web protocol/status/CORS
hooks as `supported` and service/binding/handlers/limits/instrumentation hooks
and request/context/method readers as `spi`. Use the owning RBS interface and
guide, including bounded reads, completion, reactor affinity and
original-processor ownership. An SPI does not grant access to the private
objects its implementation happens to expose.

Protocol/compression/parser helpers, cancellation arbitration, private lifecycle
bookkeeping, native socket/lease/session helpers and generator implementation
are internal unless explicitly classified otherwise. They receive no patch
extension guarantee. This does not exempt their observable supported behavior
from patch compatibility. The [strict source list](../typecheck/source.rb) is
finite; consumer coverage does not mean all optional implementation is checked.

## Deliberate policies and limitations

Native Connect streaming clients require HTTP EOF and transport completion
within one absolute second after their terminal envelope, even with a nil RPC
deadline. Extra bytes fail, and slow valid EOF may be rejected. This is an
interoperability policy, not a Connect requirement, gRPC timer or NetHTTP unary
timeout. [Streaming](streaming.md#completion-policy) owns recovery and cleanup
budgets. Native gRPC requires actual final H2 HEADERS and EOF and deliberately
rejects malformed/duplicate rich details, details on OK and mismatched rich codes
with safe internal errors. See [gRPC](grpc.md) for its stricter-than-grpc-go policy.

Falcon Connect H1 remains response-first. Incomplete original input disables
reuse; after original processing finishes (any RPC outcome) and the full response
is delivered, retirement uses a cooperative **500 ms total from the actual final
HTTP flush**, including the unchanged absolute 100 ms / 256 KiB discard allowance
with full 64 KiB read reservations. The remaining budget adds no reads or pipeline reuse. Admission
and socket ownership last through retirement and original processing join.
Cooperative timers can overshoot and cannot preempt arbitrary native/application
work. [Falcon Web cleanup](grpc-web.md#bounds-and-host-spi) is separate: original
input cleanup gates its staged terminal and incomplete-upload rejection head;
it does not inherit Connect's response-first grace.

Processing, delivery, cleanup and admission are independent facts. Full native
write/cleanup does not prove peer receipt or TLS close_notify. Only actual H2
CANCEL signals peer cancellation to Context; H1 EOF/disconnect, other resets and
shutdown do not. A stuck H2 frame may fail its connection and siblings during
bounded cleanup. Retention/byte limits do not bound total RSS, parser/HPACK
allocation, codec work or application CPU.

Rack cannot preserve underscore versus hyphen request metadata names after env
normalization. NetHTTP and rackup/WEBrick also coalesce some repeated fields.
Native metadata acceptance is not narrowed to compensate. OTel's default
unknown-method redaction omits the convention's `rpc.method_original` attribute;
this is a documented privacy choice, not full semantic-convention compliance.
SDK/exporter ownership and blocking remain application responsibilities.

## Adoption gates

[#26's disposition ledger](https://github.com/jimeh/connectrpc-ruby/issues/26)
is the current repair/decision authority. At the source baseline above, #39
resolved H1/M2 and the core part of M1; #40 resolved L17's archive modes. These
fixes do not close the remaining findings. This guide corrects L18's current
documentation drift without declaring the audit complete.

| Unresolved group | Gate before adopting the affected profile |
| --- | --- |
| Falcon fallback cache M3; standalone Async Host accept failures M4 | Resolve the fallback default/configuration defect in its owning repair. Diagnose current accept-failure behavior and settle bounded recovery/fail-visible policy. Native RPC success does not qualify fallback or listener resilience |
| Custom observer detach unwind | A fatal/nonlocal exit in one detach can skip earlier detaches. Require a repair or explicit support decision with focused core/SDK evidence. Bundled OTel detach does not yield; old evidence does not prove cross-RPC contamination |
| Normalization L1/L3/L6; cancellation and exchange SPIs L2/L5 | Settle cause/error attribution, the exposed mutable unary cancellation owner versus read-only SPI intent, and duplicate exchange `close_send` ownership. Do not treat contradictory intent as a satisfied guarantee |
| gRPC bounds L4/L7; upload cancellation L10 | Bound rejected-media draining and surplus-message decoding. Verify the current H2 unary cancellation reset on wire; unchanged source and historical probes do not establish its current result |
| Native lifecycle metadata/OTel L8/L9/L11; host remnants of M1 | Reconcile early metadata loss, frozen gRPC unary request metadata, local OTel classification casing, and any reachable ambient-exception path with current evidence and explicit contracts |
| Generator/tooling/package L13-L16 | Resolve or explicitly disposition generator name collisions, unmatched native selectors, bundle-task isolation and direct gemspec-caller behavior. Canonical builds and existing green CI do not prove those other paths |

L12 is disclosed above as an adapter limitation. Other material findings require
accepted fixes or explicit residual-limit decisions recorded in #26, with
current regression/qualification evidence for affected rows. This document does
not pre-accept the remaining defects as supported behavior. Before adoption,
reconcile every accepted material finding, qualify the adopting revision with
its declared CI/installed-caller/protocol gates, and identify the release and
migration notes that adopt this contract. Release preparation remains
[#36](https://github.com/jimeh/connectrpc-ruby/issues/36); publication and removal
of the `https://gems.invalid` guard require separate authorization.
