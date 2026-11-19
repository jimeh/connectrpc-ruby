# Supported 0.x profile and compatibility

Use this guide to choose a protocol, host and runtime combination, and to
understand which APIs and wire behavior the first supported 0.x profile will
preserve. It records the target contract agreed in
[#27](https://github.com/jimeh/connectrpc-ruby/issues/27).

**The profile has not been adopted by a release.** Current artifacts remain
experimental and are not declared supported or production ready. The audit,
lifecycle and policy-review gates are closed; adoption now waits for a qualified
release that names this contract, as the [adoption gates](#adoption-gates) describe.
This document does not authorize publication, release preparation, deployment
or a 1.x promise.

For a specific integration, start with the [environment](#qualified-environment)
and [protocol and host matrix](#protocol-and-host-matrix). The
[API boundaries](#application-apis-extension-spis-and-internals) and
[compatibility policy](#compatibility-and-package-versions) define what an
adopting release will preserve. The [evidence map](#what-the-evidence-establishes)
explains what has been exercised and where qualification is limited.

This guide describes the source on `main`. For a release, read it at that
release's tag. Reading source for this guide adds no runtime qualification. The
[#26 disposition ledger](https://github.com/jimeh/connectrpc-ruby/issues/26)
distinguishes source inspection and historical probes from current runtime
evidence.

## Compatibility and package versions

Once a release adopts this profile, patch releases preserve the documented
application API, extension SPI and wire defaults within its 0.x minor series.
This includes documented options, generated caller contracts, error behavior,
limits, timing and resource policies, and observable tracing conventions.
Interoperability policies marked **provisional** under
[deliberate policies](#deliberate-policies-and-limitations) are the exception:
that section defines how a release may change them.

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
ConnectRPC gem used by the application: `connectrpc-rack`, `connectrpc-on-rails`,
`connectrpc-async`, `connectrpc-falcon`, `connectrpc-testing`, `connectrpc-otel`,
`connectrpc-prometheus`, `connectrpc-yabeda`, `connectrpc-health`,
`connectrpc-sentry` and, since 0.3.0, `connectrpc-hyper`.
Only the packages actually used need to be installed.

Existing gemspec ranges of the form `~> M.m.0` allow patch versions to differ.
An installable combination is not necessarily supported. This policy adds no
runtime version guard and does not itself change gemspec dependency ranges;
[development](development.md#gem-dependency-constraints) owns that policy. Native packages
also use private core implementation, so their compatibility cannot be inferred
from the public SPI alone.

## Qualified environment

The initial target uses **Ubuntu 24.04 on x86_64**, **MRI 3.4.12 or 4.0.9**, and
**Bundler 4.0.23**. Both MRI versions apply to the rows below unless a row says
otherwise. Development locks are not committed: every CI run and fresh checkout
resolves the newest versions the gemspecs and Gemfiles allow, within a
three-day Bundler cooldown, and CI repeats that daily. A separate Ruby 3.4.12
lane pins each runtime dependency to its declared floor (see
[development](development.md#fresh-resolution-and-the-lowest-allowed-lane)).
The [CI workflow](../.github/workflows/ci.yml) defines the executed combinations;
[Mise tasks](../mise.toml) define their local equivalents.

| Integration | Tested range | Scope of the evidence |
| --- | --- | --- |
| Core, Rack, Testing | google-protobuf 4.36.2 (exact development pin); Rack newest allowed and floor 3.2.7 where used | Portable Rack socket checks use rackup/WEBrick |
| Native Async | async, async-http, protocol-http2 and the rest of the stack at newest allowed and at the gemspec floors (async 2.46.0, async-http 0.105.0, protocol-http2 0.29.2). async-http and protocol-http2 are patch-pinned; async takes `< 3` behind the require-time finalization guard (see [dependency constraints](development.md#gem-dependency-constraints)) | Requires an application-owned Async reactor and thread |
| Falcon | Falcon and its native stack at newest allowed and at the floors (Falcon 0.57.0) | Native RPC and explicit Rack/Rails fallback have separate execution contracts |
| Rails supported profile | Newest Rails/Action Pack 8.1.x | Native unary controller execution on both MRI versions |
| Rails compatibility evidence | Newest Rails/Action Pack 8.0.x and the Action Pack floor 8.0.5.1 | MRI 3.4.12 only; retained CI lanes outside the supported Rails profile |
| OpenTelemetry | Newest allowed API, SDK, Rack and NetHTTP instrumentation, plus the API floor 1.11.1 | Tested coexistence includes native Rails 8.1. Other SDK/instrumentation combinations need their own evidence |
| Prometheus metrics | prometheus-client newest allowed 5.x, plus the floor 5.0.0 | Default and `DirectFileStore` stores; real Rack, Async and Falcon hosts. Rails relies on the Rack path plus documentation |
| Yabeda metrics | yabeda newest allowed 0.x, plus the floor 0.16.0 | Declaration and values through `yabeda/testing`; yabeda-prometheus naming is checked structurally, not installed |
| Health service | google-protobuf newest allowed 4.x, plus the floor 4.36.2; grpc-go 1.85.0 health client and reference server | Check and List on real Rack, Async and Falcon hosts, Watch on Async and Falcon; grpc-go client over h2c, TLS and mTLS against Async and Falcon workers |
| Sentry | sentry-ruby, sentry-rails and sentry-opentelemetry newest allowed 7.x, plus the floor 6.7.0 | Real SDK with an in-memory transport; sentry-rails with native Rails 8.1 and ActiveRecord, Rack, native Async/Falcon and ThreadedTransport. Fiber hosts require `hub_isolation_level = :fiber` |
| Hyper transport | The committed `Cargo.lock` (hyper 1.12.0, h2 0.4.20, tokio 1.54.1, rustls 0.23.46 with aws-lc-rs 1.19.0), built with Rust 1.98.0 and at the declared MSRV 1.85; rb_sys newest allowed and the floor 0.9.130 for the source gem; grpc-go 1.85.0 and connect-go 1.21.1 peers | Calls from plain threads; async 2.46.0's worker pool only for the Ruby 4.0 offload specs. Platforms: [below](#connectrpc-hyper-platforms) |

The official browser fixture runs against both MRI server versions. Its
[artifact pins](../browser/artifacts.json) and [JS lock](../browser/bun.lock)
select grpc-web 2.1.1, google-protobuf 4.0.3, Playwright 1.64.1, Chromium
156.0.8078.4, Node 24.22.1 and Bun 1.4.4. Its protocol and TLS limitations are
listed [below](#grpc-web).

The Ruby `>= 3.4` gemspec floor and broader dependency ranges allow installation;
testing the newest and lowest allowed releases does not qualify every matching
version between them. Likewise, platform entries in `mise.lock` and the committed
tool locks do not prove runtime compatibility. macOS, Windows, ARM, JRuby,
TruffleRuby, Puma, every Rack server, and every combination of allowed framework
and dependency versions are outside this initial supported environment. This
does not assert they fail. The Rails 8.0 lane does not extend upstream maintenance.
connectrpc-hyper's prebuilt gems are the one exception, described next.

### connectrpc-hyper platforms

Since 0.3.0, connectrpc-hyper ships a source gem and platform gems that carry
its Rust extension for Ruby 3.4 and 4.0. CI and the release workflow
cross-build every platform gem twice in digest-pinned `rbsys/<platform>:0.9.130`
images and publish only archives that installed and passed smoke calls on a
native runner for their OS and architecture with both MRI versions. Every row
below gates the release:

| Platform gem | Built | Installed and exercised |
| --- | --- | --- |
| `x86_64-linux` (glibc 2.29+) | CI | Offline install and smoke calls on Ubuntu 24.04 x86_64; the full specs and interop run against source builds on the same image |
| `aarch64-linux` (glibc 2.29+) | CI | Offline install and smoke calls on native Ubuntu 24.04 arm64 runners |
| `x86_64-linux-musl`, `aarch64-linux-musl` | CI | Offline install and smoke calls in the official `ruby:3.4-alpine` and `ruby:4.0-alpine` images on the matching architecture. musl hosts need `libgcc_s` |
| `arm64-darwin` (macOS 11+) | CI | Offline install and smoke calls on a macOS 15 arm64 runner |
| `x86_64-darwin` | CI | Offline install and smoke calls on a macOS 15 Intel runner |
| `x64-mingw-ucrt` | CI | Provisional: offline install and smoke calls on Windows Server 2025; the specs that apply off Linux and a Go interop subset also run there in a non-gating job |
| `aarch64-mingw-ucrt` | CI | Provisional: offline install and smoke calls on Windows 11 arm64 |
| Source gem | CI | The published source gem compiles at install time and passes smoke calls on Ubuntu 24.04 x86_64; an offline build from the packaged `Cargo.lock` and a pre-fetched crate cache is also checked. Needs Rust 1.85+, libclang and a C compiler |

Smoke calls are real Connect unary calls over cleartext HTTP/1.1 and TLS
against a local server. The two Windows platforms are provisional for
connectrpc-hyper only; every other package keeps the Ubuntu x86_64 profile.
Their full specs are not gating yet. The reactor-thread hangs the Windows spec
job showed came from the spec harness's console output, not from
connectrpc-hyper. [#175](https://github.com/jimeh/connectrpc-ruby/issues/175)
is separately fixing a `close` wait on Ruby 4.0 with a single-worker async
pool, which affects every platform, not only Windows. Expect the Windows
platforms to change or be withdrawn without a minor release while they are
provisional.

The transport verifies TLS with rustls, not OpenSSL, so it is stricter than
NetHTTP and the Async transports: server certificates that are CN-only, X.509
v1, a single-label wildcard or a CA certificate used as the leaf fail
verification; encrypted private keys and keys aws-lc-rs cannot sign with
(Ed448) raise `ArgumentError`; and `SSL_CERT_FILE` or `SSL_CERT_DIR` replaces
the platform roots instead of adding to them. See the
[package guide](../gems/connectrpc-hyper/README.md#tls).

## Protocol and host matrix

U means unary, SS server streaming, CS client streaming, and bidi bidirectional
streaming. H1 means HTTP/1.1 and H2 means HTTP/2 in these tables. All entries
are target profile capabilities, subject to the adoption gates.

### Connect

Connect clients default to protobuf (`:proto`) and identity encoding. ProtoJSON
requires `codec: :json`; servers accept both codecs. Gzip, client GET and optional
integrations require explicit configuration. Gzip negotiation and independent
wire and decoded limits remain those in the [unary guide](unary.md#compress-with-gzip) and
[streaming guide](streaming.md).

Every unary entry below supports POST and eligible GET. GET is opt-in on the
client and restricted to methods declared `:no_side_effects`. It never enables
replay or fallback. Server routing and authentication remain application owned.
Host-neutral and Testing entries operate on encoded HTTP state without owning
a socket.

| Endpoint | RPC shapes | HTTP and security evidence | Owning evidence |
| --- | --- | --- | --- |
| Host-neutral `Server#call` | U | Host-supplied HTTP state and bounded input | Core specs, `rbs:check` API callers and installed consumers |
| Default NetHTTP client | U | H1 TCP, one connection per call and no pooling; verified TLS, and since 0.3.0 custom trust (`ca_file:`), mTLS and `http://` proxies from the environment or `proxy:`, with `CONNECT` tunnels for HTTPS. TLS and proxies are qualified in focused specs, outside the plaintext official profile | `test:conformance`, `test:interop`, `test:interop:get`, core TLS/proxy/cancellation specs and `async:package` |
| Rack middleware | U | Portable unary translation; official socket profile is H1 TCP via rackup/WEBrick. No portable TLS/mTLS qualification | `test:conformance`, `test:interop`, `rack:test`, `rack:package` |
| Native Rails controller | U | Portable unary execution through explicit routes. No separate Rails TLS/mTLS qualification | `test:rails`, `check:package:rails`, `generation:check:rails`; no exact Rails conformance inventory |
| Testing transport | U | In process; no HTTP socket or security negotiation | `testing:test`, `testing:package`; not independent interoperability evidence |
| Async Host | U/SS/CS/bidi | H2 prior-knowledge h2c, verified TLS `h2`, mTLS | `async:interop`, `async:interop:streaming`, `async:conformance`, native specs and installed callers |
| Falcon native server | U/SS/CS on H1; U/SS/CS/bidi on H2 | H1 TCP/TLS/mTLS and H2 h2c/TLS/mTLS | `falcon:interop`, `falcon:interop:streaming`, `falcon:conformance`, `falcon:conformance:http1`, native specs and installed callers |
| Async `Transport` | U/SS/CS/bidi | H2 h2c, verified TLS `h2`, mTLS | `async:interop`, `async:interop:streaming`, `async:conformance`, native specs and installed callers |
| Async `ThreadedTransport` | Connect U/SS; gRPC U/SS | H2 h2c, verified TLS `h2`, mTLS; from any thread | `async:conformance:threaded`, `async:interop` unary, `async:interop:streaming` and `async:interop:grpc` cases from plain threads, threaded specs and installed generated callers |
| Async `PerCallTransport` (since 0.3.0) | Connect U/SS; gRPC U/SS | H2 h2c, verified TLS `h2`, mTLS; one connection per call to the request URL's origin, from any thread or inline in a reactor; `http://` proxies (explicit, or the environment proxy for the URL's scheme as in NetHTTP) through HTTP `CONNECT` for h2c and TLS | `async:conformance:per-call`, `async:interop` unary, `async:interop:streaming` and `async:interop:grpc` cases from plain threads and inside a reactor, per-call specs including local `CONNECT` proxy specs, and installed generated callers |
| Async `HTTP1Transport` | U/SS/CS | H1 TCP, verified TLS/mTLS; accepts only H1 ALPN or absent ALPN | `async:conformance:http1`, `async:interop:http1`, H1 specs and installed generated callers |
| Hyper `Transport` (since 0.3.0) | U/SS/CS/bidi on H2; U/SS/CS with `http2: false` | H2 h2c, verified rustls TLS `h2`, mTLS; H1 TCP/TLS; from any thread without a reactor. Streaming over a TLS connection that negotiated `http/1.1` fails before any request | `hyper:test`, `hyper:interop` (connect-go over h2c, TLS, mTLS and H1), `hyper:package`, platform smoke jobs and installed callers |

The standalone Async Host remains strict H2. Selecting `HTTP1Transport` for a
client does not add H1 to that Host.

NetHTTP is the dependency-free client: one Connect unary call per HTTP/1.1
connection, from any thread. It verifies TLS and, since 0.3.0, takes a custom CA,
a client certificate and HTTP proxies (see
[NetHTTP TLS and proxies](unary.md#nethttp-tls-and-proxies)). It does not pool
or reuse connections; the Async transports are the pooled and HTTP/2 options.

Native clients require an application-owned Async reactor. Construction, I/O
and close run on its reactor and thread; foreign-thread cancellation only
signals that owner. The exception is `ThreadedTransport`: it drives the H2
`Transport` from one owner thread per process for callers on any thread. It
supports Connect unary (POST and GET) and server streaming, and native gRPC
unary and server streaming; stream blocks run on the caller's thread. Since
0.3.0, `PerCallTransport` supports the same shapes over one connection per
call: inline on the caller's reactor inside one, and on a short-lived owner
thread per call elsewhere. There is no ordinary-thread client or bidi
streaming in connectrpc-async; since 0.3.0, connectrpc-hyper's `Transport`
provides every shape from any thread.
The H2 `Transport` manages its own connections: shared attempts outside RPC
lifecycles, capped jittered backoff, opt-in wait-for-ready, count and request-byte
admission, rerouting only of calls that wrote nothing, replacement of a
gracefully draining GOAWAY connection within `connection_limit + max_draining`
live sockets with an optional drain timeout, and one resend of a Connect unary call
whose stream the server provably never processed. Native gRPC unary calls and
streams are never resent.
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
| Async `PerCallTransport` (since 0.3.0) | U/SS | H2 h2c, verified TLS and mTLS, one connection per call, optionally tunnelled through an `http://` proxy with `CONNECT`; protobuf, identity or opt-in per-message gzip; no GET | `async:conformance:per-call`, `async:interop:grpc` and per-call specs |
| Hyper `Transport` (since 0.3.0) | U/SS/CS/bidi | H2 h2c, verified rustls TLS and mTLS; protobuf; no GET; from any thread | `hyper:interop` against grpc-go over h2c, TLS and mTLS, including trailers-only responses, and `hyper:test` |

NetHTTP, Rack, Rails, Testing, HTTP1Transport, Falcon H1 and the hyper
transport with `http2: false` do not provide native gRPC.

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
new backend, or ordinary-thread client bidi streaming outside the hyper
transport, in this contract. Faraday is outside the profile.

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

[CI](ci.md) assigns these tasks to scoped bundles and both Ruby lanes where
specified. Use `mise tasks` for actual task names. No passing count substitutes
for an executed profile, a ledger's complete accounting or a missing test axis.

## Application APIs, extension SPIs and internals

The owning gem's shipped `sig/` and manifest, the health gem's generated
`rbs/grpc_health_v1` bridge, the [RBS guide](rbs.md), and
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
client operations, Server configuration, metadata, errors, contexts and their
`on_cancel` listeners, cancellation sources/tokens, interceptor registration
(`interceptors:`) with the `ClientUnaryCall`, `ServerUnaryCall`,
`ClientStreamCall` and `ServerStreamCall` call objects (including the
`ServerStreaming` subclasses) and `InterceptorStateError`, scoped readers/writers/duplex
endpoints, the health registry and its generated `grpc.health.v1` client, and the
optional packages' documented constructors, lifecycle and configuration
operations. Generated service factories, typed handlers and installed generated
client contracts are included. Optional protobuf, framework and generated health
message bridges remain explicit opt-ins with their documented finite coverage.

The extension SPIs cover these boundaries. Binding, cancellation and lifecycle
interfaces are shared with streaming and gRPC execution.

- Shared interfaces and unary execution: `_Binding`, `_Transport`, `_Input`, invokers,
  `_Cancellation`, and the `_ClientUnaryInterceptor`, `_ClientStreamInterceptor`,
  `_ServerUnaryInterceptor` and `_ServerStreamInterceptor` interceptor
  interfaces.
- Streaming and gRPC: `_StreamingTransport`, `_StreamExchange`, streaming I/O,
  `_GRPCTransport`, `_GRPCStreamExchange` and `_GRPCStatusCodec`.
- Server hosting and observation: native publication/output interfaces,
  including `_GRPCWebOutput`, host lifecycle handles and subscriber/observer
  callbacks.

Rails' private configuration hooks are also deliberate extension points. The
inventory classifies the Web protocol/status/CORS hooks as `supported`, and the
service/binding/handlers/limits/instrumentation/interceptors hooks and request/context/method
readers as `spi`.

Use each interface within its documented bounded reads, completion, reactor
affinity and original-processor ownership rules. An SPI does not grant access
to private objects its implementation happens to expose. Protocol, compression
and parser helpers, cancellation arbitration, interceptor continuations and
chains, private lifecycle bookkeeping, native socket/lease/session helpers and
generator implementation are internal unless explicitly classified otherwise.
Their observable supported behavior still falls under patch compatibility.

The [strict source list](../typecheck/source.rb) is finite. Consumer coverage
does not mean all optional implementation is checked.

## Deliberate policies and limitations

The [#27](https://github.com/jimeh/connectrpc-ruby/issues/27) policy review on
2026-11-17 compared each deliberate interoperability policy with connect-go
1.21.1, connect-es 2.3.0 and grpc-go 1.85.0, then classified it:

- **Promised** policies follow the
  [compatibility rules](#compatibility-and-package-versions): patch releases
  preserve them, and only a minor release with migration notes changes them.
- **Provisional** policies are kept but remain under review. Any minor release
  may change one, with release notes. A patch release may also relax a
  provisional rejection, so that it accepts peer behavior it rejects today, but
  never makes one reject more.

| Policy | Classification | Follow-up |
| --- | --- | --- |
| [Native Connect client completion](#native-client-completion) | Provisional | [#102](https://github.com/jimeh/connectrpc-ruby/issues/102) |
| [Native gRPC rich status](#native-grpc-rich-status) | Provisional | None |
| [Falcon Connect H1 retirement](#falcon-connect-h1-retirement) | Behavior promised; budgets provisional | None |
| [Server cancellation signal](#delivery-cancellation-and-metadata) | Provisional | [#103](https://github.com/jimeh/connectrpc-ruby/issues/103) |

The other entries in this section describe scope and accepted limitations, and
follow the ordinary compatibility rules.

### Native client completion

Native Connect streaming clients require HTTP EOF and transport completion
within one absolute second after their terminal envelope, even with a nil RPC
deadline. Extra bytes fail, and slow valid EOF may be rejected. This is the
library's interoperability policy, not a Connect requirement. It is neither a
gRPC timer nor a NetHTTP unary timeout. The
[streaming guide](streaming.md#completion-policy) defines recovery and cleanup
budgets.

This policy is **provisional**. Neither connect-go nor connect-es has a tail
timer. connect-go waits for EOF, bounded only by the call's deadline, and
rejects extra data. connect-es does the same after a successful terminal, but
reports a terminal error without reading further. The one-second bound stops a nil-deadline client from holding a stream
whose peer never closes it. It can also turn a fully received result into
`unavailable` when a server or proxy delays EOF. After a successful handler,
library servers send the terminal envelope before `processing_finished`
lifecycle observation, and their EOF waits for that observation to finish. [#102](https://github.com/jimeh/connectrpc-ruby/issues/102)
evaluates a configurable budget, or returning the received terminal after a
stalled tail.

Native gRPC requires actual final H2 HEADERS and EOF, as gRPC itself does, and
has no tail timer. That requirement follows the ordinary compatibility rules.

### Native gRPC rich status

Native gRPC clients reject each of these with a safe `internal` error:
malformed or duplicate `grpc-status-details-bin`, details on an OK status, a
rich status code that differs from `grpc-status`, an invalid detail type URL,
and duplicate `grpc-status` or `grpc-message` fields. The
[gRPC guide](grpc.md) defines the full status parsing rules.

This policy is **provisional**. No reference client rejects all of these
cases. grpc-go 1.85.0 also fails a code mismatch and invalid base64 with
`internal`. It ignores details that repeat or do not parse as a
`google.rpc.Status`, keeping `grpc-status`, and accepts details on OK only when
their code is also OK. connect-go 1.21.1 fails malformed
details, but lets a rich code replace `grpc-status` and ignores details on OK.
connect-es 2.3.0 also lets the rich status replace `grpc-status`, and reports
malformed details as `data_loss`.
Strict handling never silently drops rich data. The cost is an `internal` error
where grpc-go would report the server's own code, which takes a misbehaving
server to trigger. A later release may relax it toward grpc-go.

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

The behavior is **promised**: response-first ordering, no reuse after
incomplete input, no pipelined dispatch on a retiring connection, and admission
and socket ownership through retirement. The budgets are **provisional** tuning
values: the 500 ms total, the 100 ms / 256 KiB discard allowance, the 64 KiB
read reservation, and Falcon Web's 100 ms rejected-input cleanup allowance. The
Connect values follow Go's `net/http` server, which discards up to 256 KiB of
unread body (`maxPostHandlerReadBytes`) and waits 500 ms after its write
half-close (`rstAvoidanceDelay`). Unless the connection is already closing or
full duplex, Go reads that remainder before it writes the response, and closes
the connection instead when more remains. Falcon writes the response first, so
a client that sends its whole upload before reading can stall when the response
exceeds socket buffering. That needs a large early response; early Connect responses
are usually small errors.

### HTTP/2 keepalive and connection age

The Async `Host` and the Falcon native HTTP/2 listener own their HTTP/2
connections and offer server keepalive PINGs, grpc-go PING enforcement and
graceful idle and age drains (`ServerKeepalive`). Rails and Rack hosts cannot:
Puma serves HTTP/1.1, and HTTP/2 to those applications is terminated by a proxy
or a third-party server that owns the connection. Configure keepalive, PING
enforcement and connection age limits there, for example Envoy's
`max_connection_duration` or nginx's `keepalive_timeout`.

Since 0.3.0, the same hosts limit streams per HTTP/2 connection
(`max_concurrent_streams`, default 100) below the host-wide `max_requests`
(default 128), refuse a stream over either limit with `REFUSED_STREAM`, and
reset a stalled request body (`request_body_timeout`, default 60 seconds) with
`CANCEL`, except for client and bidi streams. `close(grace:)`, managed Falcon
worker stop and `drain_tls` use the same two-phase GOAWAY drain, with debug data
`graceful_stop` or `tls_rotation`, and keep serving streams opened before its
cutoff. For Rails and Rack hosts the proxy owns these limits too, for example
Envoy's HTTP/2 `max_concurrent_streams` or nginx's
`http2_max_concurrent_streams` and `client_body_timeout`.

Since 0.3.0, the same hosts stop accepting while a worker holds
`max_connections` (default 1024) connections, so new peers wait in the listen
backlog or reach a sibling Falcon worker instead of being reset, and resource
accept errors such as `EMFILE` retry with a warning and a delay capped at 1
second instead of stopping the listener. An identity that expires while
published keeps being served and is reported once as `:expired`. Some ways to
hold a slot remain by design, as in grpc-go and connect-go: idle client-streaming
and bidi calls, server-streaming and bidi responses whose peer keeps the
connection healthy but grants no flow-control credit, and a peer that grants a
little credit just within each `response_body_timeout`. Per-client connection
limits belong in a proxy. The
[Async guide](../gems/connectrpc-async/README.md#connection-capacity) owns these
policies.

Since 0.3.0, the Async hosts and HTTP/2 client transports take
`stream_receive_window` and `connection_receive_window` (default 16 MiB,
announced with a WINDOW_UPDATE right after SETTINGS). The stream window is also
the per-stream ingress bound. It starts at 64 KiB on hosts and 4 MiB on
`Transport` and `ThreadedTransport`, as in Go's net/http2 client. A stream moves
at most one stream window per round trip, so by default the stream windows
grow at run time from BDP PINGs, as grpc-go's do, up to `max_receive_window`
(default 1 MiB on hosts and 16 MiB on `Transport` and `ThreadedTransport`). The
connection window grows only when the estimate exceeds
`connection_receive_window`. With dynamic windows each stream's unread payload
is bounded by the larger of `stream_receive_window` and `max_receive_window`.
`dynamic_receive_windows: false` keeps both windows static. See
[flow-control windows](../gems/connectrpc-async/README.md#flow-control-windows)
for the memory ceilings.
For Rails and Rack hosts the proxy owns these windows, for example Envoy's
`initial_stream_window_size` and `initial_connection_window_size`.

Since 0.3.0, the Async `Host` and Falcon's native HTTP/2 listener also bound
peers that stop reading responses. A unary, client-streaming or fallback
response that waits `response_body_timeout` (default 60 seconds) for
flow-control credit is reset with `CANCEL`, like nginx's `send_timeout`. Each
HTTP/2 frame write must finish within `frame_write_timeout` (default 60
seconds), or the connection is aborted; nil turns it off. That second bound
applies to every RPC shape, and control frames queued behind a stalled write
wait for it rather than for the keepalive timeout. A write blocked on a full
send buffer finishes only after the peer drains a large part of it, about 950 KB
on Linux defaults. On Linux 4.6 and later, such a write is renewed for another
bound whenever the socket's `TCP_INFO` shows that the peer acknowledged new data
while unsent data was queued, so a peer whose TCP acknowledgements keep
advancing, however slowly, stays connected, and one that stops is aborted one to
two bounds after the later of its last progress and the write's start. On other platforms and older kernels each write keeps a fixed
deadline, which a slow peer can miss while still reading; raise
`frame_write_timeout` there for slow links. grpc-go has no flow-control wait
bound and no frame write bound, but sets `TCP_USER_TIMEOUT` to its keepalive
timeout, so the kernel fails a connection whose sent data stays unacknowledged,
or unsent behind a zero window, that long; a peer that keeps reading slowly
resets it. The Linux renewal applies the same idea, checked once per bound. Go's
`golang.org/x/net/http2` server has no credit bound either; its
`WriteByteTimeout` is off by default, and `http.Server.WriteTimeout` limits the
whole response of every stream shape, which is why streaming servers leave it
off. See [Stream
admission](../gems/connectrpc-async/README.md#stream-admission).

`async:interop:keepalive` checks client keepalive, PING enforcement, BDP PINGs
and the age drain between the pinned grpc-go peer and the Async `Transport` and
`Host`. `falcon:interop:grpc` checks only the age drain through real Falcon
workers over TLS; Falcon's PING enforcement and server PINGs are covered by its
specs.

### Delivery, cancellation and metadata

Processing, delivery, cleanup and admission are independent facts. Full native
write and cleanup do not prove peer receipt or TLS close_notify. Only actual H2
CANCEL signals peer cancellation to Context. H1 EOF/disconnect, other resets and
shutdown do not. `Context#on_cancel` listeners therefore observe only those
signals and an observed deadline. A stuck H2 frame may fail its connection and
siblings during bounded cleanup. Retention and byte limits do not bound total RSS, parser/HPACK
allocation, codec work or application CPU.

The cancellation signal is **provisional**. grpc-go 1.85.0 cancels a handler on
any peer reset and on connection loss, and Go's `net/http`, which connect-go
uses, cancels the request context when the client's connection closes. A
library handler whose peer has gone keeps running until it finishes, a send
fails or its deadline expires.
[#103](https://github.com/jimeh/connectrpc-ruby/issues/103) evaluates observing
those signals; a change would land in a minor release.

Rack env normalization loses the distinction between underscore and hyphen
request metadata names. NetHTTP and rackup/WEBrick also coalesce some repeated
fields. Native metadata acceptance is not narrowed to compensate.

A health Watch stream whose peer sends another reset code or disappears from an
idle HTTP/1.1 connection keeps its watcher until its next send attempt fails,
connection retirement or `shutdown`. Changes that coalesce back to the status it
last sent cause no send; `max_watchers` bounds that cost.
See the [health guide](../gems/connectrpc-health/README.md#watch-streams-the-server-cannot-see-end).

OTel never records the convention's `rpc.method_original` attribute for unknown
methods: a server call carries no request path, and the adapter does not derive
the attribute from the `url_path` that client calls carry. This is a documented
limitation, not full semantic-convention compliance. SDK/exporter ownership and
blocking remain application responsibilities.

## Adoption gates

The review gates are closed. A release adopts the profile once its revision is
qualified and its documentation and release notes record the adoption.

### Closed gates

- **Audit.** [#26](https://github.com/jimeh/connectrpc-ruby/issues/26) closed
  on 2026-11-16 with a final disposition for every finding. L12, the Rack
  metadata-name limitation, is an accepted residual limitation, disclosed
  [above](#delivery-cancellation-and-metadata) and in the
  [Rack guide](../gems/connectrpc-rack/README.md). Every other finding is
  resolved, as listed under [audit dispositions](#audit-dispositions).
- **Lifecycle validation.**
  [#99](https://github.com/jimeh/connectrpc-ruby/pull/99), merged as `1cb5647`,
  delivered [#34](https://github.com/jimeh/connectrpc-ruby/issues/34): a
  non-gating campaign that repeats connection churn, GOAWAY, cancellation,
  slow and failing peers, TLS rotation, keepalive and shutdown across the native
  clients and hosts, and requires every owned resource to retire. It found no
  library defects. The [`soak.yml`](../.github/workflows/soak.yml) run on
  `1cb5647` on 2026-11-17 passed all four lanes: Async and Falcon on both MRI
  versions. [CI](ci.md#lifecycle-campaign) describes the nightly schedule.
- **Policy review.** Completed on 2026-11-17 and recorded under
  [deliberate policies](#deliberate-policies-and-limitations). Its follow-ups,
  [#102](https://github.com/jimeh/connectrpc-ruby/issues/102) and
  [#103](https://github.com/jimeh/connectrpc-ruby/issues/103), change only
  provisional policies and do not gate adoption.

### Audit dispositions

Each entry keeps its #26 audit ID. #48 repaired a separate lifecycle finding
reported during #44.

- #39 resolved H1/M2 and the core part of M1; #40 resolved L17's archive modes.
- [#47](https://github.com/jimeh/connectrpc-ruby/pull/47), merged as `073f95a`,
  resolved M3. Falcon fallback caching defaults to `cache: false`, with explicit
  opt-in through `ConnectRPC::Falcon::Server.new` or `server_options`. Native RPC
  stays outside fallback cache middleware. The [Falcon guide](../gems/connectrpc-falcon/README.md#rack-fallback-caching)
  retains the opt-in cache semantics and application responsibilities.
- [#50](https://github.com/jimeh/connectrpc-ruby/pull/50), merged as `95ecf9f`,
  resolved M4. Each Async Host listener retried named transient accept errors
  with bounded backoff, and the sixth consecutive retried failure or any other
  listener error stopped the Host. Since 0.3.0
  ([#117](https://github.com/jimeh/connectrpc-ruby/issues/117)), resource
  errors retry indefinitely with a delay capped at 1 second, `ECONNABORTED` and
  `EINTR` retry at once, and Falcon's native listener follows the same policy.
  Any other listener error still stops the Host, and `Host#wait` raises the
  original cause after cleanup. The
  [Async guide](../gems/connectrpc-async/README.md#accept-errors) owns the
  retried errors, delays and lifecycle semantics. This repair does not explain
  historical H2 client failures.
- [#48](https://github.com/jimeh/connectrpc-ruby/pull/48), merged as `63b70f4`,
  repaired custom observer detach unwind. Every successful attachment receives
  one reverse-order detach attempt even when another detach raises a fatal
  exception or throws. [Design](design.md#rpc-lifecycle-observation) keeps the
  observer's own exception-safety obligations.
- [#61](https://github.com/jimeh/connectrpc-ruby/pull/61) resolves L4 and L7. A
  gRPC client stops reading a rejected-media response body at the first byte
  past 262,144 bytes, and a surplus unary request envelope fails before its
  payload is read or decoded, as the [gRPC guide](grpc.md) describes.
- [#79](https://github.com/jimeh/connectrpc-ruby/pull/79) resolves L13-L16.
  L13: the generator reserves the stdlib top-level roots the core runtime loads,
  such as `URI`, `Zlib`, `Net` and `OpenSSL`. Roots outside that closure, such
  as optional gem roots (`Async`, `Sentry`) or `Bundler`, stay unreserved, so
  generated modules may still share or replace them. L14: an unmatched native
  or Web conformance selector fails before the runner starts. L15: the backend
  gRPC interop tasks run the peer generation check through their own bundle
  and no longer reach root `setup`. L16: gemspec manifests no longer depend on
  the working directory, and a direct `gem build` from the wrong directory now
  fails instead of packaging another package's files.
- [#81](https://github.com/jimeh/connectrpc-ruby/pull/81) resolves L8, L9 and
  L11. Server outcomes record request metadata when processing begins, every
  server Context keeps mutable request metadata, and OTel uppercases only gRPC
  and gRPC-Web status codes in `error.type`, as
  [design](design.md#rpc-lifecycle-observation) and the
  [OTel guide](../gems/connectrpc-otel/README.md) describe.
- [#84](https://github.com/jimeh/connectrpc-ruby/pull/84) resolves L1, L2,
  L3, L5 and L6. Errors that core normalizes from a rescued exception carry no
  `cause`; lifecycle observers receive the original (L1). Transports receive a
  read-only `CallCancellation::View` without completion methods (L2). A
  binding exception other than `CodecError` reaches the client caller unchanged
  (L3). Core calls a stream exchange's `close_send` at most once (L5). A nil
  server response is `internal` on every protocol (L6). The
  [design](design.md) and [streaming guide](streaming.md#custom-transport-spi)
  own these contracts.
- [#96](https://github.com/jimeh/connectrpc-ruby/pull/96) resolves L10
  and the host part of M1. L10: when an Async HTTP/2 unary call is cancelled,
  reaches its deadline or has its task stopped while its upload waits for
  flow-control credit, the client now resets the stream with
  `RST_STREAM(CANCEL)` instead of the pinned async-http's `INTERNAL_ERROR`.
  Native servers treat only a peer `CANCEL` as explicit cancellation. A stream
  upload cancelled while it waits for credit takes the same path. Servers send
  no cancellation reset: handler cancellation is an ordinary `canceled`
  response, and a client reset or lost connection closes the stream without
  another frame. M1: the Async and Falcon hosts pass each exchange, publication,
  rejected Web input and Rack completion the exception from its own unwind
  instead of reading `$ERROR_INFO`. Before the change, only async-http's
  HTTP/1 malformed-request response reached a host site with an unrelated
  exception, and no RPC exchange was live there. The Falcon Rack completion
  body still reads `$ERROR_INFO` on an argless close, because protocol-rack
  closes a rejected Rack response from its own rescue clause without passing
  the exception.
- L12 is the accepted Rack adapter limitation described above.
  [#46](https://github.com/jimeh/connectrpc-ruby/pull/46), merged as `805bb80`,
  resolved L18's documentation drift.

### Before a release adopts the profile

1. Choose the release: a future 0.x release, chosen once the library is judged
   stable. A 1.x release comes later, after the contract has proven stable for
   some time.
2. Qualify that release's revision with its declared CI, installed-caller and
   protocol gates.
3. Record adoption in that release. A documentation change replaces the notice
   at the top of this guide with the adopting version, and the release notes
   state that the version adopts this contract.

Release automation ([#36](https://github.com/jimeh/connectrpc-ruby/issues/36))
is described in [releasing](releasing.md). Merging a release PR is the separate
release decision.
