<div align="center">

<img height="196px" width="196px" src="./img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-ruby

**Typed Connect and gRPC clients and servers for Ruby.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![GitHub Issues](https://img.shields.io/github/issues/jimeh/connectrpc-ruby?logo=github&label=Issues)](https://github.com/jimeh/connectrpc-ruby/issues)
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/jimeh/connectrpc-ruby?logo=github&label=PRs)](https://github.com/jimeh/connectrpc-ruby/pulls)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

connectrpc-ruby is an unofficial Ruby implementation of the
[Connect protocol](https://connectrpc.com/docs/protocol/). You describe a
service in a `.proto` file, generate typed Ruby clients and handler maps, and
serve the same handlers from Rack, Rails, Async or Falcon. The same
declarations also speak native [gRPC](docs/grpc.md) and server-side
[gRPC-Web](docs/grpc-web.md).

- **Typed on both sides.** `protoc-gen-connectrpc-ruby` generates service
  modules alongside Google's official `_pb.rb` messages, so clients and
  handlers exchange protobuf objects instead of hashes.
- **Plain HTTP.** A Connect unary call is an HTTP POST with a protobuf or JSON
  body, so `curl` and ordinary HTTP tooling work. Methods declared free of side
  effects can opt into GET.
- **Host-neutral handlers.** A handler is a callable that takes a request and a
  context. The same handler runs in Rack middleware, a Rails controller, the
  standalone Async HTTP/2 host, Falcon workers, or an in-process test transport.
- **No runtime dependencies in core.** `require "connectrpc"` loads no other
  gems. Protobuf, Rack, Rails, Async, Falcon and OpenTelemetry support are
  explicit opt-ins.
- **Streaming and gRPC where the host supports them.** Async and Falcon HTTP/2
  hosts serve all four RPC shapes over Connect and gRPC.

> [!WARNING]
> This project is experimental. Every gem is published on RubyGems, but no
> release is declared supported or production ready yet. The
> [supported 0.x contract](docs/support.md) defines the target profile.
> Adoption depends on the open
> [#26 findings](https://github.com/jimeh/connectrpc-ruby/issues/26) and release
> qualification.

## Quick start

This walkthrough builds a one-method service, serves it with Rack and calls it
from Ruby and from `curl`. You need CRuby 3.4 or newer, Bundler, and
[`protoc`](https://protobuf.dev/installation/) on your PATH. The project
verifies with protoc 36.2.

1. Add the core gem, the Rack integration, Google's protobuf runtime and a
   Rack server to your Gemfile, then run `bundle install`:

   ```ruby
   gem "connectrpc", "~> 0.3.0"
   gem "connectrpc-rack", "~> 0.3.0"
   gem "google-protobuf", ">= 4.36.2", "< 5"
   gem "rackup"
   gem "webrick"
   ```

2. Describe the service in `proto/greet/v1/greet.proto`:

   ```proto
   syntax = "proto3";

   package greet.v1;

   message GreetRequest {
     string name = 1;
   }

   message GreetResponse {
     string greeting = 1;
   }

   service GreetService {
     rpc Greet(GreetRequest) returns (GreetResponse);
   }
   ```

3. Generate the messages and the service module into `lib/`. Running protoc
   through `bundle exec` puts the gem's `protoc-gen-connectrpc-ruby` plugin on
   PATH:

   ```sh
   mkdir -p lib
   bundle exec protoc -I proto --ruby_out=lib --connectrpc-ruby_out=lib greet/v1/greet.proto
   ```

   This writes `lib/greet/v1/greet_pb.rb` and `lib/greet/v1/greet_connect.rb`.
   Commit them or regenerate them in your build; don't edit them by hand.

4. Implement the handler and mount it in `config.ru`:

   ```ruby
   $LOAD_PATH.unshift(File.expand_path("lib", __dir__))
   require "connectrpc/rack"
   require "greet/v1/greet_connect"

   class Greeter
     def greet(request, _context)
       Greet::V1::GreetResponse.new(greeting: "Hello, #{request.name}!")
     end
   end

   service = Greet::V1::GreetService
   server = ConnectRPC::Server.new(
     services: [service::SERVICE],
     handlers: service.handlers_for(Greeter.new, strict: true)
   )
   run ConnectRPC::Rack.new(server: server)
   ```

   Since 0.3.0, the generated file loads the Google protobuf binding, so the
   Server and Client use it without a `binding:` argument, and
   `handlers_for` maps each RPC to the object's method of the same snake-case
   name. `service.handlers(greet: ->(request, context) { ... })` takes one
   callable per RPC instead.

5. Start the server:

   ```sh
   bundle exec rackup --host 127.0.0.1 --port 9292
   ```

6. From another terminal, call it with `curl`. Connect accepts JSON as well as
   binary protobuf:

   ```console
   $ curl -H "Content-Type: application/json" -d '{"name":"curl"}' \
       http://127.0.0.1:9292/greet.v1.GreetService/Greet
   {"greeting":"Hello, curl!"}
   ```

7. Call it from Ruby with the generated client. Save this as `client.rb` and
   run `bundle exec ruby client.rb`:

   ```ruby
   $LOAD_PATH.unshift(File.expand_path("lib", __dir__))
   require "connectrpc"
   require "greet/v1/greet_connect"

   core = ConnectRPC::Client.new(base_url: "http://127.0.0.1:9292")
   client = Greet::V1::GreetService::Client.new(client: core)
   response = client.greet(Greet::V1::GreetRequest.new(name: "Ruby"), timeout: 2)
   puts response.message.greeting # => Hello, Ruby!
   ```

The core client uses Net::HTTP and binary protobuf by default. Pass
`codec: :json` for JSON, or `transport:` to use another transport. Each call
accepts `metadata:`, `timeout:` (30 seconds by default), `deadline:` and
`cancellation:`. Since 0.3.0, a handler that calls another service passes
`deadline: context.deadline` so the downstream call ends no later than its own.
Handlers raise `ConnectRPC::Error` to return a typed error code to the caller.

Next steps:

- The [unary guide](docs/unary.md) covers generation options, GET, gzip,
  metadata, errors, limits, deadlines and cancellation.
- Unexpected handler exceptions reach the client only as a generic `internal`
  error. Since 0.3.0, pass `on_error:` to the Server to log or report them;
  see [errors](docs/unary.md#errors).
- The [runnable examples](examples/README.md) pair Rack, Rails, Async and
  Falcon servers with Net::HTTP and Async clients, including mutual TLS.
- The [streaming guide](docs/streaming.md) covers server, client and
  bidirectional streaming.

## Packages

Every package is a separate gem. Install the core gem and add only the
integrations you use:

| Gem | Use it when | Require |
| --- | --- | --- |
| [`connectrpc`](https://rubygems.org/gems/connectrpc) | Always. Core client, Net::HTTP transport, host-neutral server, Google protobuf binding and the `protoc-gen-connectrpc-ruby` generator | `connectrpc`, `connectrpc/bindings/google_protobuf` |
| [`connectrpc-rack`](gems/connectrpc-rack/README.md) | You serve unary RPCs from any Rack 3 application or server | `connectrpc/rack` |
| [`connectrpc-on-rails`](gems/connectrpc-on-rails/README.md) | You want handlers to run as Rails controller actions, with callbacks, `rescue_from` and Rails instrumentation | `connectrpc/on_rails` |
| [`connectrpc-async`](gems/connectrpc-async/README.md) | You need a standalone HTTP/2 host, streaming, gRPC, a pooled HTTP/2 or HTTP/1.1 client, or a shared HTTP/2 client for threaded apps such as Puma and Sidekiq | `connectrpc/async` |
| [`connectrpc-falcon`](gems/connectrpc-falcon/README.md) | You host on Falcon workers and want native HTTP/1.1 and HTTP/2 RPC next to an ordinary Rack or Rails app, with managed TLS | `connectrpc/falcon` |
| [`connectrpc-testing`](gems/connectrpc-testing/README.md) | You want real encoded client/server round trips in tests without opening sockets | `connectrpc/testing` |
| [`connectrpc-otel`](gems/connectrpc-otel/README.md) | You want OpenTelemetry CLIENT and SERVER spans for each RPC | `connectrpc/otel` |
| [`connectrpc-prometheus`](gems/connectrpc-prometheus/README.md) | You want go-grpc-prometheus compatible server, client and Async connection metrics in a prometheus-client registry | `connectrpc/prometheus` |
| [`connectrpc-yabeda`](gems/connectrpc-yabeda/README.md) | You want the same metrics through Yabeda and any of its adapters | `connectrpc/yabeda` |
| [`connectrpc-health`](gems/connectrpc-health/README.md) | You want the standard `grpc.health.v1` service (Check, List and Watch) for Kubernetes probes, `grpc_health_probe` and load balancers | `connectrpc/health` |
| [`connectrpc-sentry`](gems/connectrpc-sentry/README.md) | You use the Sentry Ruby SDK and want one native Sentry transaction or span per RPC side, plus reports of unexpected handler exceptions | `connectrpc/sentry` |
| [`connectrpc-hyper`](gems/connectrpc-hyper/README.md) | Since 0.3.0: you want a pooled HTTP/2 client for any thread, with every call shape and native gRPC, without an Async reactor. A Rust extension, with prebuilt platform gems | `connectrpc/hyper` |

Use the same version of `connectrpc` and every ConnectRPC gem in an
application. Their `~>` dependency ranges allow patch versions to differ, but the
[support policy](docs/support.md#match-the-connectrpc-package-versions)
requires matching versions.

### Pick packages by scenario

Every scenario also needs `connectrpc`, and `google-protobuf` for generated
code. The [protocol and host matrix](#protocol-and-host-support) lists exactly
what each host and transport supports.

| Scenario | Add | How |
| --- | --- | --- |
| A Rails app serves RPCs | `connectrpc-on-rails` | [Rails controller usage](gems/connectrpc-on-rails/README.md#usage). Rails serves Connect and gRPC-Web unary only, not native gRPC or streaming. |
| Puma or Sidekiq calls a Go service (connect-go or grpc-go) | `connectrpc-async` | [`ThreadedTransport`](gems/connectrpc-async/README.md#ordinary-threads) from any thread, with `protocol: :grpc` for grpc-go. Core's Net::HTTP transport is enough for Connect unary calls to connect-go. Since 0.3.0, [`connectrpc-hyper`](gems/connectrpc-hyper/README.md#which-transport) covers every call shape from any thread. |
| You run a gRPC server | `connectrpc-async` | The [gRPC quick start](docs/grpc.md#quick-start) serves Connect and native gRPC from one Async `Host`. |
| Kubernetes health checks | `connectrpc-health`, plus `connectrpc-async` or `connectrpc-falcon` | [Health hosts](gems/connectrpc-health/README.md#hosts). Kubernetes `grpc:` probes need native gRPC over HTTP/2. |
| RPCs run next to a Rack or Rails app on Falcon | `connectrpc-falcon` | [Run a service](gems/connectrpc-falcon/README.md#run-a-service) with the app as the Rack fallback. |
| Browser clients call the server | The host's package | [gRPC-Web](docs/grpc-web.md) for grpc-web clients. Cross-origin Connect clients such as connect-es need [outer CORS middleware](docs/grpc-web.md#cors-and-rails). |
| Tests call handlers without sockets | `connectrpc-testing` | [Testing usage](gems/connectrpc-testing/README.md#usage). |
| You need traces, metrics or error reports | `connectrpc-otel`, `connectrpc-prometheus` or `connectrpc-yabeda`, `connectrpc-sentry` | Each package README, linked in the table above. |

## Protocol and host support

U means unary, SS server streaming, CS client streaming and bidi bidirectional
streaming. H1 is HTTP/1.1 and H2 is HTTP/2.

| Server | Connect | Native gRPC | gRPC-Web |
| --- | --- | --- | --- |
| Rack middleware | U over H1 | No | U, binary and text |
| Rails controller | U | No | U, binary and text |
| Async `Host` | U/SS/CS/bidi over H2 (h2c, TLS, mTLS) | U/SS/CS/bidi | U/SS over H2 |
| Falcon | U/SS/CS over H1; U/SS/CS/bidi over H2 (TCP, TLS, mTLS) | U/SS/CS/bidi over H2 | U/SS over H1 and H2 |
| Testing transport | U, in process | No | U, manually encoded |

| Client transport | Connect | Native gRPC |
| --- | --- | --- |
| Net::HTTP (core default) | U over H1, one connection per call (TCP, TLS, mTLS, HTTP proxies) | No |
| Async `Transport` | U/SS/CS/bidi over H2 (h2c, TLS, mTLS) | U/SS/CS/bidi |
| Async `ThreadedTransport` | U/SS over H2, callable from any thread | U/SS |
| Async `HTTP1Transport` | U/SS/CS over H1 (TCP, TLS, mTLS) | No |
| Hyper `Transport` (since 0.3.0) | U/SS/CS/bidi over H2 (h2c, TLS, mTLS), U/SS/CS over H1, from any thread | U/SS/CS/bidi |

Connect is the default protocol. Connect supports binary protobuf
(`application/proto`) and protobuf JSON (`application/json`), identity or
opt-in gzip encoding, and opt-in GET for safe unary methods. Native gRPC uses
protobuf with identity or gzip. gRPC-Web is server-only and must be enabled
explicitly; there is no Ruby gRPC-Web client.

These limits apply across the matrix:

- connectrpc-async's `ThreadedTransport` covers unary and server streaming
  from ordinary threads, for Connect and native gRPC; client and bidi
  streaming there are tracked in
  [issue #29](https://github.com/jimeh/connectrpc-ruby/issues/29). Since
  0.3.0, connectrpc-hyper's `Transport` makes every call shape from any
  thread.
- The standalone Async `Host` is H2-only. Falcon is the Ruby H1 streaming host.
- Native Connect streaming clients apply a provisional one-second HTTP tail
  policy, even without an RPC deadline. See the
  [streaming guide](docs/streaming.md).
- There is no H1 bidi, HTTP/3, protocol fallback or RPC replay. Faraday is
  unsupported. The Async H2 `Transport` resends a Connect unary call once only
  when the server provably never processed its stream; see
  [client connection management](gems/connectrpc-async/README.md#reroutes-and-the-refused-stream-retry).

The [support contract](docs/support.md#protocol-and-host-matrix) lists the
evidence behind each row, the qualified Ruby and dependency versions, and the
remaining adoption gates. CRuby 3.4 or newer is required; project verification
targets Ruby 3.4.12 and 4.0.9 on Linux x64.

## Documentation

Guides on `main` describe the next release. "Since X.Y.Z" marks a feature,
option or metric that older installs lack. To read the guides for an installed
version, browse the repository at its tag, such as
[`v0.2.1`](https://github.com/jimeh/connectrpc-ruby/tree/v0.2.1).

| Guide | Covers |
| --- | --- |
| [Unary RPC](docs/unary.md) | Code generation, manual declarations, GET, gzip, metadata, errors, limits, deadlines, cancellation and instrumentation |
| [Streaming](docs/streaming.md) | Server, client and bidirectional streaming, completion policy and bounds |
| [Native gRPC](docs/grpc.md) | gRPC clients and servers on the Async and Falcon HTTP/2 hosts |
| [gRPC-Web](docs/grpc-web.md) | Server-side gRPC-Web, CORS and browser clients |
| [Signatures (RBS)](docs/rbs.md) | Shipped RBS signatures, generated `.rbs` files and Steep setup |
| [Support contract](docs/support.md) | Target 0.x protocol, host, platform and API compatibility |
| [Upgrading](docs/upgrading.md) | Migration notes for releases with breaking changes |
| [Design](docs/design.md) | Internal boundaries, wire behavior and extension seams |
| [Runnable examples](examples/README.md) | Two-terminal servers and clients for every host and transport |
| [Development](docs/development.md) | Setup, verification tiers, coverage, updates and PR guidance |
| [Releasing](docs/releasing.md) | The synchronized release train and trusted publishing |

## Development

Install [Mise](https://mise.jdx.dev/) 2026.9.14 or newer and the
[Ruby source-build prerequisites](https://github.com/rbenv/ruby-build/wiki#suggested-build-environment),
then run:

```sh
mise run setup
mise run example
mise run verify
mise run rack:verify
```

[The example](examples/unary.rb) starts Rack on an ephemeral loopback port,
calls a typed handler through real Net::HTTP in both codecs, prints the results,
and closes the host and its workers. Its schema comes from the shared
[descriptor fixture](spec/fixtures/echo.proto). Go is needed only for interop
and final verification, and those tasks install its project pin separately.
`mise tasks` lists every task, and the [development guide](docs/development.md)
explains the verification tiers.

## License and releases

Original project work is licensed under the [MIT License](LICENSE), copyright
2026 Jim Myhrberg. Every gem archive includes the complete license.
Third-party dependencies and imported fixtures retain their own licenses and
notices, including the separately licensed
[conformance schemas](conformance/README.md).

The gems pin `allowed_push_host` to `https://rubygems.org` and require MFA for
manual pushes. Releases are published only by the tag-triggered
[release workflow](docs/releasing.md), using RubyGems trusted publishing with
Sigstore attestations.
