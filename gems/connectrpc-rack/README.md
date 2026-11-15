<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-rack

**Rack middleware for serving ConnectRPC unary services.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-rack?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-rack)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-rack` serves a [connectrpc](../../README.md) `Server` from any Rack 3
application or server. Use it when you want Connect unary RPCs in an existing
Rack stack, or when you want the simplest way to put a service on the network.
For Rails controller callbacks and `rescue_from`, use
[connectrpc-on-rails](../connectrpc-on-rails/README.md) instead. For streaming
or HTTP/2, use [connectrpc-async](../connectrpc-async/README.md) or
[connectrpc-falcon](../connectrpc-falcon/README.md).

The package is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

Requires Ruby >= 3.4, `connectrpc ~> 0.1.0` and `rack >= 3.2.7, < 4`. Add the
gem to your Gemfile; it installs compatible core and Rack dependencies:

```ruby
gem "connectrpc-rack", "~> 0.1.0"
```

Core installed alone has no runtime dependencies, loads no Rack, and has no
`connectrpc/rack` file. Applications that used middleware from `connectrpc`
before it moved to this package must add this dependency. Keep
`require "connectrpc/rack"`, `ConnectRPC::Rack.new(server: server)` and
`call(env)` unchanged. The entry point loads core normally. This changes
installation only; Rack already required an explicit optional require.

## Usage

Build a host-neutral `ConnectRPC::Server` with your binding and handlers, then
wrap it:

```ruby
require "connectrpc/rack"

# Build a host-neutral ConnectRPC::Server with your binding and handlers.
app = ConnectRPC::Rack.new(server: server)
# Or preserve an existing application's unrelated routes:
app = ConnectRPC::Rack.new(downstream_app, server: server)
```

Supply the app to your Rack host, for example with `run app` in `config.ru`.
This package does not start a production host. The root
[quick start](../../README.md#quick-start) shows a complete service, and
[examples/unary.rb](../../examples/unary.rb) hosts one on an ephemeral port.

## Routing and translation

The middleware translates `PATH_INFO`, raw `QUERY_STRING`, request headers and
`rack.input` into core Server calls and returns Rack status, headers and body.

- Mount prefixes use `SCRIPT_NAME`.
- Registered service paths belong to ConnectRPC, including unknown methods.
- Unrelated paths call the downstream app, or return 404 when none was supplied.
- Repeated response metadata and `trailer-*` fields remain Rack 3 arrays.
- Rack env normalization loses the distinction between underscore and hyphen
  request metadata names; native metadata acceptance is unchanged.

The core Server owns unary proto/JSON negotiation, identity/opt-in gzip, bounded
input/output, typed errors and cooperative deadlines. See the
[unary guide](../../docs/unary.md) for those options.

### Limitations

Synchronous Rack cannot interrupt blocked reads or arbitrary handlers or
reliably detect disconnects. Rack adds no peer cancellation detection or
cancellation token env injection. Streaming is rejected before input. See
[design](../../docs/design.md) for the ownership boundaries.

## gRPC-Web

Explicitly enabled binary/text unary gRPC-Web runs through this middleware,
including the rackup/WEBrick fixture. Streaming is rejected before input.
Configure core CORS or keep it nil when outer middleware owns the mount. See
the [Web guide](../../docs/grpc-web.md) for protocol, CORS, bounds and
completion rules.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional
[OpenTelemetry adapter](../connectrpc-otel/README.md). A supplied
`rack.response_finished` extension takes precedence over body close. Without
it the observation is only `body_closed`, with no delivery promise. Consumers
must close bodies or invoke promised callbacks.

## Static signatures

This package owns `sig/` and its discovery manifest. Add
`library "connectrpc-rack"` to a strict Steep target with this gem installed.
See the [RBS guide](../../docs/rbs.md) for supported APIs, extension contracts,
partial records, runtime validation and framework-signature limits.
`mise run rbs:check` checks application callers for this package; strict core
source checking is a separate target from backend implementation checking.

## Development

The independent development bundle resolves the newest allowed Rack and reuses
the root tool pins; `floors:rack` repeats specs and package checks at Rack 3.2.7.
Run from the repository root:

```sh
mise run rack:setup
mise run rack:test
mise run rack:check
mise run rack:package
mise run verify
mise run rack:verify
```

Focused specs accept selectors, for example
`mise run rack:test -- spec/rack_spec.rb`. Bundles, Bundler state, lint cache and
archives live under `vendor/rack`; coverage is `coverage/rack/<Ruby>/` and starts
before the middleware require.

`rack:package` builds its own core/Rack archives, checks exact
manifests/dependencies/push hosts, installs core alone and then resolves the
Rack package offline in a fresh external GEM_HOME. Real Google proto/JSON
identity/gzip calls, safe errors/details and metadata run under Rack::Lint over
ephemeral localhost NetHTTP, using installed sources only.

CI runs Rack specs/package checks on Ruby 3.4.12 and 4.0.9. Final local handoff
requires both `verify` and `rack:verify`; root verification composition is
unchanged.

## License

This package uses the [MIT license](LICENSE), copyright 2026 Jim Myhrberg. It is
released with the whole [release train](../../docs/releasing.md).
