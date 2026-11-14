# connectrpc-rack

Optional, experimental Rack middleware for the unofficial Connect unary Ruby gem.
Requires Ruby >= 3.4, `connectrpc ~> 0.1.0` and `rack >= 3.2.7, < 4`.
This package uses the [MIT license](LICENSE), copyright 2026 Jim Myhrberg.
It is released with the whole [release train](../../docs/releasing.md).

See the [supported 0.x contract](../../docs/support.md) for the target support
profile, matched package versions, compatibility policy and adoption gates.

## Installation and migration

Rack middleware now ships in this separate package. Applications that previously
used middleware from `connectrpc` must add this dependency to their Gemfile:

```ruby
gem "connectrpc-rack", "~> 0.1.0"
```

Keep `require "connectrpc/rack"`, `ConnectRPC::Rack.new(server: server)` and
`call(env)` unchanged. The entry point loads core normally. Core installed alone
has no runtime dependencies, loads no Rack, and has no `connectrpc/rack` file.
This changes installation; Rack already required an explicit optional require.

## Usage

```ruby
require "connectrpc/rack"

# Build a host-neutral ConnectRPC::Server with your binding and handlers.
app = ConnectRPC::Rack.new(server: server)
# Or preserve an existing application's unrelated routes:
app = ConnectRPC::Rack.new(downstream_app, server: server)
```

Supply the app to your Rack host. This package does not start a production host.
It translates `PATH_INFO`, raw `QUERY_STRING`, request headers and `rack.input` into core Server
calls and returns Rack status, headers and body. Mount prefixes use `SCRIPT_NAME`;
registered service paths belong to ConnectRPC, including unknown methods.
Unrelated paths call the downstream app, or return 404 when none was supplied.
Repeated response metadata and `trailer-*` fields remain Rack 3 arrays. Rack env
normalization loses the distinction between underscore and hyphen request
metadata names; native metadata acceptance is unchanged.

The core Server owns unary proto/JSON negotiation, identity/opt-in gzip, bounded
input/output, typed errors and cooperative deadlines. Synchronous Rack cannot
interrupt blocked reads or arbitrary handlers or reliably detect disconnects.
See [design](../../docs/design.md), [examples](../../examples/unary.rb) and the
[optional Rails adapter](../connectrpc-rails/README.md) for native controller execution.

## Development

The independent frozen development bundle reuses existing exact registry pins.
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
before the middleware require. `rack:package` builds its own core/Rack archives,
checks exact manifests/dependencies/publication guards, installs core alone and
then resolves the Rack package offline in a fresh external GEM_HOME. Real Google
proto/JSON identity/gzip calls, safe errors/details and metadata run under
Rack::Lint over ephemeral localhost NetHTTP, using installed sources only.
CI runs Rack specs/package checks on Ruby 3.4.12 and 4.0.9. Final local handoff
requires both `verify` and `rack:verify`; root verification composition is unchanged.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional [OpenTelemetry adapter](../connectrpc-otel/README.md).
A supplied `rack.response_finished` extension takes precedence over body close.
Without it the observation is only `body_closed`, with no delivery promise.
Consumers must close bodies or invoke promised callbacks.

## Static signatures

This package owns `sig/` and its discovery manifest. Add
`library "connectrpc-rack"` to a strict Steep target with this gem installed.
See the [RBS guide](../../docs/rbs.md) for supported APIs, extension contracts,
partial records, runtime validation and framework-signature limits.
`mise run rbs:check` checks application callers for this package; strict core
source checking is a separate target from backend implementation checking.

## gRPC-Web

Explicitly enabled binary/text unary gRPC-Web runs through this middleware, including the rackup/WEBrick fixture. Streaming is rejected before input. Configure core CORS or keep it nil when outer middleware owns the mount.
See the [Web guide](../../docs/grpc-web.md) for protocol, CORS, bounds and completion rules.
