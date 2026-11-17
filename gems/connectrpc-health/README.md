<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-health

**The standard gRPC health service for ConnectRPC servers.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-health?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-health)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-health` implements
[`grpc.health.v1.Health`](https://github.com/grpc/grpc/blob/master/doc/health-checking.md)
(`Check`, `List` and `Watch`) on a status registry that any thread or fiber can
update. Kubernetes gRPC probes, `grpc_health_probe`, load balancers and grpc-go
health clients can query the service, and the generated Ruby client calls it
over Connect or gRPC.

The package is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

```ruby
gem "connectrpc-health", "~> 0.2.0"
```

The runtime dependencies are `connectrpc ~> 0.2.0` and
`google-protobuf >= 4.36.2, < 5`. Core and the hosting packages do not depend
on this package.

## Usage

```ruby
require "connectrpc/bindings/google_protobuf"
require "connectrpc/health"

health = ConnectRPC::Health::Registry.new(max_watchers: 100)
health.set("demo.v1.DemoService", :serving)

server = ConnectRPC::Server.new(
  services: [Demo::V1::DemoService::SERVICE, ConnectRPC::Health::SERVICE],
  handlers: demo_handlers.merge(health.handlers),
  binding: ConnectRPC::Bindings::GoogleProtobuf.new, protocol: :auto
)
```

| Method | Behavior |
| --- | --- |
| `set(service, status)` | Stores `:unknown`, `:serving` or `:not_serving` and returns true. Returns false without a change after `shutdown`. |
| `status(service)` | The stored status, or nil for an unregistered name. |
| `shutdown` | Marks every entry `:not_serving` and ends Watch streams. Idempotent. |
| `shutdown?` | Whether `shutdown` ran. |
| `watcher_count` | Admitted Watch streams. |
| `handlers` | A frozen Hash of the Check, List and Watch handlers keyed by path. |

The overall server entry `""` starts `:serving`. Service names are Strings and
must be valid UTF-8; the registry stores frozen copies. Other names, statuses and
a `max_watchers:` that is not a positive Integer raise `ArgumentError`. There is
no unregister. `ConnectRPC::Health::SERVICE` is the generated
`Grpc::Health::V1::Health::SERVICE`.

- **Check** returns the stored status. An unregistered name fails `not_found`.
- **List** returns a snapshot of every entry. More than 100 entries fail
  `resource_exhausted`, as grpc-go does.
- **Watch** sends the current status, or `SERVICE_UNKNOWN` for an unregistered
  name, then each change. A slow stream sees only the latest status, never a
  queue, and a status equal to the last one sent is skipped. More than
  `max_watchers` concurrent streams fail `resource_exhausted` before any message.
  A Watch deadline ends the stream with `deadline_exceeded`, and peer
  cancellation ends it with `canceled`.

Status updates only signal the stream; each Watch handler sends on its own
reactor or thread. `set` from a plain thread wakes a watcher on an Async
reactor.

## Hosts

| Host | Check and List | Watch |
| --- | --- | --- |
| Async Host (HTTP/2: h2c, TLS, mTLS) | Connect and gRPC | Connect and gRPC |
| Falcon HTTP/2 | Connect and gRPC | Connect and gRPC |
| Falcon HTTP/1.1 | Connect | Connect |
| Rack, Rails, Testing | Connect (gRPC-Web unary where enabled) | Rejected by protocol |

Portable adapters reject Watch the same way they reject any server stream:
Connect streaming media gets 415 before dispatch, a unary-encoded request to the
Watch path fails `unimplemented`, and a generated Watch call on a unary-only
transport fails locally with `ArgumentError`.

Kubernetes [`grpc:` probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-grpc-liveness-probe)
and `grpc_health_probe` need native gRPC, so serve health from the Async Host or
Falcon over HTTP/2. Kubernetes probes use plaintext h2c only. A second
plaintext `Host` on a probe port can share the registry with a TLS application
host; that separate port is also the recommended authentication boundary.

Kubelet probes the pod IP, so bind the probe host to `0.0.0.0`. The default
`127.0.0.1` accepts only loopback connections, and every probe would fail.
`Host#start` also needs an application-owned Async task; see
[reactor ownership](../connectrpc-async/README.md#reactor-ownership). Both
hosts run in the same reactor. `server` is the application server from
[Usage](#usage), and `tls` is its
[TLS hash](../connectrpc-async/README.md#tls-and-client-authentication):

```ruby
require "async"
require "connectrpc/async"

probe = ConnectRPC::Server.new(services: [ConnectRPC::Health::SERVICE], handlers: health.handlers,
                               binding: ConnectRPC::Bindings::GoogleProtobuf.new, protocol: :grpc)
Sync do
  app_host = ConnectRPC::Async::Host.new(server: server, host: "0.0.0.0", port: 8443, tls: tls).start
  probe_host = ConnectRPC::Async::Host.new(server: probe, host: "0.0.0.0", port: 8081).start
  app_host.wait # To stop, call health.shutdown, then app_host.close(grace: 5).
ensure
  health.shutdown
  app_host&.close(grace: 5)
  probe_host&.close(grace: 5)
end
```

The pod's probe then targets port 8081:

```yaml
readinessProbe:
  grpc:
    port: 8081
```

With Falcon, each worker process builds its own server, so a registry is
worker-local. Set statuses inside each worker, for example while building
`connectrpc_server`.

### Authentication

Server [interceptors](../../docs/unary.md#interceptors) on the application's
`Server` cover Check and List through `server_unary` and Watch through
`server_stream`. To expose health behind authentication, reject before `yield`
so a denied call never reaches the registry. A denied Watch then reveals no
status and allocates no watcher. `invoker:` is not enough: it covers unary
calls only, and Watch bypasses it.

```ruby
class HealthAuth
  def server_unary(call, &) = authorize!(call, &)
  def server_stream(call, &) = authorize!(call, &)

  private

  # authorized?(context) is the application's own check, for example of a
  # credential in context.request_metadata.
  def authorize!(call)
    if call.method.service == ConnectRPC::Health::SERVICE.name && !authorized?(call.context)
      raise ConnectRPC::Error.new(:unauthenticated, "credentials required")
    end

    yield
  end
end

server = ConnectRPC::Server.new(services: services, handlers: handlers, binding: binding,
                                protocol: :auto, interceptors: [HealthAuth.new])
```

An interceptor's `server_stream` frame, and anything it holds, lasts as long as
the Watch stream. Avoid holding scarce resources across `yield`.

## Shutdown and drain

Call `health.shutdown` before `host.close(grace:)`. Every entry becomes
`NOT_SERVING`, existing Watch streams send `NOT_SERVING` if they have not
already sent it and end with OK, and later Watch calls fail `unavailable`
before sending any message. A message followed by a normal end would reset the
grpc-go client's backoff and cause tight retry loops during drain. Graceful
close then finishes well before grace expires. Final delivery is best effort,
bounded by the host's grace. grpc-go's own server keeps Watch streams open after
`Shutdown`; this registry ends them so hosts can drain.

A host that closes without `health.shutdown` releases its watchers through
grace expiry, forced close or connection loss.

### Watch streams the server cannot see end

Peer `RST_STREAM(CANCEL)`, HTTP/2 connection loss and host retirement release a
watcher immediately. Other HTTP/2 reset codes and an HTTP/1.1 client that
disappears while idle signal nothing to the handler. Such a watcher is released
when its next send attempt fails, at connection retirement or at
`shutdown`. Only a change that differs from the last status sent triggers a
send; changes that coalesce back to it before the stream runs send nothing.
`max_watchers` bounds that cost; there is no separate Watch
lifetime limit.

## grpc gem load order

The generated files use the canonical feature name
`grpc/health/v1/health_pb`, which the `grpc` gem shares. This package's
descriptor is a superset that adds List. `require "connectrpc/health"` loads
its own file by path, even when the `grpc` gem comes first on the load path,
and the `grpc` gem's later require of that feature name reuses it. When an older descriptor without List is already loaded,
`require "connectrpc/health"` raises a `LoadError` that asks you to require it
before the grpc gem's health files.

## Schema

`proto/grpc/health/v1/health.proto` is the unchanged canonical schema from
grpc/grpc-proto at a pinned commit; [`proto/README.md`](proto/README.md) records
the commit, SHA256 and license. `mise run generation:build` regenerates
`lib/grpc/health/v1/` and `rbs/grpc_health_v1/grpc/health/v1/`, and
`mise run generation:check` checks them. Never edit the generated files by hand.

## Development

From the repository root use `mise run health:setup`, `health:test`,
`health:check`, `health:build`, `health:package`, `health:interop` and
`health:verify`. The independent development bundle resolves the newest allowed
versions and includes the local Rack, Async, Falcon and testing packages for
real-host specs. Caches and archives are in `vendor/health`, and coverage is in
`coverage/health/<Ruby>/`. Both pinned Ruby 3.4.12 and 4.0.9 CI leaves run
`health:verify`.

`health:interop` runs grpc-go's standard `grpc_health_v1` client from
`interop/grpc` against the Async Host and real Falcon workers over h2c, TLS and
mTLS, and runs the same client script against grpc-go's reference
`health.NewServer()`. The reference has no watcher count, and after `Shutdown`
it keeps streams open and admits new watchers; the script records those two
differences. `floors:health` repeats specs and package checks with every
runtime dependency at its gemspec floor.

## Static signatures

This package owns `sig/` and its discovery manifest. Add `library "connectrpc"`
and `library "connectrpc-health"` to a strict Steep target with this gem
installed to type the registry and `ConnectRPC::Health::SERVICE`. The manifest
lists only standard libraries, so name each connectrpc library explicitly; this
also works with `rbs collection`.

The generated `grpc.health.v1` message, client and handler factory signatures
are an explicit opt-in in `rbs/grpc_health_v1`, outside normal gem discovery.
The generated messages subclass `Google::Protobuf::AbstractMessage`, which only
core's opt-in `rbs/google_protobuf_4_36` bridge (or your own protobuf runtime
signatures) declares. Steep 2.1.0 builds `library` signatures before project
`signature` paths, so shipping them in `sig/` would break the library itself.
To type calls on the generated client or messages, add both directories as
Steepfile-relative `signature` paths:

```ruby
require "pathname"
bridges = { "connectrpc-health" => "rbs/grpc_health_v1", "connectrpc" => "rbs/google_protobuf_4_36" }
  .map do |name, directory|
    Pathname(Gem::Specification.find_by_name(name).full_gem_path).join(directory)
      .relative_path_from(Pathname(__dir__)).to_s
  end

target :app do
  library "connectrpc"
  library "connectrpc-health"
  signature "sig", *bridges
  check "app"
  configure_code_diagnostics(Steep::Diagnostic::Ruby.strict)
end
```

See the [RBS guide](../../docs/rbs.md#opt-in-framework-compatibility).

## License

Original project work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg.
The vendored health schema and the code generated from it are Apache 2.0
licensed; see [`proto/LICENSE`](proto/LICENSE). All package archives pin
`allowed_push_host=https://rubygems.org`; these checks never publish or release
a gem.
