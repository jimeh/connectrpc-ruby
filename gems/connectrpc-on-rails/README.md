<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-on-rails

**Run ConnectRPC unary handlers as native Rails controller actions.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-on-rails?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-on-rails)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-on-rails` runs generated Connect unary handlers inside native Rails
controllers. Handlers execute through Rails' real `process_action` chain, so
`before_action` callbacks, `rescue_from`, around-action cleanup and Rails
instrumentation all apply. Use it when your RPCs need the authentication,
error handling and request context your Rails controllers already have. To
mount a service in front of Rails without controller callbacks, use
[connectrpc-rack](../connectrpc-rack/README.md) instead.

The core gem remains independent of Rails and has zero runtime dependencies.
This adapter reuses its Server invoker and the connectrpc-rack translation. It
does not add another protocol implementation. Streaming, native gRPC and
production hosting are outside this adapter.

The package is experimental and not production ready. See the
[supported 0.x contract](../../docs/support.md) for the target support profile,
matched package versions, compatibility policy and adoption gates.

## Installation

Add `connectrpc-on-rails` and your chosen protobuf binding dependency to your
application Gemfile:

```ruby
gem "connectrpc-on-rails", "~> 0.2.0"
gem "google-protobuf", ">= 4.36.2", "< 5"
```

Runtime dependencies are `connectrpc ~> 0.2.0`, `connectrpc-rack ~> 0.2.0`, and
`actionpack >= 8.0.5.1, < 8.2`. The Rack package supplies the
`rack >= 3.2.7, < 4` requirement. The application chooses its controller base,
binding, authentication, and security policy.

Requiring `connectrpc/on_rails` loads neither `ActionController::Base` nor
`ActionController::API`, and does not load Google protobuf. `Bundler.require`
loads the same entry point through `connectrpc-on-rails`.

## Usage

### 1. Generate code outside Zeitwerk

Generate official protobuf messages and Connect service glue as described in
the [unary guide](../../docs/unary.md#generate-services-from-protobuf). Keep
generated files outside Zeitwerk's managed paths. For example, use
`generated/rpc`, add that directory to `$LOAD_PATH`, and require `echo_connect`
from an initializer.

If placing generated files under `lib`, explicitly ignore their directory in
the application's Zeitwerk configuration before eager loading. Generated
filenames do not match the constants they define, generated glue requires its
message files, and it intentionally rejects `load`/redefinition. These files are
non-reloadable. Ordinary application controllers retain normal Rails reload
behavior.

### 2. Include the controller concern

For the repository's generated `Example::V1::EchoService` fixture:

```ruby
require "connectrpc/on_rails"
require "connectrpc/bindings/google_protobuf"
require "echo_connect"

class EchoController < ApplicationController
  include ConnectRPC::OnRails::Controller

  private

  def connect_service = Example::V1::EchoService::SERVICE
  def connect_binding = ConnectRPC::Bindings::GoogleProtobuf.new
  def connect_handlers
    Example::V1::EchoService.handlers(echo: method(:rpc_echo), fail: method(:rpc_fail))
  end
  def connect_limits = { request_limit: 1_048_576 }

  def rpc_echo(message, context)
    context.response_metadata.add("x-worker", "rails")
    context.response_trailers.add("x-finished", "yes")
    message
  end

  def rpc_fail(_message, _context)
    raise ConnectRPC::Error.new(:invalid_argument, "safe application message")
  end
end
```

### 3. Route the service

Use normal named controller routing, optionally under a literal scope:

```ruby
scope "/rpc" do
  match "/example.v1.EchoService(/*rpc)", to: "echo#connect", via: :all, format: false
end
```

`via: :all` lets core return the protocol's unsupported-verb response.
`format: false` preserves dots in service names. Dynamic scope segments are
not supported. Route/service mismatches raise a configuration error.

The adapter strips only the matched route's literal prefix. Unknown RPC names,
deeper suffixes, method case and raw escapes reach core unchanged after that
boundary. Rails itself normalizes duplicate/trailing slashes and escape case
before controller dispatch, so path acceptance differs from a direct Rack mount.

## Controller hooks

Only `connect` is an action supplied by the concern. Keep the three required
configuration hooks and typed handlers private, using prefixed handler names to
avoid controller/Kernel method collisions. Handlers return typed messages.
There is no render helper or routing DSL.

| Private hook | Required | Purpose |
| --- | --- | --- |
| `connect_service` | Yes | The generated `SERVICE` this controller serves |
| `connect_binding` | Yes | The message binding, such as `ConnectRPC::Bindings::GoogleProtobuf.new` |
| `connect_handlers` | Yes | The handler map, usually from the generated `handlers(...)` |
| `connect_limits` | No | Core limit and encoding keywords |
| `connect_instrumentation` | No | A `ConnectRPC::Instrumentation`, default nil; see [RPC observation](#rpc-observation) |
| `connect_interceptors` | No | Server interceptors, default `[]`; see [Interceptors](#interceptors) |
| `connect_protocol`, `connect_status_codec`, `connect_cors` | No | Enable unary gRPC-Web; see [gRPC-Web](#grpc-web) |

`connect_limits` accepts core `request_limit:`, `response_limit:`,
`request_wire_limit:`, `response_wire_limit:`, `get_query_limit:` and
`encoding:` keywords; omitted options use core's 4 MiB defaults.

Private `connect_request`, `connect_context`, and `connect_method` expose the
same typed objects to callbacks and handlers.

### Gzip

To enable unary gzip, return `encoding: :gzip` from the private `connect_limits`
hook. Core validates wire length, inflates under the decoded request limit, and
decodes before callbacks and native instrumentation. JSON scalar bodies still
bypass Rails body-parameter parsing. Negotiated success responses are compressed
after native execution; errors remain identity.

A compression/size failure after execution does not rerun callbacks, and native
process-action instrumentation can record 200 before core replaces the response
with its final error. Wire limits default to the corresponding message limits;
gzip framing overhead must fit too.

## Lifecycle and errors

Core negotiates, validates deadlines and bounds, reads, and decodes before
running native Rails callbacks. Authentication callbacks therefore run after
up to 4 MiB of input has been decoded by default. Preflight failures do not run
controller callbacks or emit `process_action.action_controller` events.

Callbacks, `rescue_from`, around-action cleanup and instrumentation use Rails'
real `process_action` chain. Native completion occurs before core serializes
the typed result. Configuration, handlers, server and context are evaluated
fresh for each request. Synchronize any application-owned shared state.

### Raise Connect errors

Raise `ConnectRPC::Error` to reject a request or map an application exception.
Its deliberate code, safe message, typed details, metadata and trailers survive.
If the application has a catch-all rescue, declare a reraising handler after
it because native Rails rescue precedence favors later declarations:

```ruby
rescue_from StandardError, with: :handle_application_error
rescue_from ConnectRPC::Error do |error|
  raise error
end
```

### How Rails outcomes map to Connect codes

Raw native render/head halts and exceptions covered by Rails'
`rescue_responses` use only the unambiguous inverse of Connect HTTP statuses:

| Rails status | Connect code |
| --- | --- |
| 401 | `unauthenticated` |
| 403 | `permission_denied` |
| 404 | `not_found` |
| 429 | `resource_exhausted` |
| 499 | `canceled` |
| 501 | `unimplemented` |
| 503 | `unavailable` |
| 504 | `deadline_exceeded` |

Messages are generic. Ambiguous 400/409/500, redirects, unmapped statuses, and
success without a typed result become `internal`. A non-200 native halt after
the handler also prevents typed success. Raw Rails bodies and exception
messages never become protocol error messages. An exception that escapes the
controller is normalized without a `cause`, and lifecycle observers receive the
original exception.

Exceptions explicitly registered in Rails' `rescue_responses` remain expected
outcomes and do not produce adapter error reports or exception logs. Unexpected
exceptions are logged with their original backtraces and reported through
`Rails.error` before safe core normalization. Request preparation uses the same
classification, including application overrides to `rescue_responses`.

### Limits of controller execution

Controller instrumentation describes controller execution: a deliberate Connect
exception can appear as a Rails 500, while successful controller execution can
precede a core encode or response-limit failure. Deadlines remain cooperative;
they cannot interrupt arbitrary application code or blocked synchronous reads.
Warden/Devise throws and failure-app responses remain host-owned non-Connect
responses unless the application explicitly adapts them.

## Interceptors

Override private `connect_interceptors` to run core
[server interceptors](../../docs/unary.md#interceptors) around every Connect
and gRPC-Web call of the controller. The hook defaults to `[]`. The controller
reads it for each request, and concurrent requests share the interceptor
objects, so return a frozen list built once and keep per-call state in the call
object:

```ruby
class EchoController < ApplicationController
  include ConnectRPC::OnRails::Controller

  INTERCEPTORS = [RequireTenant.new].freeze

  private

  def connect_interceptors = INTERCEPTORS
end
```

The Rails invoker is the innermost layer. Interceptors run after core decodes
the request and around Rails' whole `process_action` chain, so they see the
call before `before_action` and around-action callbacks, CSRF verification,
`rescue_from` and the `process_action.action_controller` event, and see the
result or error after `after_action` callbacks and around-action cleanup. The
first interceptor listed is the outermost.

- Rails policy still decides success. A server interceptor must yield; one that
  returns without yielding fails the call `internal` before any callback or
  handler runs. A successful response always means the full Rails chain
  succeeded, including CSRF and authentication callbacks.
- A Rails rejection, such as a `before_action` that renders 401, a CSRF failure
  or an `after_action` that sets 403, reaches interceptors as the mapped
  `ConnectRPC::Error` (see
  [How Rails outcomes map to Connect codes](#how-rails-outcomes-map-to-connect-codes)).
  An interceptor can translate it by raising another error. If it rescues the
  error and returns a message, core discards the message and the client still
  receives the rejection.
- An interceptor that raises before `yield` skips the callbacks, the handler,
  `rescue_from` and the Rails instrumentation event, like a core preflight
  failure. Interceptor exceptions do not reach `rescue_from`, `Rails.error` or
  the adapter's error log. A `ConnectRPC::Error` becomes the wire error, any
  other exception becomes `internal`, and lifecycle observers receive the
  original.
- An unexpected controller exception is still logged and reported once through
  `Rails.error`. Interceptors see the normalized `ConnectRPC::Error`
  (`internal`, "Internal error", no `cause`); lifecycle observers receive the
  original.
- Interceptors get the call's `Context`, the same object as `connect_context`,
  but no controller state such as `current_user`, the session or the Rails
  request. Keep policy that needs Rails state in callbacks.

The adapter serves unary calls only, so `server_stream` methods never run here.

## Request and security policy

During Connect execution, Rails receives an empty request-local indifferent body
parameter cache and an explicit JSON format. This prevents ParamsWrapper and
instrumentation from rereading ProtoJSON scalar/array/object roots or parsing
malformed Accept headers. Use typed `connect_request` for message fields;
query and route parameters remain accessible by string and symbol. Ordinary
non-Connect actions retain normal body parsing and format negotiation.

The adapter does not skip CSRF protection, disable sessions, or alter cookie
policy. A dedicated API controller suits token-authenticated RPC. Base
controllers use Rails' native safe-verb policy: GET does not require a CSRF
token; POST retains its configured token/session checks. The application must
explicitly route desired GET calls and preserve authentication/authorization. An
application may choose a narrowly scoped exemption according to its security
model; inclusion alone grants none.

Final core status, wire content type and emitted headers replace the interim
Rails representation. Stale content-length, content-encoding, ETag,
Last-Modified and Location are removed. Other host headers, default security
headers, CSP, authentication headers and cookies survive; repeated Rack 3
values remain arrays. Downstream Rails/Rack middleware retains its normal
ownership of request ids, ETags, content length and other final host behavior.

## gRPC-Web

Private `connect_protocol`, `connect_status_codec` and `connect_cors` hooks
enable binary/text unary gRPC-Web through the real controller invoker.
POST/OPTIONS routes are explicit; auth, callbacks, cookies, security headers and
Rails CSRF/origin controls remain active. See the
[Web guide](../../docs/grpc-web.md) for protocol, CORS, bounds and completion
rules.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional
[OpenTelemetry adapter](../connectrpc-otel/README.md). Native controllers
override private `connect_instrumentation` (default nil), separately from
`connect_limits`. Escaping original failures are captured before
reporting/normalization; successfully handled `rescue_from` exceptions are not
automatically failures. Configuration before Server construction is outside it.

## Static signatures

This package owns `sig/` and its discovery manifest. Add
`library "connectrpc-on-rails"` to a strict Steep target with this gem installed.
See the [RBS guide](../../docs/rbs.md) for supported APIs, extension contracts,
partial records, runtime validation and framework-signature limits.
`mise run rbs:check` checks application callers for this package; strict core
source checking is a separate target from backend implementation checking.

The finite upstream compatibility signatures in `rbs/action_controller_8` are
explicit opt-ins, separate from normal gem discovery. See the
[RBS guide](../../docs/rbs.md#opt-in-framework-compatibility) for tested
versions, supported framework forms and how to avoid duplicate
application-owned declarations.

## Tested combinations

The target supported Rails profile is 8.1. Rails 8.0 remains compatibility
evidence outside that profile. The tested combinations use Linux x64 and
Bundler 4.0.23:

| Rails | Ruby | Target profile status |
| --- | --- | --- |
| Newest 8.1.x | 3.4.12 and 4.0.9 | Supported framework profile |
| Newest 8.0.x | 3.4.12 | Compatibility evidence only |
| 8.0.5.1 (Action Pack floor) | 3.4.12 | Lowest-allowed evidence (`floors:rails`) |

These rows do not promise every framework/runtime combination. As checked on
2026-11-02, Rails'
[maintenance policy](https://rubyonrails.org/maintenance) lists security support
ending 2026-11-07 for 8.0 and 2027-10-10 for 8.1. The 8.0 compatibility lane
is not a promise to extend upstream maintenance.

## Development

From the repository root:

```sh
mise tasks
RAILS_LANE=8.0 mise run test:rails
RAILS_LANE=8.1 mise run verify:rails
MISE_RUBY_VERSION=4.0.9 RAILS_LANE=8.1 mise run test:rails
mise run generation:check:rails
mise run check
mise run verify
```

`setup:rails`, `test:rails`, `generation:check:rails`, `build:rails`,
`check:package:rails`, and `verify:rails` select `RAILS_LANE=8.1` by default.
Tests accept RSpec paths/options relative to this adapter directory.

Actual Appraisal 2.5.0 runs under the selected lane bundle, with no extra
base lock. `BUNDLE_COOLDOWN=3` applies to every adapter bundle process; the plain
Gemfile source is necessary because Appraisal cannot parse source keywords.
Pinned Bundler must exist before Appraisal can run, preventing its legacy
installer fallback. Never use `appraisal install` in CI. Generation is checked
in a temporary relative-layout copy, without rewriting tracked files.

`Appraisals` admits any `~> 8.0.0` or `~> 8.1.0` release, and each lane resolves
an ignored local lane lock with the newest allowed versions. `floors:rails`
instead runs specs and package checks directly under an ignored wrapper of the
8.0 lane Gemfile, with Action Pack and Rack at their gemspec floors. Runtime cache
uses root `vendor/bundle` by ABI; adapter artifacts and coverage live under
`vendor/rails`. SimpleCov starts before adapter requires and includes every
adapter library file, independently of core coverage.

`check:package:rails` builds all three core/Rack/Rails archives, checks exact
manifests and metadata, and installs them with conventional offline dependency
resolution into fresh GEM_HOME/GEM_PATH outside the checkout. Installed
generated clients perform binary/JSON success and structured error calls
through a real Rails application and Rack::Lint, loading integration code only
from the installed archive. See [development](../../docs/development.md) for the
separate core protocol, generation, Go interoperability and official
conformance evidence.

## License

Original project work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg.
Both gem archives include the complete license and pin `allowed_push_host` to
`https://rubygems.org`; only the [release workflow](../../docs/releasing.md)
publishes them. Verification authorizes no publication.
