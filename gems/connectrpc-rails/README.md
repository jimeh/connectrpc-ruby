# connectrpc-rails

Optional native Rails controller execution for the experimental, unofficial
ConnectRPC unary Ruby gem. The core gem remains independent of Rails and has
zero runtime dependencies. This adapter reuses its Server invoker and the
optional [connectrpc-rack](../connectrpc-rack/README.md) translation. It does not add another protocol implementation.

## Use a controller

Add `connectrpc-rails` and your chosen protobuf binding dependency to your
application Gemfile. Runtime dependencies are `connectrpc ~> 0.1.0`,
`connectrpc-rack ~> 0.1.0`, and `actionpack >= 8.0.5.1, < 8.2`.
The Rack package supplies the `rack >= 3.2.7, < 4` requirement. The application
chooses its controller base, binding, authentication, and security policy.
Requiring `connectrpc/rails` loads neither `ActionController::Base` nor
`ActionController::API`, and does not load Google protobuf.

Generate official protobuf messages and Connect service glue as described in
the [root README](../../README.md). Keep generated files outside Zeitwerk's
managed paths. For example, use `generated/rpc`, add that directory to
`$LOAD_PATH`, and require `echo_connect` from an initializer. If placing generated
files under `lib`, explicitly ignore their directory in the application's
Zeitwerk configuration before eager loading. Generated filenames do not match
the constants they define, generated glue requires its message files, and it
intentionally rejects `load`/redefinition. These files are non-reloadable.
Ordinary application controllers retain normal Rails reload behavior.

For the repository's generated `Example::V1::EchoService` fixture:

```ruby
require "connectrpc/rails"
require "connectrpc/bindings/google_protobuf"
require "echo_connect"

class EchoController < ApplicationController
  include ConnectRPC::Rails::Controller

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

Only `connect` is an action supplied by the concern. Keep the three required
configuration hooks and typed handlers private, using prefixed handler names to
avoid controller/Kernel method collisions. `connect_limits` is optional and
accepts core `request_limit:`, `response_limit:`, `request_wire_limit:`,
`response_wire_limit:` and `encoding:` keywords; omitted options
use core's 4 MiB defaults. Private `connect_request`, `connect_context`, and
`connect_method` expose the same typed objects to callbacks and handlers.
Handlers return typed messages. There is no render helper or routing DSL.

## Lifecycle and errors

Core negotiates, validates deadlines and bounds, reads, and decodes before
running native Rails callbacks. Authentication callbacks therefore run after
up to 4 MiB of input has been decoded by default. Preflight failures do not run
controller callbacks or emit `process_action.action_controller` events.
Callbacks, `rescue_from`, around-action cleanup and instrumentation use Rails'
real `process_action` chain. Native completion occurs before core serializes
the typed result. Configuration, handlers, server and context are evaluated
fresh for each request. Synchronize any application-owned shared state.

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

Raw native render/head halts and exceptions covered by Rails'
`rescue_responses` use only the unambiguous inverse of Connect HTTP statuses:
401/403/404/429/499/501/503/504 become unauthenticated/permission_denied/
not_found/resource_exhausted/canceled/unimplemented/unavailable/
deadline_exceeded. Messages are generic. Ambiguous 400/409/500, redirects,
unmapped statuses, and success without a typed result become `internal`.
A non-200 native halt after the handler also prevents typed success. Raw Rails
bodies and exception messages never become protocol error messages.
Exceptions explicitly registered in Rails' `rescue_responses` remain expected
outcomes and do not produce adapter error reports or exception logs. Unexpected
exceptions are logged with their original backtraces and reported through
`Rails.error` before safe core normalization. Request preparation uses the same
classification, including application overrides to `rescue_responses`.

Controller instrumentation describes controller execution: a deliberate Connect
exception can appear as a Rails 500, while successful controller execution can
precede a core encode or response-limit failure. Deadlines remain cooperative;
they cannot interrupt arbitrary application code or blocked synchronous reads.
Warden/Devise throws and failure-app responses remain host-owned non-Connect
responses unless the application explicitly adapts them.

## Request and security policy

During Connect execution, Rails receives an empty request-local indifferent body
parameter cache and an explicit JSON format. This prevents ParamsWrapper and
instrumentation from rereading ProtoJSON scalar/array/object roots or parsing
malformed Accept headers. Use typed `connect_request` for message fields;
query and route parameters remain accessible by string and symbol. Ordinary
non-Connect actions retain normal body parsing and format negotiation.

The adapter does not skip CSRF protection, disable sessions, or alter cookie
policy. A dedicated API controller suits token-authenticated RPC. Base
controllers with forgery protection still require a valid token/session. An
application may choose a narrowly scoped exemption according to its security
model; inclusion alone grants none.

Final core status, wire content type and emitted headers replace the interim
Rails representation. Stale content-length, content-encoding, ETag,
Last-Modified and Location are removed. Other host headers, default security
headers, CSP, authentication headers and cookies survive; repeated Rack 3
values remain arrays. Downstream Rails/Rack middleware retains its normal
ownership of request ids, ETags, content length and other final host behavior.

## Verification and tested combinations

The tested Linux x64 combinations are Rails 8.0.5.1 on Ruby 3.4.12, and Rails
8.1.4 on Ruby 3.4.12 and 4.0.9, with Bundler 4.0.23. This is not a full
cross-product support claim. As checked on 2026-11-02, Rails'
[maintenance policy](https://rubyonrails.org/maintenance) lists security support
ending 2026-11-07 for 8.0 and 2027-10-10 for 8.1. The 8.0 compatibility lane
is not a promise to extend upstream maintenance.

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
Actual Appraisal 2.5.0 runs under the selected frozen lane bundle, with no extra
base lock. `BUNDLE_COOLDOWN=7` applies to every adapter bundle process; the plain
Gemfile source is necessary because Appraisal cannot parse source keywords.
Pinned Bundler must exist before Appraisal can run, preventing its legacy
installer fallback. Never use `appraisal install` in CI. Generation is checked
in a temporary relative-layout copy, without rewriting tracked files.

Both lane locks include checksums and `ruby`/`x86_64-linux` platforms. Native
variants recorded by Bundler remain checksum-locked. New resolved artifacts
require a primary registry release-age and archive SHA256 audit. Runtime cache
uses root `vendor/bundle` by ABI; adapter artifacts and coverage live under
`vendor/rails`. SimpleCov starts before adapter requires and includes every
adapter library file, independently of core coverage.

`check:package:rails` builds all three core/Rack/Rails archives, checks exact manifests and metadata,
and installs them with conventional offline dependency resolution into fresh
GEM_HOME/GEM_PATH outside the checkout. Installed generated clients perform
binary/JSON success and structured error calls through a real Rails application
and Rack::Lint, loading integration code only from the installed archive.
See [development](../../docs/development.md) for the separate core protocol,
generation, Go interoperability and official conformance evidence.

## Status and licensing

Experimental and not production ready. Streaming, GET, gRPC/gRPC-Web and production hosting are outside this adapter.
Original project work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg.
Both gem archives include the complete license and retain the
`https://gems.invalid` push guard. Publishing, releasing or deploying requires
separate, explicit authorization. Verification authorizes no publication.


To enable unary gzip, return `encoding: :gzip` from the private `connect_limits`
hook. Core validates wire length, inflates under the decoded request limit, and
decodes before callbacks and native instrumentation. JSON scalar bodies still
bypass Rails body-parameter parsing. Negotiated success responses are compressed
after native execution; errors remain identity. A compression/size failure after
execution does not rerun callbacks, and native process-action instrumentation can
record 200 before core replaces the response with its final error. Wire limits
default to the corresponding message limits; gzip framing overhead must fit too.
