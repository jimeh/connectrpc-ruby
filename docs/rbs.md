# Optional RBS

The gem ships core signatures and can generate unary service signatures. RBS
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

The mutable Response, TransportRequest and TransportResponse signatures cover
fully initialized keyword constructors only. Supply every member, including
Metadata objects and an explicit nil deadline where applicable. Ordinary Ruby
Struct construction also permits omitted members that default to nil; that
construction is outside the precise typed subset. Generic covariance and broad
Struct indexing are intentionally absent.

## Repository checks

```sh
mise run rbs:setup
mise run rbs:check
mise run rbs:package
MISE_RUBY_VERSION=4.0.9 mise run rbs:check
MISE_RUBY_VERSION=4.0.9 mise run rbs:package
```

The independent checksummed `typecheck/Gemfile.lock` pins only development tools.
Bundles, Bundler state, compiler caches and local evidence live under
`vendor/rbs/`. The package check installs into a fresh GEM_HOME and exercises
normal installed signature/executable discovery outside the checkout. It clears
checkout Ruby and Bundler load paths. Both Ruby lanes run these feature gates in
CI; core, Rails and Async bundles remain independent.
