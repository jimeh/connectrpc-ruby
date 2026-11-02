# connectrpc-ruby

A private, unofficial, experimental Ruby gem skeleton for future
[Connect RPC](https://connectrpc.com/) clients and servers. It currently exposes
only `ConnectRPC::VERSION`. There is no RPC implementation, and it is not ready
for production.

## Development

Install [Mise](https://mise.jdx.dev/) 2026.9.14 or newer and the
[Ruby source-build prerequisites](https://github.com/rbenv/ruby-build/wiki#suggested-build-environment),
then run:

```sh
mise run setup
mise exec -- bundle exec ruby -rconnectrpc -e 'puts ConnectRPC::VERSION'
# 0.1.0
mise run verify
```

CRuby >= 3.4 is the intended baseline. The tested versions are 3.4.12 (local
default) and 4.0.9. This is a project support choice; Ruby 3.3 remains in upstream
security maintenance.

See [development](docs/development.md) for commands and coverage reports, and
[design](docs/design.md) for accepted future boundaries and the proposed first
product slice.

## Private status and licensing

Licensing is undecided. `Nonstandard` gem metadata grants no new license.
The gem's `allowed_push_host` is `https://gems.invalid`, a reserved invalid
domain, to guard against accidental public RubyGems pushes. It is not a
publication destination or a complete access-control mechanism. Do not publish
this gem without an explicit licensing and publication decision.
