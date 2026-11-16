# gRPC health schema

`grpc/health/v1/health.proto` is the unchanged canonical gRPC schema from
[grpc/grpc-proto](https://github.com/grpc/grpc-proto) at commit
`2eb777aba6593c31e21f7f69a163486bdc793501`, SHA256
`164b670058855c6277a0511a14e2a302dd5a8ff899853ca6851b6b201a413d28`.
Source: https://github.com/grpc/grpc-proto/blob/2eb777aba6593c31e21f7f69a163486bdc793501/grpc/health/v1/health.proto

The schema is Apache 2.0 licensed and retains its copyright notice. The license
text from the same commit is in `LICENSE`. The generated `lib/grpc/health/v1/`
Ruby files and `rbs/grpc_health_v1/grpc/health/v1/` signatures are derived from
this schema.

`mise run generation:build` produces them with pinned protoc 36.2 and
`protoc-gen-connectrpc-ruby`. `mise run generation:check` checks byte parity,
the exact generated file sets and this schema's pinned SHA256. Original
ConnectRPC work remains MIT licensed, copyright 2026 Jim Myhrberg.
