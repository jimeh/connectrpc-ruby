# gRPC status descriptors

`google/rpc/status.proto` is the unchanged Google APIs schema at commit
`ebd1d23ac613b177828dad42ad8dfb13ba498279`, SHA256
`f5bfd262e6705c7ae73f32e0ad8ee20ce8c0a2578df8c4f76ebf76b572f295ed`.
Its Apache 2.0 license is in `google/LICENSE`; the schema retains its copyright
notice. Source: https://github.com/googleapis/googleapis/blob/ebd1d23ac613b177828dad42ad8dfb13ba498279/google/rpc/status.proto

The private descriptor includes canonical `google/protobuf/any.proto` from
protoc 36.2. Its BSD 3-Clause license is in `protobuf-LICENSE`.
Source: https://github.com/protocolbuffers/protobuf/tree/v36.2

`mise run grpc:status:build` produces the descriptor from those compiler inputs.
`mise run grpc:status:check` checks byte parity and its pinned SHA256. No generated
Google RPC constants or global descriptor registration are introduced.
Original ConnectRPC work remains MIT licensed, copyright 2026 Jim Myhrberg.
