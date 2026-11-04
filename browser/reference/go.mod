module github.com/jimeh/connectrpc-ruby/browser/reference

go 1.27.3

require (
	connectrpc.com/connect v1.21.1
	github.com/jimeh/connectrpc-ruby/interop/grpc v0.0.0
	google.golang.org/grpc v1.85.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.6.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cenkalti/backoff/v4 v4.1.1 // indirect
	github.com/desertbit/timer v0.0.0-20180107155436-c41aec40b27f // indirect
	github.com/improbable-eng/grpc-web v0.15.0 // indirect
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/rs/cors v1.11.1 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260817212433-ac3dfec99bb1 // indirect
	nhooyr.io/websocket v1.8.6 // indirect
)

replace github.com/jimeh/connectrpc-ruby/interop/grpc => ../../interop/grpc
