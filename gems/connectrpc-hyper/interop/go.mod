module github.com/jimeh/connectrpc-ruby/gems/connectrpc-hyper/interop

go 1.27.3

require (
	connectrpc.com/connect v1.21.1
	github.com/jimeh/connectrpc-ruby/interop/grpc v0.0.0
	golang.org/x/net v0.60.0
	google.golang.org/grpc v1.85.0
)

require (
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260817212433-ac3dfec99bb1 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/jimeh/connectrpc-ruby/interop/grpc => ../../../interop/grpc
