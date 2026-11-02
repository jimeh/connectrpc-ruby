// The peers share spec/fixtures/echo.bin, an explicitly built FileDescriptorProto.
// This executable uses the reference connect Client and Handler, not wire stubs.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const prefix = "/example.v1.EchoService/"

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, what string) {
	if !ok {
		panic(what)
	}
}

func schema() protoreflect.ServiceDescriptor {
	bytes, err := os.ReadFile("../spec/fixtures/echo.bin")
	must(err)
	file := new(descriptorpb.FileDescriptorProto)
	must(proto.Unmarshal(bytes, file))
	desc, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	must(err)
	must(protoregistry.GlobalFiles.RegisterFile(desc))
	msg := desc.Messages().ByName("EchoMessage")
	must(protoregistry.GlobalTypes.RegisterMessage(dynamicpb.NewMessageType(msg)))
	check(msg.Fields().ByName("name").HasOptionalKeyword(), "fixture optional presence")
	check(msg.Fields().ByName("count").Kind() == protoreflect.Int64Kind, "fixture int64")
	check(msg.Fields().ByName("at").Message().FullName() == "google.protobuf.Timestamp", "fixture WKT")
	check(msg.Enums().ByName("Mood").Values().ByName("HAPPY").Number() == 1, "fixture enum")
	return desc.Services().ByName("EchoService")
}

func initializer(spec connect.Spec, message any) error {
	desc := spec.Schema.(protoreflect.MethodDescriptor)
	target := desc.Input()
	if spec.IsClient {
		target = desc.Output()
	}
	*message.(*dynamicpb.Message) = *dynamicpb.NewMessage(target)
	return nil
}

func message(desc protoreflect.MessageDescriptor) *dynamicpb.Message {
	msg := dynamicpb.NewMessage(desc)
	fields := desc.Fields()
	msg.Set(fields.ByName("name"), protoreflect.ValueOfString(""))
	msg.Set(fields.ByName("count"), protoreflect.ValueOfInt64(9007199254740993))
	msg.Set(fields.ByName("mood"), protoreflect.ValueOfEnum(1))
	msg.Set(fields.ByName("at"), protoreflect.ValueOfMessage(timestamppb.New(time.Unix(1704067200, 0)).ProtoReflect()))
	msg.Set(fields.ByName("data"), protoreflect.ValueOfBytes([]byte{0, 255}))
	return msg
}

func serve(service protoreflect.ServiceDescriptor, gzip bool) {
	mux := http.NewServeMux()
	for _, name := range []protoreflect.Name{"Echo", "Fail"} {
		desc := service.Methods().ByName(name)
		callback := func(ctx context.Context, request *connect.Request[dynamicpb.Message]) (*connect.Response[dynamicpb.Message], error) {
			if name == "Fail" {
				detail, err := connect.NewErrorDetail(request.Msg)
				must(err)
				failure := connect.NewError(connect.CodeInvalidArgument, errors.New("deliberate"))
				failure.AddDetail(detail)
				failure.Meta().Add("X-Error", "a")
				failure.Meta().Add("X-Error", "b")
				info, ok := connect.CallInfoForHandlerContext(ctx)
				check(ok, "missing handler context")
				info.ResponseTrailer().Add("X-End", "c")
				info.ResponseTrailer().Add("X-End", "d")
				return nil, failure
			}
			response := connect.NewResponse(request.Msg)
			for _, entry := range request.Header().Values("X-Request") {
				response.Header().Add("X-Echo", entry)
			}
			response.Header().Add("X-Repeat", "one")
			response.Header().Add("X-Repeat", "two")
			response.Trailer().Add("X-Tail", "three")
			response.Trailer().Add("X-Tail", "four")
			return response, nil
		}
		options := []connect.HandlerOption{connect.WithSchema(desc), connect.WithRequestInitializer(initializer), connect.WithReadMaxBytes(1024)}
		if gzip {
			options = append(options, connect.WithCompressMinBytes(0))
		}
		mux.Handle(prefix+string(name), connect.NewUnaryHandler(prefix+string(name), callback, options...))
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Println("http://" + listener.Addr().String())
	// Parent closes stdin after reference and Ruby client assertions.
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	must(server.Shutdown(ctx))
	err = <-done
	check(errors.Is(err, http.ErrServerClosed), "Go server failed")
}

type metadataExpectations struct {
	echo, repeat, tail, errorHeader, errorTrailer []string
}

func client(service protoreflect.ServiceDescriptor, url, target string) {
	// These are expectations for known fixture fields, not general HTTP header
	// normalization. The Go host preserves raw fields; WEBrick coalesces them.
	profiles := map[string]metadataExpectations{
		"Go":   {[]string{"a", "b"}, []string{"one", "two"}, []string{"three", "four"}, []string{"a", "b"}, []string{"c", "d"}},
		"Ruby": {[]string{"a, b"}, []string{"one, two"}, []string{"three, four"}, []string{"a, b"}, []string{"c, d"}},
	}
	expected, ok := profiles[target]
	check(ok, "unknown reference client target: "+target)
	httpTransport := &http.Transport{Proxy: nil, DisableCompression: true}
	defer httpTransport.CloseIdleConnections()
	httpClient := &http.Client{Transport: httpTransport, Timeout: 5 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	for _, codec := range []string{"proto", "json"} {
		label := "Go->" + target + " " + codec
		options := []connect.ClientOption{connect.WithResponseInitializer(initializer)}
		if codec == "json" {
			options = append(options, connect.WithProtoJSON())
		}
		echoDesc := service.Methods().ByName("Echo")
		echoOptions := append(append([]connect.ClientOption{}, options...), connect.WithSchema(echoDesc))
		echo := connect.NewClient[dynamicpb.Message, dynamicpb.Message](httpClient, url+prefix+"Echo", echoOptions...)
		msg := message(echoDesc.Input())
		request := connect.NewRequest(msg)
		request.Header().Set("Accept-Encoding", "identity")
		request.Header().Add("X-Request", "a")
		request.Header().Add("X-Request", "b")
		response, err := echo.CallUnary(context.Background(), request)
		must(err)
		check(proto.Equal(response.Msg, msg), label+" typed message")
		check(response.Msg.Has(echoDesc.Output().Fields().ByName("name")), label+" optional presence")
		check(reflect.DeepEqual(response.Header().Values("X-Echo"), expected.echo), label+" request metadata")
		check(reflect.DeepEqual(response.Header().Values("X-Repeat"), expected.repeat), label+" response metadata")
		check(reflect.DeepEqual(response.Trailer().Values("X-Tail"), expected.tail), label+" response trailers")
		fmt.Println("PASS " + label + " success/presence/int64/enum/WKT/bytes/metadata/trailers")

		failDesc := service.Methods().ByName("Fail")
		failOptions := append(append([]connect.ClientOption{}, options...), connect.WithSchema(failDesc))
		failure := connect.NewClient[dynamicpb.Message, dynamicpb.Message](httpClient, url+prefix+"Fail", failOptions...)
		_, err = failure.CallUnary(context.Background(), connect.NewRequest(msg))
		var rpcError *connect.Error
		check(errors.As(err, &rpcError), label+" error type")
		check(rpcError.Code() == connect.CodeInvalidArgument && rpcError.Message() == "deliberate", label+" error code/message")
		check(len(rpcError.Details()) == 1, label+" detail count")
		decoded, err := rpcError.Details()[0].Value()
		must(err)
		check(proto.Equal(decoded, msg), label+" typed detail")
		check(reflect.DeepEqual(rpcError.Meta().Values("X-Error"), expected.errorHeader), label+" error leading metadata")
		check(reflect.DeepEqual(rpcError.Meta().Values("X-End"), expected.errorTrailer), label+" error trailing metadata")
		fmt.Println("PASS " + label + " deliberate error/detail/metadata/trailers")

		msg.Set(echoDesc.Input().Fields().ByName("name"), protoreflect.ValueOfString(string(make([]byte, 2048))))
		_, err = echo.CallUnary(context.Background(), connect.NewRequest(msg))
		check(connect.CodeOf(err) == connect.CodeResourceExhausted, label+" request limit")
		fmt.Println("PASS " + label + " request limit")
	}
}

func main() {
	service := schema()
	switch os.Args[1] {
	case "serve":
		serve(service, false)
	case "serve-gzip":
		serve(service, true)
	case "client-gzip":
		gzipClient(service, os.Args[2], os.Args[3])
	case "client":
		client(service, os.Args[2], os.Args[3])
	default:
		panic("serve|client")
	}
}
