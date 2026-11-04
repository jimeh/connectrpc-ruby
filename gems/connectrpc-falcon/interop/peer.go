// The peers share spec/fixtures/echo.bin, an explicitly built FileDescriptorProto.
// This executable uses the reference connect Client and Handler, not wire stubs.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
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

func serve(service protoreflect.ServiceDescriptor) {
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
		mux.Handle(prefix+string(name), connect.NewUnaryHandler(prefix+string(name), callback,
			connect.WithSchema(desc), connect.WithRequestInitializer(initializer), connect.WithReadMaxBytes(1024)))
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		check(r.ProtoMajor == 2, "Go server did not receive HTTP/2")
		if r.TLS != nil {
			check(r.TLS.NegotiatedProtocol == "h2", "Go server ALPN")
		}
		mux.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: handler, Protocols: protocols, ReadHeaderTimeout: 5 * time.Second}
	if os.Args[2] != "h2c" {
		server.TLSConfig = tlsConfig(os.Args[3], os.Args[2] == "mtls", true)
	}
	done := make(chan error, 1)
	scheme := "http"
	go func() {
		if server.TLSConfig == nil {
			done <- server.Serve(listener)
		} else {
			done <- server.ServeTLS(listener, "", "")
		}
	}()
	if server.TLSConfig != nil {
		scheme = "https"
	}
	fmt.Println(scheme + "://" + listener.Addr().String())
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
		"Falcon": {[]string{"a", "b"}, []string{"one", "two"}, []string{"three", "four"}, []string{"a", "b"}, []string{"c", "d"}},
		"Go":     {[]string{"a", "b"}, []string{"one", "two"}, []string{"three", "four"}, []string{"a", "b"}, []string{"c", "d"}},
		"Ruby":   {[]string{"a, b"}, []string{"one, two"}, []string{"three, four"}, []string{"a, b"}, []string{"c, d"}},
	}
	expected, ok := profiles[target]
	check(ok, "unknown reference client target: "+target)
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	httpTransport := &http.Transport{Proxy: nil, DisableCompression: true, Protocols: protocols}
	if strings.HasPrefix(url, "https:") {
		httpTransport.TLSClientConfig = tlsConfig(os.Args[4], os.Args[5] == "mtls", false)
	}
	defer httpTransport.CloseIdleConnections()
	httpClient := &http.Client{Transport: checkedTransport{httpTransport}, Timeout: 5 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	for _, codec := range []string{"proto", "json"} {
		label := "Go->" + target + " " + codec
		options := []connect.ClientOption{connect.WithResponseInitializer(initializer)}
		if os.Getenv("CONNECTRPC_FALCON_GZIP") != "" {
			options = append(options, connect.WithSendGzip(), connect.WithCompressMinBytes(0))
		}
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
		serve(service)
	case "client":
		client(service, os.Args[2], os.Args[3])
	case "web-head":
		webRejection(service, os.Args[2], os.Args[3])
	case "tail":
		tail(service, os.Args[2], os.Args[3], os.Args[4])
	default:
		panic("serve|client")
	}
}

// The unchanged typed fixture assertions above are independent connect-go
// calls. This wrapper also proves the actual negotiated wire version.
type checkedTransport struct{ transport *http.Transport }

func (c checkedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if os.Args[1] == "client" && os.Getenv("CONNECTRPC_FALCON_GZIP") != "" {
		check(r.Header.Get("Content-Encoding") == "gzip", "Go client actual gzip request")
	}
	response, err := c.transport.RoundTrip(r)
	if err == nil {
		check(response.ProtoMajor == 2, "Go client HTTP/2")
		if os.Args[1] == "client" && os.Getenv("CONNECTRPC_FALCON_GZIP") != "" && response.StatusCode == http.StatusOK {
			check(response.Header.Get("Content-Encoding") == "gzip", "Falcon actual gzip response")
		}
		if r.URL.Scheme == "https" {
			check(response.TLS != nil, "Go client TLS state")
			check(response.TLS.NegotiatedProtocol == "h2", "Go client ALPN")
		}
	}
	return response, err
}
func tlsConfig(directory string, mutual, server bool) *tls.Config {
	ca, err := os.ReadFile(directory + "/ca.pem")
	must(err)
	roots := x509.NewCertPool()
	check(roots.AppendCertsFromPEM(ca), "CA PEM")
	config := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}, RootCAs: roots}
	if server || mutual {
		name := "client"
		if server {
			name = "server"
		}
		certificate, err := tls.LoadX509KeyPair(directory+"/"+name+".pem", directory+"/"+name+".key")
		must(err)
		config.Certificates = []tls.Certificate{certificate}
	}
	if server && mutual {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = roots
	}
	return config
}

// Observe decrypted frame boundaries, without participating in HTTP/2 or HPACK.
// TLS wrapping implements the Go 1.27 connectionStater interface.
type observedConn struct {
	net.Conn
	state   tls.ConnectionState
	pending []byte
	once    sync.Once
	goaway  chan struct{}
}

func (c *observedConn) ConnectionState() tls.ConnectionState { return c.state }
func (c *observedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.pending = append(c.pending, p[:n]...)
	for len(c.pending) >= 9 {
		size := int(c.pending[0])<<16 | int(c.pending[1])<<8 | int(c.pending[2])
		if len(c.pending) < 9+size {
			break
		}
		if c.pending[3] == 7 && size >= 8 {
			code := c.pending[13:17]
			check(code[0]|code[1]|code[2]|code[3] == 0, "nonzero GOAWAY")
			c.once.Do(func() { close(c.goaway) })
		}
		c.pending = c.pending[9+size:]
	}
	return n, err
}
func tail(service protoreflect.ServiceDescriptor, url, codec, directory string) {
	done := make(chan struct{})
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: protocols, Proxy: nil, DisableCompression: true}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		observer := &observedConn{Conn: conn, goaway: done}
		if strings.HasPrefix(url, "https:") {
			config := tlsConfig(directory, false, false)
			config.ServerName = "localhost"
			ssl := tls.Client(conn, config)
			if err := ssl.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			observer.Conn = ssl
			observer.state = ssl.ConnectionState()
			check(observer.state.NegotiatedProtocol == "h2", "tail ALPN")
		}
		return observer, nil
	}
	transport.DialContext = dial
	transport.DialTLSContext = dial
	defer transport.CloseIdleConnections()
	method := service.Methods().ByName("Echo")
	options := []connect.ClientOption{connect.WithSchema(method), connect.WithResponseInitializer(initializer)}
	if codec == "json" {
		options = append(options, connect.WithProtoJSON())
	}
	client := connect.NewClient[dynamicpb.Message, dynamicpb.Message](&http.Client{Transport: checkedTransport{transport}, Timeout: 5 * time.Second}, url+prefix+"Echo", options...)
	message := dynamicpb.NewMessage(method.Input())
	message.Set(method.Input().Fields().ByName("name"), protoreflect.ValueOfString(strings.Repeat("x", 200000)))
	request := connect.NewRequest(message)
	request.Header().Set("Accept-Encoding", "identity")
	response, err := client.CallUnary(context.Background(), request)
	must(err)
	check(proto.Equal(message, response.Msg), "typed response tail truncated")
	fmt.Println("PASS Go tail " + codec + " bytes=200000 typed response HTTP/2")
	select {
	case <-done:
		fmt.Println("PASS Go clean GOAWAY")
	case <-time.After(5 * time.Second):
		panic("GOAWAY absent")
	}
	// The peer closes after GOAWAY. The host must see EOF before returning true.
}
