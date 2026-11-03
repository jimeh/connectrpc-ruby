package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func schemaIdempotency(desc protoreflect.MethodDescriptor) connect.IdempotencyLevel {
	return connect.IdempotencyLevel(desc.Options().(*descriptorpb.MethodOptions).GetIdempotencyLevel())
}

// Observe the reference client's real selected verb and query independently of
// Ruby's parser. Never rewrite a request, payload, header or response.
type getTransport struct {
	base     http.RoundTripper
	method   string
	codec    string
	encoding string
	desc     protoreflect.MessageDescriptor
}

func (transport *getTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.method = request.Method
	if request.Method == "GET" {
		check(request.Body == nil || request.Body == http.NoBody, "GET has native body")
		for _, key := range []string{"Content-Type", "Content-Encoding", "Content-Length", "Connect-Protocol-Version"} {
			check(request.Header.Get(key) == "", "GET synthetic "+key)
		}
		query := request.URL.Query()
		check(query.Get("encoding") == transport.codec, "GET query codec")
		payload := []byte(query.Get("message"))
		if query.Get("base64") == "1" {
			var err error
			payload, err = base64.RawURLEncoding.DecodeString(strings.TrimRight(string(payload), "="))
			must(err)
		}
		wireSize := len(payload)
		if transport.encoding == "gzip" {
			check(query.Get("compression") == "gzip", "GET gzip query")
			reader, err := gzip.NewReader(bytes.NewReader(payload))
			must(err)
			payload, err = io.ReadAll(reader)
			must(err)
			must(reader.Close())
		}
		decoded := dynamicpb.NewMessage(transport.desc)
		if transport.codec == "proto" {
			must(proto.Unmarshal(payload, decoded))
		} else {
			must(protojson.Unmarshal(payload, decoded))
		}
		observed, err := json.Marshal(map[string]any{"http_method": request.Method, "codec": transport.codec,
			"compression": transport.encoding, "query_payload_bytes": wireSize, "http_body_bytes": 0})
		must(err)
		fmt.Println("GET_WIRE " + string(observed))
	}
	return transport.base.RoundTrip(request)
}

func getClient(service protoreflect.ServiceDescriptor, url, target string) {
	base := &http.Transport{Proxy: nil, DisableCompression: true}
	defer base.CloseIdleConnections()
	desc := service.Methods().ByName("Echo")
	observed := &getTransport{base: base, desc: desc.Input()}
	httpClient := &http.Client{Transport: observed, Timeout: 5 * time.Second}
	for _, codec := range []string{"proto", "json"} {
		for _, encoding := range []string{"identity", "gzip"} {
			observed.codec, observed.encoding = codec, encoding
			options := []connect.ClientOption{connect.WithSchema(desc), connect.WithResponseInitializer(initializer),
				connect.WithHTTPGet(), connect.WithIdempotency(schemaIdempotency(desc)), connect.WithCompressMinBytes(0)}
			if codec == "json" {
				options = append(options, connect.WithProtoJSON())
			}
			if encoding == "gzip" {
				// The reference GET client compresses only when needed to fit its URL cap.
				options = append(options, connect.WithSendGzip(), connect.WithHTTPGetMaxURLSize(384, false))
			}
			stub := connect.NewClient[dynamicpb.Message, dynamicpb.Message](httpClient, url+prefix+"Echo", options...)
			msg := message(desc.Input())
			msg.Set(desc.Input().Fields().ByName("name"), protoreflect.ValueOfString(strings.Repeat("read ", 40)))
			request := connect.NewRequest(msg)
			request.Header().Set("X-Get-Probe", "yes")
			request.Header().Set("X-Request", "get")
			result, err := stub.CallUnary(context.Background(), request)
			must(err)
			check(observed.method == "GET" && request.HTTPMethod() == "GET", "reference client failed to select GET")
			check(proto.Equal(result.Msg, msg), "GET message mismatch")
			check(result.Header().Get("X-Observed-HTTP-Method") == "GET", "peer did not observe GET")
			check(result.Header().Get("X-Echo") == "get" && result.Trailer().Get("X-Tail") != "", "GET metadata/trailers")
			fmt.Printf("PASS GET Go->%s %s/%s success/actual-verb/payload/metadata/trailers\n", target, codec, encoding)
			request = connect.NewRequest(msg)
			request.Header().Set("X-Get-Probe", "yes")
			request.Header().Set("X-Get-Fail", "yes")
			_, err = stub.CallUnary(context.Background(), request)
			check(connect.CodeOf(err) == connect.CodeInvalidArgument, "GET error code")
			failure := err.(*connect.Error)
			check(failure.Message() == "deliberate" && len(failure.Details()) == 1, "GET typed error")
			decoded, err := failure.Details()[0].Value()
			must(err)
			check(proto.Equal(decoded, msg), "GET error detail")
			check(failure.Meta().Get("X-Error") != "" && failure.Meta().Get("X-End") != "", "GET error metadata/trailers")
			check(observed.method == "GET", "GET error method")
			fmt.Printf("PASS GET Go->%s %s/%s error/details/metadata/trailers\n", target, codec, encoding)
		}
	}
	// Unknown safety still uses POST even with the reference GET option enabled.
	unsafeDesc := service.Methods().ByName("Fail")
	unsafe := connect.NewClient[dynamicpb.Message, dynamicpb.Message](httpClient, url+prefix+"Fail",
		connect.WithSchema(unsafeDesc), connect.WithResponseInitializer(initializer), connect.WithHTTPGet())
	_, err := unsafe.CallUnary(context.Background(), connect.NewRequest(message(desc.Input())))
	check(connect.CodeOf(err) == connect.CodeInvalidArgument && observed.method == "POST", "unsafe GET selection")
	fmt.Printf("PASS GET Go->%s unsafe remains POST\n", target)
}
