package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Observe and restore the exact HTTP bodies around the independent reference
// client. No coding, payload, headers or result is repaired by this fixture.
type wireObservation struct {
	RequestAccept    string `json:"request_accept"`
	Label            string `json:"label"`
	RequestEncoding  string `json:"request_encoding"`
	RequestBytes     int    `json:"request_bytes"`
	RequestGzip      bool   `json:"request_gzip"`
	ResponseEncoding string `json:"response_encoding"`
	ResponseBytes    int    `json:"response_bytes"`
	ResponseGzip     bool   `json:"response_gzip"`
	Status           int    `json:"status"`
}
type observedTransport struct {
	base  http.RoundTripper
	last  wireObservation
	label string
}

func coding(value string) string {
	if value == "" {
		return "identity"
	}
	return value
}
func gzipMagic(body []byte) bool { return len(body) >= 2 && body[0] == 31 && body[1] == 139 }
func (transport *observedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	request.Body.Close()
	request.Body = io.NopCloser(bytes.NewReader(body))
	transport.last = wireObservation{Label: transport.label, RequestAccept: request.Header.Get("Accept-Encoding"), RequestEncoding: coding(request.Header.Get("Content-Encoding")), RequestBytes: len(body), RequestGzip: gzipMagic(body)}
	response, err := transport.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	body, err = io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	transport.last.ResponseEncoding = coding(response.Header.Get("Content-Encoding"))
	transport.last.ResponseBytes = len(body)
	transport.last.ResponseGzip = gzipMagic(body)
	transport.last.Status = response.StatusCode
	encoded, err := json.Marshal(transport.last)
	must(err)
	fmt.Println("WIRE " + string(encoded))
	return response, nil
}
func gzipClient(service protoreflect.ServiceDescriptor, url, target string) {
	base := &http.Transport{Proxy: nil, DisableCompression: true}
	defer base.CloseIdleConnections()
	observed := &observedTransport{base: base}
	httpClient := &http.Client{Transport: observed, Timeout: 5 * time.Second}
	for _, codec := range []string{"proto", "json"} {
		desc := service.Methods().ByName("Echo")
		common := []connect.ClientOption{connect.WithResponseInitializer(initializer), connect.WithSchema(desc)}
		if codec == "json" {
			common = append(common, connect.WithProtoJSON())
		}
		for _, mode := range []string{"gzip-gzip", "identity-gzip", "gzip-identity"} {
			label := "Go->" + target + " " + codec + " " + mode
			observed.label = label
			options := append([]connect.ClientOption{}, common...)
			if mode != "identity-gzip" {
				options = append(options, connect.WithSendGzip())
			}
			stub := connect.NewClient[dynamicpb.Message, dynamicpb.Message](httpClient, url+prefix+"Echo", options...)
			msg := message(desc.Input())
			msg.Set(desc.Input().Fields().ByName("name"), protoreflect.ValueOfString(strings.Repeat("gzip ", 40)))
			request := connect.NewRequest(msg)
			accept := "gzip, identity"
			if mode == "gzip-identity" {
				accept = "identity"
			}
			request.Header().Set("Accept-Encoding", accept)
			request.Header().Add("X-Request", "a")
			request.Header().Add("X-Request", "b")
			response, err := stub.CallUnary(context.Background(), request)
			must(err)
			check(proto.Equal(response.Msg, msg), label+" message")
			expectedRequest := "gzip"
			if mode == "identity-gzip" {
				expectedRequest = "identity"
			}
			expectedResponse := "gzip"

			check(observed.last.RequestEncoding == expectedRequest && observed.last.RequestGzip == (expectedRequest == "gzip"), label+" actual request coding")
			check(observed.last.ResponseEncoding == expectedResponse && observed.last.ResponseGzip == (expectedResponse == "gzip"), label+" actual response coding")
			echo := []string{"a", "b"}
			repeat := []string{"one", "two"}
			tail := []string{"three", "four"}
			if target == "Ruby" {
				echo = []string{"a, b"}
				repeat = []string{"one, two"}
				tail = []string{"three, four"}
			}
			check(reflect.DeepEqual(response.Header().Values("X-Echo"), echo), label+" metadata")
			check(reflect.DeepEqual(response.Header().Values("X-Repeat"), repeat), label+" leading")
			check(reflect.DeepEqual(response.Trailer().Values("X-Tail"), tail), label+" trailers")
			fmt.Println("PASS " + label + " typed/metadata/trailers/actual-wire")
			if mode == "gzip-identity" {
				check(strings.Contains(observed.last.RequestAccept, "gzip"), "reference client accept overwrite changed")
				fmt.Println("REFERENCE QUIRK: public Go client overwrites identity advertisement with gzip; raw probe follows")
			}
		}
		gzipIdentityRaw(httpClient, observed, desc, url, target, codec)
		options := append(append([]connect.ClientOption{}, common...), connect.WithSendGzip())
		observed.label = "Go->" + target + " " + codec + " gzip error"
		failDesc := service.Methods().ByName("Fail")
		failOptions := append(append([]connect.ClientOption{}, options...), connect.WithSchema(failDesc))
		stub := connect.NewClient[dynamicpb.Message, dynamicpb.Message](httpClient, url+prefix+"Fail", failOptions...)
		msg := message(desc.Input())
		_, err := stub.CallUnary(context.Background(), connect.NewRequest(msg))
		var failure *connect.Error
		check(errors.As(err, &failure), "gzip error type")
		check(failure.Code() == connect.CodeInvalidArgument && failure.Message() == "deliberate", "gzip error code/message")
		check(len(failure.Details()) == 1, "gzip error detail count")
		detail, err := failure.Details()[0].Value()
		must(err)
		check(proto.Equal(detail, msg), "gzip typed detail")
		expectedHead := []string{"a", "b"}
		expectedTail := []string{"c", "d"}
		if target == "Ruby" {
			expectedHead = []string{"a, b"}
			expectedTail = []string{"c, d"}
		}
		check(reflect.DeepEqual(failure.Meta().Values("X-Error"), expectedHead) && reflect.DeepEqual(failure.Meta().Values("X-End"), expectedTail), "gzip error metadata")
		check(observed.last.RequestGzip && observed.last.ResponseEncoding == "identity", "gzip error wire")
		fmt.Println("PASS " + observed.label + " details/metadata/trailers/identity-error-wire")
		observed.label = "Go->" + target + " " + codec + " gzip request limit"
		stub = connect.NewClient[dynamicpb.Message, dynamicpb.Message](httpClient, url+prefix+"Echo", options...)
		msg.Set(desc.Input().Fields().ByName("name"), protoreflect.ValueOfString(strings.Repeat("x", 2048)))
		_, err = stub.CallUnary(context.Background(), connect.NewRequest(msg))
		check(connect.CodeOf(err) == connect.CodeResourceExhausted, "gzip decoded request limit")
		fmt.Println("PASS " + observed.label)
		observed.label = "Go->" + target + " " + codec + " gzip response limit"
		options = append(options, connect.WithReadMaxBytes(1))
		stub = connect.NewClient[dynamicpb.Message, dynamicpb.Message](httpClient, url+prefix+"Echo", options...)
		_, err = stub.CallUnary(context.Background(), connect.NewRequest(message(desc.Input())))
		check(connect.CodeOf(err) == connect.CodeResourceExhausted, "gzip receive limit")
		fmt.Println("PASS " + observed.label)
	}
}

// The public connect-go client rewrites Accept-Encoding. This separate raw
// request proves the asymmetric header through independent Go gzip/HTTP/codecs.
func gzipIdentityRaw(client *http.Client, observed *observedTransport, desc protoreflect.MethodDescriptor, url, target, codec string) {
	msg := message(desc.Input())
	var body []byte
	var err error
	if codec == "proto" {
		body, err = proto.Marshal(msg)
	} else {
		body, err = protojson.Marshal(msg)
	}
	must(err)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err = writer.Write(body)
	must(err)
	must(writer.Close())
	request, err := http.NewRequest(http.MethodPost, url+prefix+"Echo", &compressed)
	must(err)
	request.Header.Set("Content-Type", "application/"+codec)
	request.Header.Set("Content-Encoding", "gzip")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Connect-Protocol-Version", "1")
	request.Header.Add("X-Request", "raw")
	observed.label = "raw Go->" + target + " " + codec + " gzip-identity"
	response, err := client.Do(request)
	must(err)
	defer response.Body.Close()
	body, err = io.ReadAll(response.Body)
	must(err)
	check(observed.last.RequestAccept == "identity" && observed.last.RequestGzip, "raw asymmetric request")
	check(response.StatusCode == 200, "raw asymmetric status")
	if target == "Go" {
		check(observed.last.ResponseGzip, "Go request-coding preference quirk")
		reader, err := gzip.NewReader(bytes.NewReader(body))
		must(err)
		body, err = io.ReadAll(reader)
		must(err)
		must(reader.Close())
	} else {
		check(observed.last.ResponseEncoding == "identity" && !observed.last.ResponseGzip, "Ruby asymmetric response")
	}
	result := dynamicpb.NewMessage(desc.Output())
	if codec == "proto" {
		err = proto.Unmarshal(body, result)
	} else {
		err = protojson.Unmarshal(body, result)
	}
	must(err)
	check(proto.Equal(result, msg), "raw asymmetric typed message")
	check(reflect.DeepEqual(response.Header.Values("X-Echo"), []string{"raw"}), "raw asymmetric metadata")
	fmt.Println("PASS " + observed.label + " typed/metadata/actual-wire")
}
