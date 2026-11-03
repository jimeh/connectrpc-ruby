# frozen_string_literal: true

APIGRPCOnlyTransport.new.open_grpc_stream(ConnectRPC::StreamRequest.new(url: URI("https://typed.invalid"),
                                                                        http_method: "POST", headers: {}))
