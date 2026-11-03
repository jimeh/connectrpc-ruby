# frozen_string_literal: true

outgoing = ConnectRPC::TransportRequest.new(url: URI("http://example.invalid"), http_method: "POST", headers: {},
                                            body: "", response_limit: 10)
outgoing[-1]&.timeout_ms
