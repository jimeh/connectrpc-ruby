# frozen_string_literal: true

client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: APIBinding.new)
method = ConnectRPC::Method.new(service: "example.Test", name: "Call", request_type: String, response_type: String)
client.call(method, "request", cancellation: ConnectRPC::CancellationSource.new)
