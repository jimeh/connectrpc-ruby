# frozen_string_literal: true

ConnectRPC::Method.new(service: "demo.Service", name: "Call", request_type: String, response_type: String,
                       idempotency: :safe)
