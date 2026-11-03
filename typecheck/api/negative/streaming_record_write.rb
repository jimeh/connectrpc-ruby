# frozen_string_literal: true

ConnectRPC::StreamRequest.new(url: URI("https://typed.invalid"), http_method: "POST", headers: {})[:deadline] = "later"
