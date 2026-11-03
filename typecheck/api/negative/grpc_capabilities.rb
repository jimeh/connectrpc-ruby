# frozen_string_literal: true

ConnectRPC::Client.new(base_url: "https://typed.invalid", binding: APIBinding.new, transport: BadGRPCTransport.new)
