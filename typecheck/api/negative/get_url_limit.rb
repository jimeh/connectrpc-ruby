# frozen_string_literal: true

ConnectRPC::Client.new(base_url: "https://example.invalid", binding: APIBinding.new, get_url_limit: "8192")
