# frozen_string_literal: true

ConnectRPC::Server.new(services: [], binding: APIBinding.new).call(path: "/demo.Service/Call", http_method: "GET",
                                                                   headers: {}, input: nil, query: nil)
