# frozen_string_literal: true

ConnectRPC::Server.new(services: [], binding: APIBinding.new).start_call(path: "/typed.API/Call", headers: {},
                                                                         http_method: :GET)
