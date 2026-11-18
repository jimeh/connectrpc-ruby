# frozen_string_literal: true

ConnectRPC::Async::Host.new(server: ConnectRPC::Server.new(services: [], binding: APIBinding.new),
                            max_receive_window: "16 MiB")
