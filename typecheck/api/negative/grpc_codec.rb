# frozen_string_literal: true

ConnectRPC::Client.new(base_url: "https://typed.invalid", binding: APIBinding.new, protocol: :grpc,
                       status_codec: BadStatusCodec.new)
