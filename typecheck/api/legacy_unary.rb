# frozen_string_literal: true

class APILegacyUnaryConsumer
  def self.build(transport)
    ConnectRPC::Client.new(base_url: "https://typed.invalid", binding: APIBinding.new, transport: transport)
  end
end
