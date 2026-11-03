# frozen_string_literal: true

class ::APIInlineFalconConfiguration
  def configure(loader)
    loader.service "inline-blocks" do
      include ConnectRPC::Falcon::Environment

      rack_app { ->(env) { [200, {}, [env.fetch("PATH_INFO").to_s]] } }
      tls_provider { -> { { version: "v1", certificate_chain: ["leaf"], private_key: "key", client_ca: nil } } }
    end
    loader.service "inline-values" do
      include ConnectRPC::Falcon::Environment

      rack_app ->(env) { [200, {}, [env.fetch("PATH_INFO").to_s]] }
      tls_provider -> { { version: "v1", certificate_chain: ["leaf"], private_key: "key", client_ca: nil } }
    end
  end
end
