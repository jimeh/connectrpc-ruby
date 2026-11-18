# frozen_string_literal: true

# The application can supply the real loader without a file-level self override.
class ::APIFalconConfiguration
  def configure(loader) # rubocop:disable Metrics/MethodLength
    loader.service "typed-connectrpc" do
      include ConnectRPC::Falcon::Environment

      url { "http://127.0.0.1:9292" }
      count 2
      prepare! { |_instance| nil }
      connectrpc_server { ConnectRPC::Server.new(services: [], binding: APIBinding.new) }
      rack_app { APIApp.new }
      tls { nil }
      tls_provider { APIProvider.new }
      server_options do
        { idle_timeout: nil, max_connections: 8, cache: true, keepalive: nil, stream_receive_window: 1_048_576,
          dynamic_receive_windows: true, max_receive_window: 4_194_304 }
      end
      connectrpc_close_grace 2
    end
  end
end
