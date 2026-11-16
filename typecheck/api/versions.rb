# frozen_string_literal: true

# @type var versions: Array[String]
versions = [ConnectRPC::VERSION, ConnectRPC::Rack::VERSION, ConnectRPC::OnRails::VERSION,
            ConnectRPC::Async::VERSION, ConnectRPC::Falcon::VERSION,
            ConnectRPC::Testing::VERSION, ConnectRPC::OTel::VERSION, ConnectRPC::Prometheus::VERSION,
            ConnectRPC::Yabeda::VERSION]
versions.each { |version| version.split(".") }
