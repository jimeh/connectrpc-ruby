# frozen_string_literal: true

require "optparse"
require "uri"
require_relative "certs"

module DemoExample
  Options = Struct.new(:client, :server, :url, :rpc, :args, :codec, :get, :gzip, :protocol, keyword_init: true) do
    def encoding = gzip ? :gzip : :identity
    # The example servers serve https:// only with --mtls.
    def mtls? = url.start_with?("https://")
    def settings = [protocol, codec, encoding, ("GET" if get), ("mTLS" if mtls?)].compact.join(", ")
    def pairing = to_h.slice(:client, :server, :rpc, :protocol, :codec, :get).merge(mtls: mtls?)

    # Native transports take only scheme, host and port.
    def origin
      uri = URI(url)
      "#{uri.scheme}://#{uri.host}:#{uri.port}"
    end
  end

  # Demo plumbing, not ConnectRPC usage: parses client arguments, finds the
  # target server and refuses unsupported pairings before the client opens any
  # connection.
  module CLI
    USAGE = <<~TEXT.freeze
      RPCs:
          greet [NAME]       Unary (default: World)
          fail               Unary with an empty name, returns invalid_argument
          countdown [N]      Server streaming, N down to 1 (default: 3)
          collect NAMES...   Client streaming (default: Ada Grace)
          chat NAMES...      Bidi streaming, one reply per name (default: Ada Grace)
      Without --server or --url the client calls the server recorded in
      #{State.path.delete_prefix("#{ROOT}/")}. An https:// server URL means the
      server runs with --mtls, and the client presents the demo client
      certificate from #{Certs.display_dir}. See #{README}.
    TEXT
    DEFAULTS = { rpc: "greet", codec: :proto, get: false, gzip: false, protocol: :connect }.freeze

    module_function

    def parse(client, argv)
      $stdout.sync = true # Keep printed results in order with errors on stderr.
      parser = option_parser(client)
      flags = {}
      rpc, *args = parser.parse(argv, into: flags)
      resolve!(Options.new(**DEFAULTS, **flags, client: client, rpc: rpc || DEFAULTS[:rpc], args: args))
    rescue OptionParser::ParseError => e
      abort "#{e.message}\n\n#{parser}"
    rescue Refusal => e
      warn "Refused: #{e.message}"
      exit 2
    end

    # Fills in the target server and refuses anything the pairing cannot do.
    def resolve!(options)
      raise Refusal, "Unknown RPC #{options.rpc}; choose one of: #{RPCS.keys.join(', ')}" unless RPCS.key?(options.rpc)

      options.server, options.url = Target.resolve(server: options.server, url: options.url)
      Support.check!(**options.pairing)
      require_certificates!(options) if options.mtls?
      options
    end

    def require_certificates!(options)
      return if Certs.present?

      raise Refusal, "#{options.url} needs the demo client certificate, but #{Certs.display_dir} has none.\n" \
                     "Start the server with: mise run example:server -- #{options.server} --mtls"
    end

    # Each long option name becomes the matching Options field.
    def option_parser(client)
      OptionParser.new do |parser|
        parser.banner = "Usage: mise run example:client -- #{client} [RPC [ARGS...]] [options]"
        parser.on("--server NAME", "Server you are calling (#{SERVERS.keys.join(', ')})")
        parser.on("--url URL", "Server URL (default: from the running server)")
        parser.on("--codec CODEC", %i[proto json], "proto (default) or json")
        parser.on("--get", "Use Connect GET for unary calls")
        parser.on("--gzip", "Compress requests and accept gzip responses")
        parser.on("--protocol PROTOCOL", %i[connect grpc], "connect (default) or grpc")
        parser.separator("\n#{USAGE}")
      end
    end
  end
end
