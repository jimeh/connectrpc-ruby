# frozen_string_literal: true

require "json"
require "fileutils"

# Plain-Ruby support shared by the runnable servers, clients, launcher and
# verification script. It loads no gems, so every owning bundle can use it.
module DemoExample
  ROOT = File.expand_path("../..", __dir__)
  DEFAULT_PORT = 8080
  README = "examples/README.md"

  SHAPES = {
    unary: "unary", server_stream: "server streaming", client_stream: "client streaming", bidi: "bidi streaming"
  }.freeze
  ALL = SHAPES.keys.freeze
  H1_STREAMING = %i[unary server_stream client_stream].freeze
  PROTOCOLS = { connect: "Connect", grpc: "gRPC" }.freeze

  RPCS = {
    "greet" => :unary, "fail" => :unary, "countdown" => :server_stream, "collect" => :client_stream, "chat" => :bidi
  }.freeze

  # What each server hosts, per HTTP version and protocol. Mirrors docs/support.md.
  SERVERS = {
    "rack" => { "HTTP/1.1" => { connect: [:unary] } },
    "rails" => { "HTTP/1.1" => { connect: [:unary] } },
    "async" => { "HTTP/2" => { connect: ALL, grpc: ALL } },
    "falcon" => { "HTTP/1.1" => { connect: H1_STREAMING }, "HTTP/2" => { connect: ALL, grpc: ALL } }
  }.freeze

  # Each client speaks exactly one HTTP version.
  CLIENTS = {
    "nethttp" => { http: "HTTP/1.1", protocols: { connect: [:unary] } },
    "async-h2" => { http: "HTTP/2", protocols: { connect: ALL, grpc: ALL } },
    "async-http1" => { http: "HTTP/1.1", protocols: { connect: H1_STREAMING } }
  }.freeze

  # Raised for any pairing or target the examples deliberately do not attempt.
  class Refusal < StandardError; end

  # Decides whether a client can call an RPC on a server before any network I/O.
  module Support
    module_function

    def check!(client:, server:, rpc:, protocol: :connect, codec: :proto, get: false)
      reason = option_reason(rpc, protocol, codec, get)
      raise Refusal, "#{reason}.\nSee #{README} for the supported options." if reason

      reason = pairing_reason(client, server, rpc, protocol)
      return unless reason

      raise Refusal, [
        "#{reason}.", *alternatives(client, server, rpc, protocol), "See #{README} for the supported pairings."
      ].join("\n")
    end

    def allowed?(client, server, rpc, protocol) = pairing_reason(client, server, rpc, protocol).nil?

    def option_reason(rpc, protocol, codec, get)
      return "gRPC requires the protobuf codec; drop --codec json" if protocol == :grpc && codec == :json
      return "gRPC has no GET form; drop --get" if protocol == :grpc && get

      "--get applies only to unary RPCs (greet, fail), not #{rpc}" if get && RPCS.fetch(rpc) != :unary
    end

    # A pair works only when the client can make the call at all, and the server
    # hosts that protocol and RPC shape on the client's one HTTP version.
    def pairing_reason(client, server, rpc, protocol)
      client_reason(client, rpc, protocol) || server_reason(client, server, rpc, protocol)
    end

    def client_reason(client, rpc, protocol)
      shape = RPCS.fetch(rpc)
      shapes = CLIENTS.fetch(client)[:protocols][protocol]
      return "#{client} does not support #{PROTOCOLS[protocol]}; native gRPC needs HTTP/2" unless shapes
      return if shapes.include?(shape)

      "#{client} supports only #{names(shapes)} #{PROTOCOLS[protocol]} calls; #{rpc} is #{SHAPES[shape]}"
    end

    def server_reason(client, server, rpc, protocol)
      http = CLIENTS.fetch(client)[:http]
      served = SERVERS.fetch(server)
      return "#{client} uses #{http}, but #{server} serves only #{served.keys.join(' and ')}" unless served[http]

      shapes = served[http][protocol]
      return "#{server} does not serve #{PROTOCOLS[protocol]} over #{http}" unless shapes
      return if shapes.include?(RPCS.fetch(rpc))

      "#{server} serves only #{names(shapes)} #{PROTOCOLS[protocol]} calls over #{http}; " \
        "#{rpc} is #{SHAPES[RPCS.fetch(rpc)]}"
    end

    def alternatives(client, server, rpc, protocol)
      over = protocol == :grpc ? " with --protocol grpc" : ""
      clients = CLIENTS.keys.select { |name| allowed?(name, server, rpc, protocol) }
      servers = SERVERS.keys.select { |name| allowed?(client, name, rpc, protocol) }
      lines = []
      lines << "Clients that can call #{rpc} on #{server}#{over}: #{clients.join(', ')}" if clients.any?
      lines << "Servers that #{client} can call #{rpc} on#{over}: #{servers.join(', ')}" if servers.any?
      lines.empty? ? ["Pairings that can call #{rpc}#{over}: #{working_pairs(rpc, protocol)}"] : lines
    end

    def working_pairs(rpc, protocol)
      pairs = CLIENTS.keys.product(SERVERS.keys).filter_map do |client, server|
        "#{client} -> #{server}" if allowed?(client, server, rpc, protocol)
      end
      pairs.join(", ")
    end

    def names(shapes) = shapes.map { |shape| SHAPES[shape] }.join(", ")
  end

  # The state file the running server writes so clients can find it. Paths are
  # absolute because the owning bundle runners change directory.
  module State
    module_function

    def path = File.expand_path(ENV.fetch("CONNECTRPC_EXAMPLE_STATE", "vendor/examples/server.json"), ROOT)

    def write(server:, url:)
      FileUtils.mkdir_p(File.dirname(path))
      temporary = "#{path}.#{Process.pid}"
      File.write(temporary, JSON.generate(server: server, url: url, pid: Process.pid))
      File.rename(temporary, path)
    end

    def remove
      File.delete(path) if read&.fetch("pid", nil) == Process.pid
    rescue Errno::ENOENT
      nil
    end

    def read
      JSON.parse(File.read(path))
    rescue Errno::ENOENT, JSON::ParserError
      nil
    end

    # Returns the record of a server that is still running, or raises with how to start one.
    def live!
      record = read
      raise Refusal, not_running("no state file at #{path}") unless record

      Process.kill(0, record.fetch("pid"))
      record
    rescue Errno::ESRCH
      raise Refusal, not_running("#{path} names pid #{record['pid']}, which has exited")
    rescue Errno::EPERM
      record
    end

    def not_running(why)
      "No running example server found (#{why}).\nStart one with: mise run example:server -- NAME " \
        "(#{SERVERS.keys.join(', ')}), or pass --server NAME [--url URL]."
    end
  end

  # Resolves which server a client calls and at what URL.
  module Target
    module_function

    def resolve(server:, url:)
      return explicit(server, url) if server

      record = State.live!
      return [record.fetch("server"), record.fetch("url")] if url.nil? || same_url?(url, record.fetch("url"))

      raise Refusal, "--url #{url} is not the running example server (#{record['server']} at #{record['url']}); " \
                     "also pass --server NAME to name the server at that URL"
    end

    # --server names the host, so the pairing check can run without a state file.
    def explicit(server, url)
      raise Refusal, "Unknown server #{server}; choose one of: #{SERVERS.keys.join(', ')}" unless SERVERS.key?(server)

      [server, url || running_url(server) || "http://127.0.0.1:#{ENV.fetch('PORT', DEFAULT_PORT)}"]
    end

    # Reuses the state file's URL only when it records the server that --server names.
    def running_url(server)
      record = State.live!
      record.fetch("url") if record.fetch("server") == server
    rescue Refusal
      nil
    end

    def same_url?(left, right) = left.delete_suffix("/") == right.delete_suffix("/")
  end
end
