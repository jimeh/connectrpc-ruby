# frozen_string_literal: true

require "uri"
require "ipaddr"

module ConnectRPC
  # Chooses the HTTP proxy for one request URL, or none. Environment rules
  # follow Go net/http (golang.org/x/net/http/httpproxy as vendored by Go 1.27).
  # Internal: transports decide which proxy schemes they can use.
  module Proxy
    # A proxy for one request. The user and password are percent-decoded.
    Endpoint = Data.define(:scheme, :host, :port, :user, :password)

    class Endpoint
      def inspect = "#<#{self.class} #{scheme} proxy>"
      def to_s = inspect
    end

    PROXY_PORTS = { "http" => 80, "https" => 443, "socks5" => 1080, "socks5h" => 1080 }.freeze
    TARGET_PORTS = { "http" => 80, "https" => 443 }.freeze

    # A proxy that this call must not use. The reason is a fixed symbol and the
    # message never includes the proxy value.
    class Unusable < StandardError
      attr_reader :reason

      def initialize(reason)
        @reason = reason
        super("Proxy unusable: #{reason}")
      end
    end

    # :environment snapshots the proxy environment once; a String or URI is
    # used for every request; nil is always direct.
    def self.resolver(option, env = ENV)
      case option
      when :environment then Environment.new(env)
      when nil then Static.new(nil)
      when String, URI::Generic then Static.new(explicit(option))
      else raise ArgumentError, "Invalid proxy configuration"
      end
    end

    def self.explicit(option)
      uri = option.is_a?(String) ? parse(option) : option
      endpoint = uri && endpoint(uri)
      raise ArgumentError, "Invalid proxy configuration" unless endpoint

      endpoint
    end
    private_class_method :explicit

    # Go retries a value without a scheme or host as http://value. A value
    # that still fails to parse is unusable.
    def self.parse(value) # rubocop:disable Metrics/MethodLength
      uri = begin
        URI.parse(value)
      rescue URI::InvalidURIError
        nil
      end
      if uri.nil? || uri.scheme.nil? || uri.host.to_s.empty?
        begin
          return URI.parse("http://#{value}")
        rescue URI::InvalidURIError
          nil
        end
      end
      uri
    end

    # Nil unless the URL names a scheme, a host and any port in 1..65535. A
    # scheme without a default port keeps a nil port; transports reject it.
    def self.endpoint(uri) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
      scheme = uri.scheme&.downcase
      host = uri.hostname
      return unless scheme && host && !host.empty?

      port = uri.port || PROXY_PORTS[scheme]
      return if port && !port.between?(1, 65_535)

      user = uri.user
      password = uri.password
      Endpoint.new(scheme: scheme.freeze, host: host.dup.freeze, port: port,
                   user: user && URI.decode_uri_component(user).freeze,
                   password: password && URI.decode_uri_component(password).freeze)
    end

    # Go net.SplitHostPort, returning nil where Go returns an error.
    def self.split_host_port(value) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
      if value.start_with?("[")
        close = value.index("]")
        return unless close && value[close + 1] == ":"

        host = value[1...close].to_s
        port = value[(close + 2)..].to_s
      else
        index = value.rindex(":")
        return unless index

        host = value[0...index].to_s
        port = value[(index + 1)..].to_s
        return if host.include?(":")
      end
      return if [host, port].any? { |part| part.include?("[") || part.include?("]") }

      [host, port]
    end

    # IPv4-mapped IPv6 addresses compare as IPv4, as Go net.IP does.
    def self.ip(value)
      return unless /\A[0-9A-Fa-f:.]+\z/.match?(value)

      address = IPAddr.new(value)
      address.ipv4_mapped? ? address.native : address
    rescue IPAddr::Error
      nil
    end

    class Static
      attr_reader :endpoint

      def initialize(endpoint)
        @endpoint = endpoint
        freeze
      end

      def for(_url) = @endpoint
      def inspect = "#<#{self.class} #{@endpoint ? 'explicit' : 'direct'}>"
    end

    class Environment
      def initialize(env)
        @http = value(env, "HTTP_PROXY", "http_proxy")
        @https = value(env, "HTTPS_PROXY", "https_proxy")
        @cgi = !env.fetch("REQUEST_METHOD", nil).to_s.empty?
        @all = false
        @ip_matchers = []
        @domain_matchers = []
        no_proxy(value(env, "NO_PROXY", "no_proxy"))
        @ip_matchers.freeze
        @domain_matchers.freeze
        freeze
      end

      def inspect = "#<#{self.class}>"

      # Order follows Go proxyForURL: select by target scheme, refuse an HTTP
      # proxy in CGI, apply NO_PROXY, then require a usable address.
      def for(url)
        scheme = url.scheme.to_s.downcase
        proxy = { "https" => @https, "http" => @http }[scheme]
        uri = proxy && Proxy.parse(proxy)
        return unless uri
        raise Unusable.new(:cgi) if scheme == "http" && @cgi # rubocop:disable Style/RaiseArgs
        return unless use_proxy?(url, scheme)

        Proxy.endpoint(uri) || raise(Unusable.new(:invalid_address)) # rubocop:disable Style/RaiseArgs
      end

      private

      def value(env, *names)
        names.each do |name|
          value = env.fetch(name, nil).to_s
          return value.dup.freeze unless value.empty?
        end
        nil
      end

      def use_proxy?(url, scheme) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        host = url.hostname.to_s.strip.downcase
        port = (url.port || TARGET_PORTS[scheme]).to_s
        return false if host == "localhost"

        address = Proxy.ip(host)
        return false if address&.loopback? || @all
        # Domain entries never match an IP host.
        return @ip_matchers.none? { |matcher| ip_match?(matcher, address, port) } if address

        @domain_matchers.none? { |matcher| domain_match?(matcher, host, port) }
      end

      def ip_match?(matcher, address, port)
        network, wanted, cidr = matcher
        if cidr
          network.family == address.family && network.include?(address)
        else
          network == address && (wanted.empty? || wanted == port)
        end
      end

      def domain_match?(matcher, host, port)
        suffix, wanted, exact = matcher
        (host.end_with?(suffix) || (exact && host == suffix[1..])) && (wanted.empty? || wanted == port)
      end

      def no_proxy(value) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        value.to_s.split(",").each do |entry|
          entry = entry.strip.downcase
          next if entry.empty?

          if entry == "*"
            @all = true
            break
          end

          network = cidr(entry)
          next @ip_matchers << [network, "", true] if network

          host, port = Proxy.split_host_port(entry)
          if host
            next if host.empty?
          else
            host = entry
            port = ""
          end
          address = Proxy.ip(host)
          next @ip_matchers << [address, port.to_s, false] if address
          next if host.empty?

          host = host[1..].to_s if host.start_with?("*.")
          exact = !host.start_with?(".")
          host = ".#{host}" if exact
          @domain_matchers << [host.freeze, port.to_s.freeze, exact]
        end
        nil
      end

      def cidr(entry)
        address, prefix = entry.split("/", 2)
        return unless address && prefix && /\A[0-9]{1,3}\z/.match?(prefix)
        return unless Proxy.ip(address)

        IPAddr.new(entry)
      rescue IPAddr::Error
        nil
      end
    end
  end
end
