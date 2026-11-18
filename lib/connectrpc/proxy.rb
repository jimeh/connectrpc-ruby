# frozen_string_literal: true

require "uri"
require "ipaddr"
require "socket"

module ConnectRPC
  # Chooses the HTTP proxy for one request URL, or none. Environment rules
  # follow Go net/http (golang.org/x/net/http/httpproxy as vendored by Go 1.27).
  # Internal: transports decide which proxy schemes they can use.
  module Proxy # rubocop:disable Metrics/ModuleLength
    # A proxy for one request. The user and password are percent-decoded.
    Endpoint = Data.define(:scheme, :host, :port, :user, :password)

    # Data#pretty_print ignores inspect, so pp would show the credentials.
    class Endpoint
      def inspect = "#<#{self.class} #{scheme} proxy>"
      def to_s = inspect
      def pretty_print(printer) = printer.text(inspect)
    end

    PROXY_PORTS = { "http" => 80, "https" => 443, "socks5" => 1080, "socks5h" => 1080 }.freeze
    TARGET_PORTS = { "http" => 80, "https" => 443 }.freeze
    LDH_HOST = /\A[a-z0-9.-]+\z/
    SCHEME = /\A[A-Za-z][A-Za-z0-9+.-]*\z/
    # RFC 3986 userinfo, plus the "@" that the last-"@" split leaves in it.
    USERINFO = /\A(?:[A-Za-z0-9\-._~!$&'()*+,;=:@]|%\h\h)*\z/
    ASCII_HOST = /\A[A-Za-z0-9._~-]+\z/
    # ASCII control characters and whitespace, which a proxy value never needs.
    CONTROL = /[\x00-\x20\x7F]/
    # Go's IDNA maps these to "."; NFKC leaves U+3002 as it is.
    FULL_STOPS = "\u3002\uFF0E\uFF61"
    # Characters Go's IDNA deletes or rejects, which this mapping does not
    # reproduce: default-ignorable code points (soft hyphen, variation
    # selectors, ZWJ/ZWNJ) and right-to-left scripts, whose bidi rules are not
    # checked. A host containing one fails closed.
    RIGHT_TO_LEFT = "\u0590-\u08FF\uFB1D-\uFDFF\uFE70-\uFEFF\u{10800}-\u{10FFF}\u{1E800}-\u{1EFFF}"
    UNMAPPED = /[\p{Default_Ignorable_Code_Point}#{RIGHT_TO_LEFT}]/

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
      else raise ArgumentError, "Invalid proxy configuration", cause: nil
      end
    end

    def self.explicit(option)
      endpoint = option.is_a?(String) ? parse(option) : uri_endpoint(option)
      raise ArgumentError, "Invalid proxy configuration", cause: nil unless endpoint

      endpoint
    end

    # URI::HTTP.new skips component checks, so delimiters in a component
    # could reshape the authority once serialized. A URI is accepted only when
    # its text parses back to the same components; the host[:port] shorthand
    # is for Strings only.
    def self.uri_endpoint(uri)
      value = uri.to_s
      return unless value.include?("://")

      reparsed = URI.parse(value)
      same = reparsed.scheme.to_s.casecmp?(uri.scheme.to_s) &&
             %i[host port userinfo].all? { |part| reparsed.public_send(part) == uri.public_send(part) }
      parse(value) if same
    rescue StandardError
      nil
    end
    private_class_method :explicit, :uri_endpoint

    # Parses scheme://userinfo@host:port[tail], or Go's host[:port] shorthand
    # for http, into an Endpoint. Nil for anything else: unlike Go, which goes
    # direct for some malformed values and dials odd ones, every value either
    # names a valid endpoint or fails closed. The tail after the authority
    # cannot change the endpoint and is ignored. Userinfo ends at the last "@"
    # (go.dev/issue/22655) and the authority is split before a host is mapped.
    def self.parse(value) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
      value = utf8(value)
      return if value.nil? || CONTROL.match?(value)

      scheme, separator, rest = value.partition("://")
      if separator.empty?
        scheme = "http"
        rest = value
      end
      return unless SCHEME.match?(scheme)

      scheme = scheme.downcase.freeze
      userinfo, at, authority = rest[%r{\A[^/?#]*}].to_s.rpartition("@")
      user, password = credentials(userinfo) unless at.empty?
      return if !at.empty? && user.nil?

      host, port = host_port(authority, scheme)
      Endpoint.new(scheme: scheme, host: host, port: port, user: user, password: password) if host
    rescue StandardError
      nil
    end

    # [user, password] percent-decoded, never form-decoded; the password is
    # nil without a ":". Nil for characters outside RFC 3986 userinfo, a stray
    # "%" or invalid UTF-8.
    def self.credentials(userinfo)
      return unless USERINFO.match?(userinfo)

      user, colon, password = userinfo.partition(":")
      user = decode(user)
      return [user, nil] if user && colon.empty?

      password = decode(password)
      [user, password] if user && password
    end

    def self.decode(part)
      decoded = part.b.gsub(/%\h\h/) { |escape| escape.delete_prefix("%").hex.chr }.force_encoding(Encoding::UTF_8)
      decoded.freeze if decoded.valid_encoding?
    end

    # [host, port] for a bracketed IPv6 address without a zone or a name, and
    # an optional port in 1..65535, else nil. A missing port takes the scheme's
    # default; a scheme without one keeps nil and transports reject it.
    def self.host_port(value, scheme) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
      if value.start_with?("[")
        address, close, port = value[1..].to_s.partition("]")
        return unless !close.empty? && address.include?(":") && ip(address)
        return unless port.empty? || port.start_with?(":")

        host = address.downcase
        port = port[1..].to_s
      else
        host, _, port = value.partition(":")
        return if host.empty? || host.include?("%") || port.include?(":")

        host = host.ascii_only? ? (host.downcase if ASCII_HOST.match?(host)) : ascii_host(host)
        return if host.nil? || numeric_shorthand?(host)
      end
      return [host.freeze, PROXY_PORTS[scheme]] if port.empty?

      [host.freeze, port.to_i] if /\A[0-9]+\z/.match?(port) && port.to_i.between?(1, 65_535)
    end

    # The C resolver reads old BSD forms such as "010.0.0.1" (octal, so
    # 8.0.0.1), "127.1" and "2130706433" as IPv4 addresses; Go refuses them.
    # Only canonical IP literals are dialled as addresses.
    def self.numeric_shorthand?(host)
      return false if ip(host)

      !Socket.getaddrinfo(host, nil, nil, :STREAM, nil, Socket::AI_NUMERICHOST).empty?
    rescue SocketError
      false
    end
    private_class_method :credentials, :decode, :host_port, :numeric_shorthand?

    # A valid UTF-8 copy of the value's bytes, or nil. Go handles bytes in any
    # encoding, so an invalidly encoded value is unparseable, not an exception.
    def self.utf8(value)
      value = value.to_s.b.force_encoding(Encoding::UTF_8)
      value if value.valid_encoding?
    end

    # Maps a non-ASCII host as Go's idna.Lookup.ToASCII does for ordinary
    # names: ideographic full stops as dots, NFKC, lowercase, then Punycode
    # for each non-ASCII label. Nil when NFKC adds a dot (U+2024 or U+2488,
    # for example), which would name a different host, or when the result is
    # not a letter, digit, hyphen and dot name, which Go's STD3 rules reject,
    # so a mapping never yields a delimiter such as "@", "/" or ":". UTS #46's
    # other validity checks and mappings are not applied.
    def self.ascii_host(host)
      return host if host.ascii_only?
      return if UNMAPPED.match?(host)

      dotted = host.tr(FULL_STOPS, ".")
      normalized = dotted.unicode_normalize(:nfkc).downcase
      return if normalized.count(".") > dotted.count(".")

      labels = normalized.split(".", -1)
      return if labels.any? { |label| label.start_with?("-") || label.end_with?("-") }

      mapped = labels.map { |label| label.ascii_only? ? label : "xn--#{Punycode.encode(label)}" }.join(".")
      mapped if LDH_HOST.match?(mapped)
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

    # RFC 3492 Punycode encoding of one label, without the "xn--" prefix.
    module Punycode
      BASE = 36
      T_MIN = 1
      T_MAX = 26
      SKEW = 38
      DAMP = 700

      def self.encode(label) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity
        points = label.codepoints
        output = points.select { |point| point < 0x80 }.pack("U*")
        basic = handled = output.length
        output << "-" if basic.positive?
        code = 0x80
        delta = 0
        bias = 72
        while handled < points.length
          following = points.select { |point| point >= code }.min.to_i
          delta += (following - code) * (handled + 1)
          code = following
          points.each do |point|
            delta += 1 if point < code
            next unless point == code

            output << digits(delta, bias)
            bias = adapt(delta, handled + 1, handled == basic)
            delta = 0
            handled += 1
          end
          delta += 1
          code += 1
        end
        output
      end

      # The generalized variable-length integer for one delta.
      def self.digits(delta, bias)
        output = +""
        k = BASE
        loop do
          threshold = (k - bias).clamp(T_MIN, T_MAX)
          return output << digit(delta) if delta < threshold

          output << digit(threshold + ((delta - threshold) % (BASE - threshold)))
          delta = (delta - threshold) / (BASE - threshold)
          k += BASE
        end
      end

      def self.digit(value) = (value < 26 ? value + 97 : value + 22).chr

      def self.adapt(delta, points, first)
        delta = first ? delta / DAMP : delta / 2
        delta += delta / points
        k = 0
        while delta > ((BASE - T_MIN) * T_MAX) / 2
          delta /= BASE - T_MIN
          k += BASE
        end
        k + (((BASE - T_MIN + 1) * delta) / (delta + SKEW))
      end
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
        @http = proxy(value(env, "HTTP_PROXY", "http_proxy"))
        @https = proxy(value(env, "HTTPS_PROXY", "https_proxy"))
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
        return unless proxy
        raise Unusable.new(:cgi), cause: nil if scheme == "http" && @cgi # rubocop:disable Style/RaiseArgs
        return unless use_proxy?(url, scheme)
        raise Unusable.new(:invalid_address), cause: nil unless proxy.is_a?(Endpoint) # rubocop:disable Style/RaiseArgs

        proxy
      end

      private

      # Parses a value once, as Go does: an Endpoint, :unusable for any value
      # that is not a valid endpoint, or nil for none.
      def proxy(value)
        return unless value

        Proxy.parse(value) || :unusable
      end

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
        # Go keeps an entry with invalid bytes or an unmappable name as written,
        # so it never matches an ASCII host; drop it and keep the others.
        value.to_s.b.split(",").each do |raw|
          entry = Proxy.utf8(raw)&.strip&.downcase
          next if entry.nil? || entry.empty?

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
          host = Proxy.ascii_host(host)
          @domain_matchers << [host.freeze, port.to_s.freeze, exact] if host
        end
        nil
      end

      # Go matches an IPv4-mapped network of /96 or longer against IPv4 hosts.
      def cidr(entry)
        address, prefix = entry.split("/", 2)
        return unless address && prefix && /\A[0-9]{1,3}\z/.match?(prefix)
        return unless Proxy.ip(address)

        network = IPAddr.new(entry)
        network.ipv4_mapped? ? network.native : network
      rescue IPAddr::Error
        nil
      end
    end
  end
end
