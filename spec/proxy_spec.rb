# frozen_string_literal: true

require "pp"
require "connectrpc/proxy"

# Cases ported from golang.org/x/net/http/httpproxy proxy_test.go, adjusted to
# the Go 1.27 vendored precedence (uppercase variables first). Examples that
# differ from Go on purpose say so.
RSpec.describe ConnectRPC::Proxy do
  def resolve(env, url = "http://example.com")
    endpoint = described_class.resolver(:environment, env).for(URI(url))
    endpoint && [endpoint.scheme, endpoint.host, endpoint.port]
  end

  [
    [{ "HTTP_PROXY" => "127.0.0.1:8080" }, "http://example.com", ["http", "127.0.0.1", 8080]],
    [{ "HTTP_PROXY" => "cache.corp.example.com:1234" }, "http://example.com", ["http", "cache.corp.example.com", 1234]],
    [{ "HTTP_PROXY" => "cache.corp.example.com" }, "http://example.com", ["http", "cache.corp.example.com", 80]],
    [{ "HTTP_PROXY" => "localhost" }, "http://example.com", ["http", "localhost", 80]],
    [{ "HTTP_PROXY" => "https://cache.corp.example.com" }, "http://example.com",
     ["https", "cache.corp.example.com", 443]],
    [{ "HTTP_PROXY" => "http://127.0.0.1:8080" }, "http://example.com", ["http", "127.0.0.1", 8080]],
    [{ "HTTP_PROXY" => "socks5://127.0.0.1" }, "http://example.com", ["socks5", "127.0.0.1", 1080]],
    [{ "HTTP_PROXY" => "foo://host" }, "http://example.com", ["foo", "host", nil]],
    [{ "HTTP_PROXY" => "http.proxy.tld", "HTTPS_PROXY" => "secure.proxy.tld" }, "http://insecure.tld/",
     ["http", "http.proxy.tld", 80]],
    [{ "HTTP_PROXY" => "http.proxy.tld", "HTTPS_PROXY" => "secure.proxy.tld" }, "https://secure.tld/",
     ["http", "secure.proxy.tld", 80]],
    [{ "HTTP_PROXY" => "http.proxy.tld" }, "https://secure.tld/", nil],
    [{ "HTTP_PROXY" => "http.proxy.tld" }, "ftp://insecure.tld/", nil],
    [{ "HTTPS_PROXY" => "https://secure.proxy.tld", "REQUEST_METHOD" => "GET" }, "https://secure.tld/",
     ["https", "secure.proxy.tld", 443]],
    [{}, "http://example.com", nil],
    [{ "NO_PROXY" => "example.com", "HTTP_PROXY" => "proxy" }, "http://example.com/", nil],
    [{ "NO_PROXY" => ".example.com", "HTTP_PROXY" => "proxy" }, "http://example.com/", ["http", "proxy", 80]],
    [{ "NO_PROXY" => "ample.com", "HTTP_PROXY" => "proxy" }, "http://example.com/", ["http", "proxy", 80]],
    [{ "NO_PROXY" => "example.com", "HTTP_PROXY" => "proxy" }, "http://foo.example.com/", nil],
    [{ "NO_PROXY" => ".foo.com", "HTTP_PROXY" => "proxy" }, "http://example.com/", ["http", "proxy", 80]]
  ].each do |env, url, expected|
    it "resolves #{url} with #{env.inspect} to #{expected.inspect}" do
      expect(resolve(env, url)).to eq(expected)
    end
  end

  it "prefers uppercase variables and skips empty ones, as Go 1.27 does" do
    env = { "HTTP_PROXY" => "upper", "http_proxy" => "lower", "HTTPS_PROXY" => "", "https_proxy" => "secure-lower",
            "NO_PROXY" => "upper.test", "no_proxy" => "lower.test" }
    expect(resolve(env)).to eq(["http", "upper", 80])
    expect(resolve(env, "https://example.com")).to eq(["http", "secure-lower", 80])
    expect(resolve(env, "http://upper.test")).to be_nil
    expect(resolve(env, "http://lower.test")).to eq(["http", "upper", 80])
  end

  it "snapshots the environment once per resolver" do
    env = { "HTTPS_PROXY" => "first:1" }
    resolver = described_class.resolver(:environment, env)
    env["HTTPS_PROXY"] = "second:2"
    expect(resolver.for(URI("https://example.com")).host).to eq("first")
    expect(described_class.resolver(:environment, env).for(URI("https://example.com")).host).to eq("second")
  end

  describe "NO_PROXY matching" do
    no_proxy = "foobar.com, .barbaz.net, *.wildcard.io, 192.168.1.1, 192.168.1.2:81, 192.168.1.3:80, " \
               "10.0.0.0/30, 2001:db8::52:0:1, [2001:db8::52:0:2]:443, [2001:db8::52:0:3]:80, 2002:db8:a::45/64"
    {
      "localhost" => false, "127.0.0.1" => false, "127.0.0.2" => false, "[::1]" => false, "[::2]" => true,
      "[::ffff:127.0.0.1]" => false, "192.168.1.1" => false, "192.168.1.2" => true, "192.168.1.3" => false,
      "192.168.1.4" => true, "10.0.0.2" => false, "[::ffff:10.0.0.2]" => false, "[2001:db8::52:0:1]" => false,
      "[2001:db8::52:0:2]" => true, "[2001:db8::52:0:3]" => false, "[2002:db8:a::123]" => false,
      "[fe80::424b:c8be:1643:a1b6]" => true, "barbaz.net" => true, "www.barbaz.net" => false,
      "foobar.com" => false, "www.foobar.com" => false, "WWW.FooBar.com" => false, "foofoobar.com" => true,
      "baz.com" => true, "localhost.net" => true, "local.localhost" => true, "barbarbaz.net" => true,
      "wildcard.io" => true, "nested.wildcard.io" => false, "awildcard.io" => true,
      # Deliberate difference: Go proxies LOCALHOST.
      "LOCALHOST" => false
    }.each do |host, proxied|
      it "#{proxied ? 'proxies' : 'bypasses'} #{host}:80" do
        env = { "HTTP_PROXY" => "proxy", "NO_PROXY" => no_proxy }
        expect(resolve(env, "http://#{host}:80").nil?).to be(!proxied)
        expect(resolve({ "HTTP_PROXY" => "proxy", "NO_PROXY" => "*" }, "http://#{host}:80")).to be_nil
      end
    end

    it "ignores entries with an empty host and never matches domain entries to IP hosts" do
      expect(resolve({ "HTTP_PROXY" => "proxy", "NO_PROXY" => ":1, ,," })).to eq(["http", "proxy", 80])
      # Both entries are suffixes of the target as text; Go 1.27.3 proxies it.
      %w[.3.4 3.4].each do |entry|
        expect(resolve({ "HTTP_PROXY" => "proxy", "NO_PROXY" => entry }, "http://1.2.3.4")).to eq(["http", "proxy", 80])
      end
    end

    it "treats IP-shaped entries that do not parse as domains" do
      env = { "HTTP_PROXY" => "proxy", "NO_PROXY" => "1.2.3, 10.0.0.0/99" }
      expect(resolve(env, "http://host.1.2.3")).to be_nil
      expect(resolve(env, "http://10.0.0.1")).to eq(["http", "proxy", 80])
    end

    it "compares entry ports with the target's default port" do
      env = { "HTTPS_PROXY" => "proxy", "NO_PROXY" => "secure.test:443, plain.test:80" }
      expect(resolve(env, "https://secure.test")).to be_nil
      expect(resolve(env, "https://secure.test:8443")).not_to be_nil
      expect(resolve(env, "https://plain.test")).not_to be_nil
    end
  end

  describe "internationalized names" do
    {
      "münchen" => "mnchen-3ya",
      [0x0644, 0x064A, 0x0647, 0x0645, 0x0627, 0x0628, 0x062A, 0x0643, 0x0644, 0x0645, 0x0648, 0x0634, 0x0639, 0x0631,
       0x0628, 0x064A, 0x061F].pack("U*") => "egbpdaj6bu4bxfgehfvwxn",
      [0x4ED6, 0x4EEC, 0x4E3A, 0x4EC0, 0x4E48, 0x4E0D, 0x8BF4, 0x4E2D, 0x6587].pack("U*") => "ihqwcrb4cv8a8dqg056pqjye",
      [0x0033, 0x5E74, 0x0042, 0x7D44, 0x91D1, 0x516B, 0x5148, 0x751F].pack("U*") => "3B-ww4c5e180e575a65lsy2b"
    }.each do |label, encoded|
      it "Punycode-encodes #{label.inspect} as RFC 3492 does" do
        expect(described_class::Punycode.encode(label)).to eq(encoded)
      end
    end

    # Expectations match http.ProxyFromEnvironment and canonicalAddr in Go 1.27.3.
    {
      "münchen.test" => { "xn--mnchen-3ya.test" => false, "sub.xn--mnchen-3ya.test" => false,
                          "xn--bcher-kva.test" => true },
      ".MÜNCHEN.test" => { "xn--mnchen-3ya.test" => true, "a.xn--mnchen-3ya.test" => false },
      "*.bücher.test:443" => { "a.xn--bcher-kva.test" => false, "a.xn--bcher-kva.test:8443" => true },
      "#{[0x4ED6, 0x4EEC, 0x4E3A, 0x4EC0, 0x4E48, 0x4E0D, 0x8BF4, 0x4E2D, 0x6587].pack('U*')}.test" =>
        { "xn--ihqwcrb4cv8a8dqg056pqjye.test" => false },
      # Decomposed and fullwidth forms map as Go's idna.Lookup does.
      "mu\u0308nchen.test" => { "xn--mnchen-3ya.test" => false },
      "m\u00FCnchen\u3002test" => { "xn--mnchen-3ya.test" => false },
      "\uFF2D\u00DCNCHEN.test" => { "xn--mnchen-3ya.test" => false }
    }.each do |no_proxy, targets|
      targets.each do |target, proxied|
        it "#{proxied ? 'proxies' : 'bypasses'} #{target} with NO_PROXY=#{no_proxy}" do
          env = { "HTTPS_PROXY" => "proxy", "NO_PROXY" => no_proxy }
          expect(resolve(env, "https://#{target}").nil?).to be(!proxied)
        end
      end
    end

    {
      "http://münchen.test:3128" => ["xn--mnchen-3ya.test", 3128],
      "MÜNCHEN.test" => ["xn--mnchen-3ya.test", 80],
      "http://user:p@ss@proxy.bücher.de" => ["proxy.xn--bcher-kva.de", 80],
      "http://mu\u0308nchen.test" => ["xn--mnchen-3ya.test", 80],
      "b\u00FCcher\u3002de:8080" => ["xn--bcher-kva.de", 8080]
    }.each do |value, (host, port)|
      it "dials the ASCII form of proxy host #{value}" do
        expect(resolve({ "HTTPS_PROXY" => value }, "https://example.com")).to eq(["http", host, port])
        expect(described_class.resolver(value).for(URI("https://x")).to_h.values_at(:host, :port)).to eq([host, port])
      end
    end
  end

  # Go 1.27.3 parses each of these, keeps the host as written, fails
  # idna.Lookup.ToASCII (STD3 rules or invalid UTF-8) and then fails the dial.
  # It never connects to another host or port, and never goes direct unless
  # NO_PROXY or a loopback target says so.
  describe "proxy hosts that Go's IDNA rejects" do
    [
      "http://proxy\uFF20evil.test:3128", "proxy\uFF20evil.test:3128", "http://relay.test\uFF0Fedge:3128",
      "http://proxy\uFF1A9999", "http://proxy\uFF03x:3128", "http://pro\u3000xy:3128",
      "http://pro\uFF3Fxy.test:3128", "http://b\u00FCcher.pro_xy:3128", "http://pr\xFFoxy:3128",
      # Go deletes these or fails their validity checks; the mapping here
      # does not reproduce that, so they fail closed instead of naming
      # another host.
      "http://a\u00ADb\u00FC.example:3128", "http://a\uFE0Fb\u00FC.example", "http://a\u200Db\u00FC.example",
      "http://a\u05D0.example:3128", "http://\u0661\u0662.example", "http://\uFE63b\u00FC.example",
      "http://b\u00FC-.example"
    ].each do |value|
      it "fails #{value.inspect} from the environment and rejects it explicitly" do
        expect { resolve({ "HTTPS_PROXY" => value }, "https://example.com") }
          .to raise_error(described_class::Unusable) { |error| expect(error.reason).to eq(:invalid_address) }
        expect(resolve({ "HTTPS_PROXY" => value, "NO_PROXY" => "example.com" }, "https://example.com")).to be_nil
        expect(resolve({ "HTTPS_PROXY" => value }, "https://localhost")).to be_nil
        expect { resolve({ "HTTP_PROXY" => value, "REQUEST_METHOD" => "GET", "NO_PROXY" => "*" }) }
          .to raise_error(described_class::Unusable) { |error| expect(error.reason).to eq(:cgi) }
        expect { described_class.resolver(value) }.to raise_error(ArgumentError, "Invalid proxy configuration")
      end
    end

    it "keeps an ASCII host as written, as Go skips IDNA for it" do
      expect(resolve({ "HTTPS_PROXY" => "http://pro_xy.test:3128" }, "https://example.com"))
        .to eq(["http", "pro_xy.test", 3128])
    end

    it "accepts values in any encoding without raising" do
      utf16 = "\xFF".dup.force_encoding(Encoding::UTF_16LE)
      [utf16, utf16.dup.freeze, "proxy.test".encode(Encoding::UTF_16LE)].each do |value|
        expect { described_class.resolver(value) }.to raise_error(ArgumentError, "Invalid proxy configuration")
      end
      expect { resolve({ "HTTPS_PROXY" => utf16 }, "https://example.com") }.to raise_error(described_class::Unusable)
      expect(resolve({ "HTTPS_PROXY" => "proxy", "NO_PROXY" => utf16 }, "https://example.com"))
        .to eq(["http", "proxy", 80])
    end
  end

  # Go keeps such an entry as written, so it never matches an ASCII host.
  describe "NO_PROXY entries that Go's IDNA rejects" do
    {
      "bad\xFF.test, good.test" => { "good.test" => false, "xn--bad-7t7s.test" => true, "other.test" => true },
      "bad\uFF20.test, good.test" => { "good.test" => false, "bad.test" => true },
      "b\u00FCcher.pro_xy, pro_xy.test" => { "pro_xy.test" => false, "a.xn--bcher-kva.pro_xy" => true },
      # NFKC maps U+2024 and U+FE52 to ".", which would name another domain.
      "corp\u2024example.com, good.test" => { "good.test" => false, "corp.example.com" => true,
                                              "www.corp.example.com" => true },
      "example\uFE52com" => { "example.com" => true, "www.example.com" => true },
      # Characters Go deletes (soft hyphen) or rejects drop the entry.
      "ex\u00ADample.com, good.test" => { "good.test" => false, "example.com" => true },
      "\u05D0.test" => { "xn--4db.test" => true }
    }.each do |no_proxy, targets|
      targets.each do |target, proxied|
        it "#{proxied ? 'proxies' : 'bypasses'} #{target} with NO_PROXY=#{no_proxy.inspect}" do
          env = { "HTTPS_PROXY" => "proxy", "NO_PROXY" => no_proxy }
          expect(resolve(env, "https://#{target}").nil?).to be(!proxied)
        end
      end
    end
  end

  describe "IPv4-mapped NO_PROXY networks" do
    # Expectations match http.ProxyFromEnvironment in Go 1.27.3.
    {
      "::ffff:10.0.0.0/120" => { "10.0.0.5" => false, "10.0.1.5" => true, "[::ffff:10.0.0.6]" => false,
                                 "[::1:5]" => true },
      "::ffff:0:0/96" => { "192.168.4.4" => false, "[2001:db8::1]" => true },
      "::ffff:10.0.0.0/88" => { "10.0.0.5" => true }
    }.each do |no_proxy, targets|
      targets.each do |target, proxied|
        it "#{proxied ? 'proxies' : 'bypasses'} #{target} with NO_PROXY=#{no_proxy}" do
          expect(resolve({ "HTTPS_PROXY" => "proxy", "NO_PROXY" => no_proxy }, "https://#{target}").nil?)
            .to be(!proxied)
        end
      end
    end
  end

  describe "CGI guard" do
    it "refuses a parsed HTTP proxy for an http target before NO_PROXY" do
      env = { "HTTP_PROXY" => "http://10.1.2.3:8080", "REQUEST_METHOD" => "GET", "NO_PROXY" => "*" }
      expect { resolve(env) }.to raise_error(described_class::Unusable) { |error| expect(error.reason).to eq(:cgi) }
      expect { resolve(env.merge("NO_PROXY" => "")) }.to raise_error(described_class::Unusable)
    end

    it "refuses any non-empty HTTP proxy value, even an invalid one, and ignores CGI for explicit proxies" do
      # Go 1.27.3 also refuses http://[bad, which it parses with host "http:".
      # Deliberate difference: it goes direct for u:p%zz, which it cannot parse.
      ["http://[bad", "http://u:p%zz@proxy.test"].each do |value|
        expect { resolve({ "HTTP_PROXY" => value, "REQUEST_METHOD" => "GET" }) }
          .to raise_error(described_class::Unusable) { |error| expect(error.reason).to eq(:cgi) }
      end
      expect(described_class.resolver("http://proxy.test:1", "REQUEST_METHOD" => "GET").for(URI("http://x")).host)
        .to eq("proxy.test")
    end
  end

  # Every value either names a valid endpoint or fails closed. "Go" names what
  # http.ProxyFromEnvironment and the transport's dial do in Go 1.27.3; rows
  # where it differs are deliberate.
  describe "proxy value parsing" do
    invalid = {
      "http://u:p w@proxy:3128" => "Go dials http:80",
      "http://u:p\"w@proxy:3128" => "Go dials http:80",
      "http://u:p|w@proxy:3128" => "Go dials http:80",
      "http://u:p^w@proxy:3128" => "Go dials http:80",
      "http://u:p{w}@proxy:3128" => "Go dials http:80",
      "http://u:p<w>@proxy:3128" => "Go dials http:80",
      "http://u:p\\w@proxy:3128" => "Go dials http:80",
      "http://u:p`w@proxy:3128" => "Go dials http:80",
      "http://u:p[w]@proxy:3128" => "Go dials http:80",
      "http://proxy:3128 " => "Go dials http:80",
      "http://proxy:3128 # comment" => "Go dials http:80",
      "http://proxy.test:3128/a b" => "Go dials proxy.test:3128",
      "http://proxy.test:3128/#a b" => "Go dials proxy.test:3128",
      "http://[fe80::1%25eth0]:3128" => "Go dials the scoped address",
      "http://[bad" => "Go dials http:80",
      "https://proxy.test:bad" => "Go dials https:80",
      "mailto:demo:synthetic-secret?broken" => "Go dials an empty host",
      "http://b%C3%BCcher.test:3128" => "Go dials xn--bcher-kva.test:3128",
      "http://%70roxy.test:3128" => "Go dials http:80",
      "http://u:p%zz@proxy.test" => "Go goes direct",
      "http://proxy\u2024corp.example:3128" => "Go's dial fails",
      "http://proxy\uFE52corp.example:3128" => "Go's dial fails",
      "http://proxy\u2488corp.example:3128" => "Go's dial fails",
      "http://proxy\u33C2corp.example:3128" => "Go's dial fails",
      "http://" => "Go dials http:80",
      "http://proxy.test:3128:1" => "Go dials http:80"
    }
    invalid.each do |value, go|
      it "fails #{value.inspect} closed (#{go})" do
        expect { resolve({ "HTTPS_PROXY" => value }, "https://example.com") }
          .to raise_error(described_class::Unusable) { |error| expect(error.reason).to eq(:invalid_address) }
        expect(resolve({ "HTTPS_PROXY" => value, "NO_PROXY" => "example.com" }, "https://example.com")).to be_nil
        expect { described_class.resolver(value) }.to raise_error(ArgumentError, "Invalid proxy configuration")
      end
    end

    # [scheme, host, port, user, password], the same from the environment and
    # explicitly. Go 1.27.3 resolves each to the same proxy.
    {
      "http://proxy.test:3128/a|b" => ["http", "proxy.test", 3128, nil, nil],
      "http://proxy.test:3128/?%zz" => ["http", "proxy.test", 3128, nil, nil],
      "http://proxy.test/\u00FC" => ["http", "proxy.test", 80, nil, nil],
      "file://localhost" => ["file", "localhost", nil, nil, nil],
      "mailto://proxy.test" => ["mailto", "proxy.test", nil, nil, nil],
      "http://user@proxy.test" => ["http", "proxy.test", 80, "user", nil],
      "http://:@proxy.test" => ["http", "proxy.test", 80, "", ""],
      "http://[::1]:3128" => ["http", "::1", 3128, nil, nil],
      "http://[::ffff:1.2.3.4]" => ["http", "::ffff:1.2.3.4", 80, nil, nil],
      "proxy.test:3128/path" => ["http", "proxy.test", 3128, nil, nil],
      # Go keeps the case as written; DNS ignores it.
      "HTTP://Proxy.Test:08080" => ["http", "proxy.test", 8080, nil, nil]
    }.each do |value, expected|
      it "parses #{value.inspect} as #{expected.inspect}" do
        from_env = described_class.resolver(:environment, "HTTPS_PROXY" => value).for(URI("https://x"))
        explicit = described_class.resolver(value).for(URI("https://x"))
        [from_env, explicit].each do |endpoint|
          expect(endpoint.to_h.values_at(:scheme, :host, :port, :user, :password)).to eq(expected)
        end
      end
    end

    it "raises only the fixed ArgumentError, without the value or a cause, for invalid explicit values" do
      values = invalid.keys + [
        URI::HTTP.new("http", "user:hunter2%", "proxy.test", 3128, nil, "/", nil, nil, nil),
        URI::HTTP.new("http", "user:hunter2 x", "proxy.test", 3128, nil, "/", nil, nil, nil),
        URI::Generic.new("mailto", nil, nil, nil, nil, nil, "demo:hunter2?broken", nil, nil),
        "http://user:hunter2@proxy.test".encode(Encoding::UTF_16LE)
      ]
      values.each do |value|
        expect { described_class.resolver(value) }.to raise_error do |error|
          expect([error.class, error.message, error.cause]).to eq([ArgumentError, "Invalid proxy configuration", nil])
        end
      end
    end
  end

  describe "numeric shorthand IPv4 hosts" do
    # The C resolver reads these as 8.0.0.1 or 127.0.0.1; Go refuses them.
    %w[010.0.0.1 2130706433 127.1 0x7f.1 0].each do |host|
      it "fails #{host} closed from the environment and rejects it explicitly" do
        value = "http://user:pw@#{host}:3128"
        expect { resolve({ "HTTPS_PROXY" => value }, "https://example.com") }
          .to raise_error(described_class::Unusable) { |error| expect(error.reason).to eq(:invalid_address) }
        expect { described_class.resolver(value) }.to raise_error(ArgumentError, "Invalid proxy configuration")
      end
    end

    it "keeps canonical IP literals" do
      expect(resolve({ "HTTPS_PROXY" => "http://10.0.0.1:3128" },
                     "https://example.com")).to eq(["http", "10.0.0.1", 3128])
      expect(resolve({ "HTTPS_PROXY" => "http://[::1]:3128" }, "https://example.com")).to eq(["http", "::1", 3128])
    end
  end

  describe "explicit URI options" do
    it "rejects a URI whose components would reshape its authority once serialized" do
      [
        URI::HTTP.new("http", nil, "intended.test@different.test", 3128, nil, "/", nil, nil, nil),
        URI::HTTP.new("http", nil, "different.test/path", 3128, nil, "/", nil, nil, nil),
        URI::HTTP.new("http", "user", "different.test:9", 3128, nil, "/", nil, nil, nil),
        URI::HTTP.new("http", nil, "proxy.test".encode(Encoding::UTF_16LE), 3128, nil, "/", nil, nil, nil)
      ].each do |uri|
        expect { described_class.resolver(uri) }.to raise_error do |error|
          expect([error.class, error.message, error.cause]).to eq([ArgumentError, "Invalid proxy configuration", nil])
        end
      end
    end

    it "accepts a URI whose text parses back to the same components" do
      endpoint = described_class.resolver(URI("HTTP://user:p%40ss@Proxy.Test:3128/x")).for(URI("https://x"))
      expect(endpoint.to_h).to eq(scheme: "http", host: "proxy.test", port: 3128, user: "user", password: "p@ss")
    end
  end

  describe "errors raised while another exception is handled" do
    def ambient
      raise "ambient"
    rescue RuntimeError
      yield
    end

    it "attaches no cause to the fixed configuration error or to Unusable" do
      cgi = { "HTTP_PROXY" => "p", "REQUEST_METHOD" => "GET" }
      [-> { described_class.resolver("http://proxy.test:0") }, -> { described_class.resolver(42) },
       -> { described_class.resolver(:environment, { "HTTPS_PROXY" => "http://:0" }).for(URI("https://x")) },
       -> { described_class.resolver(:environment, cgi).for(URI("http://x")) }]
        .each do |call|
          error = ambient do
            call.call
          rescue StandardError => e
            e
          end
          expect(error).to be_a(ArgumentError).or be_a(described_class::Unusable)
          expect(error.cause).to be_nil
        end
    end
  end

  describe "proxy addresses" do
    %w[http://proxy.test:0 http://proxy.test:99999].each do |value|
      it "fails an environment proxy #{value} instead of going direct" do
        expect { resolve({ "HTTPS_PROXY" => value }, "https://example.com") }
          .to raise_error(described_class::Unusable) { |error| expect(error.reason).to eq(:invalid_address) }
        expect { resolve({ "HTTPS_PROXY" => value, "NO_PROXY" => "example.com" }, "https://example.com") }
          .not_to raise_error
      end

      it "rejects an explicit proxy #{value} without echoing it" do
        expect { described_class.resolver(value) }.to raise_error(ArgumentError, "Invalid proxy configuration")
      end
    end

    it "rejects an explicit URI without a host and unsupported option types" do
      [URI("http://:8080"), URI("proxy.test:3128"), 42, :other].each do |value|
        expect { described_class.resolver(value) }.to raise_error(ArgumentError, "Invalid proxy configuration")
      end
    end

    it "percent-decodes credentials without form decoding and hides them from inspect and pp" do
      resolver = described_class.resolver("http://us%3Aer:p%40ss+w%2B@proxy.test:3128")
      endpoint = resolver.for(URI("https://x"))
      expect([endpoint.user, endpoint.password]).to eq(["us:er", "p@ss+w+"])
      shown = [endpoint.inspect, endpoint.to_s, PP.pp(endpoint, +""), PP.pp([endpoint], +""), PP.pp(resolver, +"")]
      expect(shown.join).not_to include("ss", "us")
      expect(described_class.resolver("http://proxy.test").for(URI("https://x")).user).to be_nil
    end

    # Go splits userinfo at the last "@" (go.dev/issue/22655); expectations
    # match http.ProxyFromEnvironment in Go 1.27.3.
    {
      "http://user:p@ss@proxy.test:8080" => ["proxy.test", 8080, "user", "p@ss"],
      "user:p@ss@proxy.test:8080" => ["proxy.test", 8080, "user", "p@ss"],
      "http://u@ser:p@ss@proxy.test" => ["proxy.test", 80, "u@ser", "p@ss"],
      "http://us%40er:p%2540@proxy.test:1/path?q=a@b" => ["proxy.test", 1, "us@er", "p%40"]
    }.each do |value, expected|
      it "splits #{value} at the last @ from the environment and explicitly" do
        from_env = described_class.resolver(:environment, "HTTPS_PROXY" => value).for(URI("https://x"))
        explicit = described_class.resolver(value).for(URI("https://x"))
        [from_env, explicit].each do |endpoint|
          expect(endpoint.to_h.values_at(:host, :port, :user, :password)).to eq(expected)
        end
      end
    end

    it "explicitly proxies every target, while nil never proxies" do
      explicit = described_class.resolver(URI("http://proxy.test:3128"), "NO_PROXY" => "*")
      direct = described_class.resolver(nil, "HTTPS_PROXY" => "proxy.test")
      %w[https://localhost http://127.0.0.1 https://example.com].each do |url|
        expect(explicit.for(URI(url)).host).to eq("proxy.test")
        expect(direct.for(URI(url))).to be_nil
      end
    end
  end
end
