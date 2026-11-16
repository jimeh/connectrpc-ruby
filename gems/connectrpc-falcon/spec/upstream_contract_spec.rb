# frozen_string_literal: true

# Each example pins one upstream behavior or hook. "Relied on" failures mean
# this package is broken on the installed stack. "Worked around" failures mean
# upstream fixed something; the description names the workaround to delete.
require_relative "../../connectrpc-async/spec/support/perturbed_require"

RSpec.describe "connectrpc-falcon upstream contract" do
  include PerturbedRequire

  def contract = ConnectRPC::Falcon.const_get(:UpstreamContract)
  def checker = AsyncHelpers::INTERNAL.const_get(:UpstreamContract)

  # Records the protocol-http1 writer's stream and body-close order.
  def write_recorded(length:)
    events = []
    stream = Object.new
    stream.define_singleton_method(:write) { |data| events << [:write, data] }
    stream.define_singleton_method(:flush) { events << :flush }
    body = Protocol::HTTP::Body::Readable.new
    chunks = %w[he llo]
    body.define_singleton_method(:length) { length }
    body.define_singleton_method(:read) { chunks.shift }
    body.define_singleton_method(:close) { |error = nil| events << [:close, error] }
    Protocol::HTTP1::Connection.new(stream, state: :open).write_body("HTTP/1.1", body)
    events << :returned
  end

  # Serves one request through async-http's HTTP/1 server loop.
  def serve_request(wire)
    reactor do |task|
      left, right = Socket.pair(:UNIX, :STREAM, 0)
      server = Async::HTTP::Protocol::HTTP1::Server.new(IO::Stream(left), "HTTP/1.1")
      observed = Async::Promise.new
      serving = task.async(finished: false) do
        server.each do |request|
          framed = request.body.body
          before = [framed.remaining, framed.length] if framed.is_a?(Protocol::HTTP1::Body::Fixed)
          before = framed.length if framed.is_a?(Protocol::HTTP1::Body::Chunked)
          read = +""
          while (chunk = request.body.read)
            read << chunk
          end
          after = framed.is_a?(Protocol::HTTP1::Body::Fixed) ? [framed.remaining, framed.length] : framed.length
          observed.resolve({ wrapper: request.body.class, framed: framed.class, before: before, after: after,
                             read: read })
          Protocol::HTTP::Response[200, {}, ["ok"]]
        end
      end
      right.write(wire)
      observed.wait
    ensure
      AsyncHelpers::INTERNAL.cancel(serving)
      AsyncHelpers::INTERNAL.join(serving)
      AsyncHelpers::INTERNAL.raw_close(left)
      AsyncHelpers::INTERNAL.raw_close(right)
    end
  end

  describe "relied on" do
    it "passes the boot-time hook checks on the installed stack" do
      expect(checker.hook_failures(contract::HOOKS)).to eq([])
    end

    it "names only tested versions that the connectrpc-async gemspec allows" do
      path = File.expand_path("../../connectrpc-async/connectrpc-async.gemspec", __dir__)
      requirements = Gem::Specification.load(path).runtime_dependencies.to_h do |dependency|
        [dependency.name, dependency.requirement]
      end
      contract::TESTED.each do |gem, version|
        expect(requirements.fetch(gem)).to be_satisfied_by(Gem::Version.new(version)), gem
      end
    end

    it "wraps a fixed-length request body in Finishable, with remaining reaching zero at complete framing" do
      observed = serve_request("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhello")
      expect(observed).to eq(wrapper: Async::HTTP::Protocol::HTTP1::Finishable, framed: Protocol::HTTP1::Body::Fixed,
                             before: [5, 5], after: [0, 5], read: "hello")
    end

    it "wraps a chunked request body in Finishable, with length known only at complete framing" do
      wire = "POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"
      observed = serve_request(wire)
      expect(observed).to eq(wrapper: Async::HTTP::Protocol::HTTP1::Finishable,
                             framed: Protocol::HTTP1::Body::Chunked, before: nil, after: 5, read: "hello")
    end

    it "closes a fixed-length response body after the full write and final flush, before write_body returns" do
      events = write_recorded(length: 5)
      expect(events.select { |event| event.is_a?(Array) && event.first == :write }.map(&:last).join)
        .to eq("content-length: 5\r\n\r\nhello")
      expect(events.last(3)).to eq([:flush, [:close, nil], :returned])
    end

    it "closes a chunked response body after the terminal chunk and final flush, before write_body returns" do
      events = write_recorded(length: nil)
      writes = events.select { |event| event.is_a?(Array) && event.first == :write }.map(&:last)
      expect(writes.join).to eq("transfer-encoding: chunked\r\n\r\n2\r\nhe\r\n3\r\nllo\r\n0\r\n\r\n")
      expect(events.last(4)).to eq([[:write, "0\r\n\r\n"], :flush, [:close, nil], :returned])
    end
  end

  describe "perturbed at require" do
    it "fails require by name when Endpoint#tcp_endpoint stops being protected" do
      output = require_perturbed("connectrpc/falcon", "Async::HTTP::Endpoint.send(:public, :tcp_endpoint)")
      expect_contract_error(output, "connectrpc-falcon", ConnectRPC::Falcon::VERSION,
                            "async-http #{Async::HTTP::VERSION}: Async::HTTP::Endpoint#tcp_endpoint expected " \
                            "protected tcp_endpoint() from Async::HTTP::Endpoint, found public tcp_endpoint() from " \
                            "Async::HTTP::Endpoint",
                            "Verified with async-http #{contract::TESTED.fetch('async-http')}.")
    end
  end

  describe "worked around" do
    it "async-http Endpoint#build_endpoint builds the TLS endpoint in the calling process " \
       "(delete Falcon::Endpoint#build_endpoint when it no longer does)" do
      endpoint = Async::HTTP::Endpoint.parse("https://localhost:0").build_endpoint
      expect(endpoint).to be_a(IO::Endpoint::SSLEndpoint)
      expect(endpoint.context).to be_a(OpenSSL::SSL::SSLContext)
    end
  end
end
