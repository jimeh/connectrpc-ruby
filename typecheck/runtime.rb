# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "service_connect"
require_relative "consumers/support"

unless Gem.loaded_specs.fetch("google-protobuf").version.to_s == "4.36.2"
  raise "Bridge runtime must be google-protobuf 4.36.2"
end

pairs = {
  RbsFixture::Api::Echo::EXCHANGE => [RbsFixture::Messages::Envelope::Request, RbsFixture::Messages::Envelope::Reply],
  RbsFixture::Api::Echo::PLAIN => [RbsPlain::V1::Request, RbsPlain::V1::Outer::Reply],
  RbsFixture::Api::Echo::RESET => [Google::Protobuf::Empty, Google::Protobuf::Empty]
}
pairs.each do |method, (request, reply)|
  raise "Official request class mismatch" unless method.request_type.equal?(request)
  raise "Official reply class mismatch" unless method.response_type.equal?(reply)
end
%w[proto json].each do |codec|
  handlers = RbsFixture::Api::Echo.handlers(
    exchange: lambda do |request, context|
      raise "Wrong concrete request" unless request.is_a?(RbsFixture::Messages::Envelope::Request)

      context.response_metadata.add("checked", "yes")
      RbsFixture::Messages::Envelope::Reply.new(count: request.text.size)
    end,
    plain: ->(request, _context) { RbsPlain::V1::Outer::Reply.new(text: request.text) },
    reset: ->(request, _context) { request }
  )
  binding = ConnectRPC::Bindings::GoogleProtobuf.new
  server = ConnectRPC::Server.new(services: [RbsFixture::Api::Echo::SERVICE], binding: binding, handlers: handlers)
  core = ConnectRPC::Client.new(base_url: "https://rbs.invalid", binding: binding,
                                transport: LoopbackTransport.new(server), codec: codec.to_sym)
  client = RbsFixture::Api::Echo::Client.new(client: core)
  request = RbsFixture::Messages::Envelope::Request.new(text: "hello", tags: ["a"], counts: { "a" => 1 })
  request.tags << "b"
  raise "RepeatedField writer return changed" unless request.tags.public_send(:[]=, 0, "first").nil?
  raise "RepeatedField append return changed" unless request.tags.<<("last").equal?(request.tags)
  raise "Map writer return changed" unless request.counts.public_send(:[]=, "b", 2) == 2

  request.counts["b"] = 2
  request.sent_at = Google::Protobuf::Timestamp.new(seconds: 1)
  response = client.exchange(request, metadata: { "caller" => ["runtime"] }, timeout: nil)
  unless response.message.is_a?(RbsFixture::Messages::Envelope::Reply) && response.message.count == 5
    raise "Reply association lost"
  end
  raise "Metadata lost" unless response.metadata["checked"] == ["yes"]
  raise "Proto2 import failed" unless client.plain(RbsPlain::V1::Request.new(text: "plain")).message.text == "plain"
  raise "WKT failed" unless client.reset(Google::Protobuf::Empty.new).message.is_a?(Google::Protobuf::Empty)

  begin
    client.exchange(RbsFixture::Messages::Envelope::Reply.new)
    raise "Wrong request accepted"
  rescue ConnectRPC::Error => e
    raise unless e.code == "invalid_argument"
  end
end
puts "Official distinct/nested/imported/ruby_package/WKT messages: generated Client and handlers " \
     "passed in proto/json (Ruby #{RUBY_VERSION})"
