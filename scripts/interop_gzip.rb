# frozen_string_literal: true

require "connectrpc/transports/net_http"
require "json"

module GzipInterop
  class ObservedTransport < ConnectRPC::Transports::NetHTTP
    attr_reader :request_wire, :response_wire

    def call(request)
      @request_wire = request
      @response_wire = super
      fields = { request_encoding: request.headers.fetch("content-encoding", ["identity"]).first,
                 request_bytes: request.body.bytesize,
                 response_encoding: @response_wire.headers.fetch("content-encoding", ["identity"]).first,
                 response_bytes: @response_wire.body.bytesize, status: @response_wire.status }
      puts "WIRE #{JSON.generate(fields)}"
      @response_wire
    end
  end

  def self.ruby_server
    ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING, encoding: :gzip,
                           request_limit: 1024, handlers: Interop.ruby_handlers)
  end

  # Keep each independent peer scenario and its assertions together.
  # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/BlockLength
  def self.ruby_clients(url)
    %i[proto json].each do |codec|
      observed = ObservedTransport.new
      %i[gzip identity].each do |encoding|
        client = ConnectRPC::Client.new(base_url: url, binding: EchoExample::BINDING, codec: codec, encoding: encoding,
                                        accept_encoding: [:gzip], transport: observed)
        message = EchoExample::Message.new(name: "gzip " * 40, count: 42)
        response = client.call(EchoExample::ECHO, message, metadata: { "x-request" => %w[a b] })
        Interop.check(response.message == message, "Gzip Ruby->Go typed message")
        Interop.check(response.metadata["x-echo"] == ["a, b"] && response.trailers["x-tail"] == %w[three four],
                      "Gzip Ruby->Go metadata/trailers")
        Interop.check(observed.response_wire.headers["content-encoding"] == ["gzip"], "Missing gzip response")
        decoded = EchoExample::BINDING.decode(Zlib.gunzip(observed.response_wire.body),
                                              type: EchoExample::Message, codec: codec)
        Interop.check(decoded == message, "Raw gzip response mismatch")
        if encoding == :gzip
          encoded = EchoExample::BINDING.encode(message, type: EchoExample::Message, codec: codec)
          Interop.check(Zlib.gunzip(observed.request_wire.body) == encoded, "Raw gzip request mismatch")
        end
        puts "PASS Ruby->Go #{codec} #{encoding}-gzip typed/metadata/trailers/actual-wire"
      end
      # Pinned connect-go prefers request gzip even when the caller advertises
      # identity. Assert the observed incompatibility, without changing the RPC.
      client = ConnectRPC::Client.new(base_url: url, binding: EchoExample::BINDING, codec: codec, encoding: :gzip,
                                      accept_encoding: [:identity], transport: observed)
      expect_error("internal") { client.call(EchoExample::ECHO, EchoExample::Message.new(name: "asymmetric")) }
      Interop.check(observed.response_wire.headers["content-encoding"] == ["gzip"], "Go preference quirk changed")
      puts "PASS Ruby->Go #{codec} observed reference gzip/identity preference quirk rejected safely"
      client = ConnectRPC::Client.new(base_url: url, binding: EchoExample::BINDING, codec: codec, encoding: :gzip,
                                      transport: observed)
      message = EchoExample::Message.new(name: "detail")
      error = expect_error("invalid_argument") { client.call(EchoExample::FAIL, message) }
      Interop.check(error.message == "deliberate" && error.metadata["x-error"] == %w[a b] &&
                    error.trailers["x-end"] == %w[c d],
                    "Go gzip error metadata/message")
      Interop.check(error.details.first.decode(binding: EchoExample::BINDING, type: EchoExample::Message) == message,
                    "Go gzip error typed detail")
      puts "PASS Ruby->Go #{codec} gzip error/details/metadata/trailers"
      expect_error("resource_exhausted") { client.call(EchoExample::ECHO, EchoExample::Message.new(name: "x" * 2048)) }
      puts "PASS Ruby->Go #{codec} gzip decoded request limit"
      limited = ConnectRPC::Client.new(base_url: url, binding: EchoExample::BINDING, codec: codec, encoding: :gzip,
                                       response_limit: 1, transport: observed)
      expect_error("resource_exhausted") { limited.call(EchoExample::ECHO, message) }
      puts "PASS Ruby->Go #{codec} gzip response limit"
    end
  end

  # rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/BlockLength

  def self.expect_error(code)
    yield
    raise "Expected #{code}"
  rescue ConnectRPC::Error => e
    Interop.check(e.code == code, "Expected #{code}, got #{e.code}")
    e
  end
end
