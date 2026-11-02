# frozen_string_literal: true

require "open3"
require "rbconfig"
require "stringio"
require "tmpdir"
require_relative "../conformance/client"
require_relative "../conformance/server"

RSpec.describe "official conformance adapters" do
  def instruction(**fields)
    values = { test_name: "adapter test", http_version: :HTTP_VERSION_1,
               protocol: :PROTOCOL_CONNECT, codec: :CODEC_PROTO,
               compression: :COMPRESSION_IDENTITY, host: "127.0.0.1", port: 1,
               service: Conformance::SERVICE_NAME, method: "Unary",
               stream_type: :STREAM_TYPE_UNARY,
               request_messages: [Conformance.pack(Conformance::UnaryRequest.new)] }
    Conformance::ClientCompatRequest.new(values.merge(fields))
  end

  def with_server(config = Conformance::ServerCompatRequest.new(protocol: :PROTOCOL_CONNECT,
                                                                http_version: :HTTP_VERSION_1))
    host = EchoExample::Localhost.new(Conformance::ServerAdapter.new(config).app)
    yield host.url
  ensure
    host&.close
  end

  it "loads official ordered descriptors with optional zero presence and nested Any JSON roundtrip" do
    expect(Conformance::DESCRIPTORS.map(&:name).select { |name| Conformance::FILES.include?(name) }).to eq(Conformance::FILES)
    absent = instruction
    present = instruction(timeout_ms: 0, cancel: { after_close_send_ms: 5 })
    expect(absent.has_timeout_ms?).to be(false)
    expect(present.has_timeout_ms?).to be(true)
    json = Conformance::ClientCompatRequest.encode_json(present)
    result = Conformance::ClientCompatRequest.decode_json(json)
    expect(result).to eq(present)
    expect(result.cancel.after_close_send_ms).to eq(5)
    expect(result.request_messages.first.type_url).to end_with(".UnaryRequest")
  end

  it "roundtrips bounded four-byte big-endian frames, drains EOF and rejects malformed frames" do
    output = StringIO.new("".b)
    message = instruction
    Conformance::Framing.write(output, message)
    expect(output.string.byteslice(0, 4).unpack1("N")).to eq(message.to_proto.bytesize)
    output.rewind
    expect(Conformance::Framing.read(output, Conformance::ClientCompatRequest)).to eq(message)
    expect(Conformance::Framing.read(output, Conformance::ClientCompatRequest)).to be_nil
    ["\x00".b, "#{[9].pack('N')}x", [Conformance::Framing::MAX_SIZE + 1].pack("N")].each do |bytes|
      expect do
        Conformance::Framing.read(StringIO.new(bytes),
                                  Conformance::ClientCompatRequest)
      end.to raise_error(RuntimeError, /Truncated|Oversized/)
    end
    expect { Conformance::Framing.read(StringIO.new([1].pack("N") + "\xff".b), Conformance::ClientCompatRequest) }
      .to raise_error(Google::Protobuf::ParseError)
  end

  it "maps actual proto and JSON success, repeated metadata, deliberate errors and receive limits" do
    with_server do |url|
      uri = URI(url)
      %i[CODEC_PROTO CODEC_JSON].each do |codec|
        message = Conformance::UnaryRequest.new(response_definition: {
                                                  response_data: "hello",
                                                  response_headers: [{ name: "X-Repeat", value: %w[a b] }],
                                                  response_trailers: [{ name: "x-tail", value: ["one,two"] }]
                                                })
        request = instruction(host: uri.host, port: uri.port, codec: codec,
                              request_messages: [Conformance.pack(message)],
                              request_headers: [{ name: "X-Request", value: ["one,two"] },
                                                { name: "x-request", value: ["three"] }])
        result = Conformance::ClientAdapter.new.perform(request)
        expect(result.error).to be_nil
        expect(result.response.error).to be_nil
        payload = result.response.payloads.first
        expect(payload.data).to eq("hello")
        expect(payload.request_info.has_timeout_ms?).to be(false)
        echoed = payload.request_info.request_headers.find { |header| header.name == "x-request" }
        expect(echoed.value.to_a).to eq(["one,two, three"])
        expect(result.response.response_trailers.find do |header|
          header.name == "x-tail"
        end.value.to_a).to eq(["one,two"])
        request.message_receive_limit = 1
        limited = Conformance::ClientAdapter.new.perform(request)
        expect(limited.response.error.code).to eq(:CODE_RESOURCE_EXHAUSTED)
        request.message_receive_limit = 0
        message.response_definition = Conformance::UnaryResponseDefinition.new(error: {
                                                                                 code: :CODE_INVALID_ARGUMENT,
                                                                                 message: "deliberate",
                                                                                 details: [Conformance.pack(Conformance::UnaryRequest.new)]
                                                                               })
        request.request_messages[0] = Conformance.pack(message)
        error = Conformance::ClientAdapter.new.perform(request).response.error
        expect(error.code).to eq(:CODE_INVALID_ARGUMENT)
        expect(error.message).to eq("deliberate")
        expect(error.details.map(&:type_url)).to eq(["type.googleapis.com/#{Conformance::UnaryRequest.descriptor.name}",
                                                     "type.googleapis.com/#{Conformance::RequestInfo.descriptor.name}"])
      end
    end
  end

  it "rejects unsupported setup distinctly from RPC errors and validates message types" do
    [%i[protocol PROTOCOL_GRPC], %i[http_version HTTP_VERSION_2], [:use_get_http_method, true]].each do |field, value|
      response = Conformance::ClientAdapter.new.perform(instruction(**{ field => value }))
      expect(response.response).to be_nil
      expect(response.error.message).to include("Unsupported client instruction")
    end
    request = instruction(request_messages: [Conformance.pack(Conformance::UnaryResponse.new)])
    expect(Conformance::ClientAdapter.new.perform(request).error.message).to include("Unexpected request type")
  end

  it "writes framed results for every input and exits promptly on EOF" do
    with_server do |url|
      uri = URI(url)
      Open3.popen3(RbConfig.ruby, "-Ilib", "conformance/client.rb") do |input, output, error, process|
        2.times do |index|
          Conformance::Framing.write(input, instruction(test_name: "EOF #{index}", host: uri.host, port: uri.port))
        end
        input.close
        2.times do |index|
          expect(output.wait_readable(5)).to be_truthy
          result = Conformance::Framing.read(output, Conformance::ClientCompatResponse)
          expect(result.test_name).to eq("EOF #{index}")
          expect(result.error).to be_nil
          expect(result.response.payloads.size).to eq(1)
        end
        expect(process.join(5)).to be_truthy
        expect(process.value).to be_success
        expect(error.read).to eq("")
        expect(output.read).to eq("")
      ensure
        if process.alive?
          Process.kill("TERM", process.pid)
          raise "Client failed to stop" unless process.join(5)
        end
      end
    end
  end

  it "serves real RPCs after configuration EOF and shuts down cleanly on SIGTERM" do
    Dir.mktmpdir("conformance-spec-") do |temporary|
      telemetry = File.join(temporary, "telemetry.jsonl")
      Open3.popen3({ "CONFORMANCE_TELEMETRY" => telemetry }, RbConfig.ruby, "-Ilib",
                   "conformance/server.rb") do |input, output, error, process|
        Conformance::Framing.write(input,
                                   Conformance::ServerCompatRequest.new(protocol: :PROTOCOL_CONNECT,
                                                                        http_version: :HTTP_VERSION_1))
        input.close
        expect(output.wait_readable(5)).to be_truthy
        config = Conformance::Framing.read(output, Conformance::ServerCompatResponse)
        # Poll the actual EOF event under a bounded deadline before invoking the
        # RPC. Thread.pass yields while the child runs, without a readiness sleep.
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
        observed = []
        loop do
          observed = if File.exist?(telemetry)
                       File.readlines(telemetry).filter_map do |line|
                         JSON.parse(line)["event"]
                       end
                     else
                       []
                     end
          break if observed.include?("server_stdin_eof")
          raise "No EOF event: #{observed.inspect}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

          Thread.pass
        end
        client = ConnectRPC::Client.new(base_url: "http://#{config.host}:#{config.port}", binding: Conformance::BINDING)
        message = Conformance::UnaryRequest.new
        response = client.call(Conformance::UNARY, message, timeout: nil)
        expect(response.message.payload.request_info.has_timeout_ms?).to be(false)
        expect(response.message.payload.request_info.requests.first).to eq(Conformance.pack(message))
        Process.kill("TERM", process.pid)
        expect(process.join(5)).to be_truthy
        expect(process.value).to be_success
        expect(error.read).to eq("")
        expect(output.read).to eq("")
        expect(File.readlines(telemetry).map { |line| JSON.parse(line)["event"] }.last).to eq("server_stopped")
      ensure
        if process.alive?
          Process.kill("TERM", process.pid)
          raise "Server failed to stop" unless process.join(5)
        end
      end
    end
  end
end
