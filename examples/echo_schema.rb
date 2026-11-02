# frozen_string_literal: true

require "connectrpc/bindings/google_protobuf"
require "google/protobuf/timestamp_pb"

module EchoExample
  pool = Google::Protobuf::DescriptorPool.generated_pool
  pool.add_serialized_file(File.binread(File.expand_path("../spec/fixtures/echo.bin", __dir__)))
  Message = pool.lookup("example.v1.EchoMessage").msgclass
  ECHO = ConnectRPC::Method.new(service: "example.v1.EchoService", name: "Echo", request_type: Message,
                                response_type: Message)
  FAIL = ConnectRPC::Method.new(service: ECHO.service, name: "Fail", request_type: Message, response_type: Message)
  SERVICE = ConnectRPC::Service.new(name: ECHO.service, methods: [ECHO, FAIL])
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new
end
