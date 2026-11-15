# frozen_string_literal: true

require "connectrpc"

module DemoExample
  # An instrumentation subscriber that prints one line per RPC, describing what
  # actually arrived at the server:
  #
  #   served Greet request-id=1f2e3d4c protocol=connect http=POST codec=json encoding=gzip code=OK
  #
  # Pass it to a server with `instrumentation: DemoExample.instrumentation`. The
  # request ID is the `demo-request-id` metadata the example clients send, so
  # example:verify can match each client call to its server line.
  class CallLog
    # The server calls start once per RPC. The returned observer receives
    # processing_finished with the outcome once the handler has run.
    def start(call) = Observer.new(call)

    Observer = Struct.new(:call) do
      def processing_finished(outcome)
        request = call.carrier # A snapshot of the request headers.
        http = http_method(outcome)
        headers = http == "GET" ? outcome.response_metadata : request
        $stdout.write("served #{call.method&.name} request-id=#{first(request, 'demo-request-id')} " \
                      "protocol=#{call.protocol} http=#{http} codec=#{codec(headers)} " \
                      "encoding=#{encoding(headers)} code=#{outcome.code || '-'}\n") # One write keeps lines whole.
      end

      private

      # The method is inferred from the headers: only a Connect GET reaches a
      # handler without a content-type. A request rejected before any handler
      # ran has no code and prints "-".
      #
      # A GET carries its codec and compression in the query string, which this
      # observer cannot see. For GET it reports the codec and encoding of the
      # response instead, which the server chose from that query and the
      # client's Accept-Encoding.
      def http_method(outcome)
        return "-" unless outcome.code

        call.carrier.key?("content-type") ? "POST" : "GET"
      end

      def first(headers, name) = headers.fetch(name, ["-"]).first

      def codec(headers)
        case first(headers, "content-type")
        when /json\z/ then "json"
        when /proto\z/, "application/grpc" then "proto"
        else "unknown"
        end
      end

      # Connect unary, Connect streaming and gRPC each name compression differently.
      def encoding(headers)
        values = headers.values_at("content-encoding", "connect-content-encoding", "grpc-encoding").compact
        values.first&.first || "identity"
      end
    end
  end

  def self.instrumentation = ConnectRPC::Instrumentation.new(subscribers: [CallLog.new])
end
