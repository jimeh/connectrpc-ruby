# frozen_string_literal: true

module StreamingInterop
  class HTTP1ClientProbe
    def initialize(core, task:, tls:)
      @core = core
      @task = task
      @tls = tls
    end

    def run
      check_headers(@core.call(METHODS[3], MESSAGE.new(value: "status")).metadata)
      @core.server_stream(METHODS[0], MESSAGE.new(value: "gate")) do |reader|
        check_headers(reader.headers)
        check(reader.receive.value == "first", "incremental SS")
        @core.call(METHODS[3], MESSAGE.new(value: "release"))
        check(reader.receive.value == "gate" && reader.receive.nil?, "SS EOF")
        check(reader.trailers["x-tail"] == ["done"], "SS trailers")
      end
      response = @core.client_stream(METHODS[1]) do |writer|
        writer.send_message(MESSAGE.new)
        @task.with_timeout(3) do
          loop do
            break if @core.call(METHODS[3], MESSAGE.new(value: "status")).message.value == "1"
          end
        end
        writer.send_message(MESSAGE.new(value: "two"))
      end
      check_headers(response.metadata)
      check(response.message.value == "2", "incremental CS")
      check(@core.client_stream(METHODS[1]) { nil }.message.value == "0", "zero input")
      begin
        @core.server_stream(METHODS[0], MESSAGE.new(value: "error")) { |reader| reader.each { |_| nil } }
        raise "Missing remote error"
      rescue ConnectRPC::Error => e
        check(e.code == "not_found", "remote error")
      end
      source = ConnectRPC::CancellationSource.new
      begin
        @core.server_stream(METHODS[0], MESSAGE.new(value: "gate"), timeout: nil,
                                                                    cancellation: source.token) do |reader|
          check(reader.receive.value == "first", "cancel wire progress")
          source.cancel
          reader.receive
        end
        raise "Missing cancellation"
      rescue ConnectRPC::Error => e
        check(e.code == "canceled", "cancellation")
      end
      check(@core.client_stream(METHODS[1]) { nil }.message.value == "0", "healthy after cancellation")
    end

    private

    def check_headers(headers)
      check(headers["x-observed-http"] == ["HTTP/1.1"], "actual HTTP/1.1")
      check(headers["x-observed-alpn"] == (@tls ? ["http/1.1"] : nil), "actual ALPN")
    end

    def check(value, message) = StreamingInterop.check(value, message)
  end
end
