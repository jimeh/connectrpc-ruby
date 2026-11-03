# frozen_string_literal: true

require_relative "support/streaming_examples"

require_relative "support/streaming_bounds"

RSpec.describe "Async native Connect streaming" do
  def build_stream_host(target, tls, scheme)
    host = ConnectRPC::Async::Host.new(server: target, tls: tls).start
    [host, "#{scheme}://127.0.0.1:#{host.port}", nil]
  end

  include_examples "native streaming matrix"
  include_examples "generated mixed streaming client"
  include_examples "native streaming bounds and cancellation"
end
