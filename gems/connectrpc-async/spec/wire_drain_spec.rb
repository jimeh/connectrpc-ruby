# frozen_string_literal: true

require_relative "support/wire_drain_examples"

RSpec.describe "HTTP/2 wire drain ownership" do
  include_examples "HTTP/2 wire drain ownership"
end
