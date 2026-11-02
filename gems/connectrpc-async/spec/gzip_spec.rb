# frozen_string_literal: true

require_relative "support/gzip_examples"

RSpec.describe "HTTP/2 gzip core integration" do
  include_examples "HTTP/2 gzip core integration"
end
