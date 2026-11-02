# frozen_string_literal: true

require_relative "support/rotation_examples"

RSpec.describe "HTTP/2 TLS retirement and session isolation" do
  include_examples "HTTP/2 TLS retirement and session isolation"
end
