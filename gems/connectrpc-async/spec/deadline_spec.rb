# frozen_string_literal: true

require_relative "support/deadline_examples"

RSpec.describe "HTTP/2 deadlines and stream isolation" do
  include_examples "HTTP/2 deadlines and stream isolation"
end
