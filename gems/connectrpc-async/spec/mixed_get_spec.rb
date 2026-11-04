# frozen_string_literal: true

require_relative "support/mixed_get_examples"

RSpec.describe "Async mixed GET dispatch" do
  include_examples "mixed GET protocol selection", :async, :h2
end
