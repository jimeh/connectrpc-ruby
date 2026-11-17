# frozen_string_literal: true

require_relative "support/scenarios/drains"

RSpec.describe "Async Host lifecycle campaign: GOAWAY drains" do
  include_examples "soak goaway drains", :host
end
