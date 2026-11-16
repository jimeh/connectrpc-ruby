# frozen_string_literal: true

require_relative "support/scenarios/failures"

RSpec.describe "Async Host lifecycle campaign: peer failure and shutdown" do
  include_examples "soak peer failure and shutdown", :host
end
