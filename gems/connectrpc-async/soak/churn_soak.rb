# frozen_string_literal: true

require_relative "support/scenarios/churn"

RSpec.describe "Async Host lifecycle campaign: churn" do
  include_examples "soak churn and reuse", :host
end
