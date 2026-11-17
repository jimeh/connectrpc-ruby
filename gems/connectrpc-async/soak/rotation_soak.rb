# frozen_string_literal: true

require_relative "support/scenarios/rotation"

RSpec.describe "Async Host lifecycle campaign: mTLS provider rotation" do
  include_examples "soak tls rotation", :host
end
