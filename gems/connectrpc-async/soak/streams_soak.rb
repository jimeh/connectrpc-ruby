# frozen_string_literal: true

require_relative "support/scenarios/streams"

RSpec.describe "Async Host lifecycle campaign: stream retirement and slow peers" do
  include_examples "soak stream retirement", :host
end
