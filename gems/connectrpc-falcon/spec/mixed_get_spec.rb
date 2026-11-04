# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/mixed_get_examples"

RSpec.describe "Falcon mixed GET dispatch" do
  include_examples "mixed GET protocol selection", :falcon, :h2
  include_examples "mixed GET protocol selection", :falcon, :http1
end
