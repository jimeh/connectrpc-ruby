# frozen_string_literal: true

scenarios = File.expand_path("../../connectrpc-async/soak/support/scenarios", __dir__)
%w[churn drains streams failures rotation keepalive].each { |name| require File.join(scenarios, name) }

RSpec.describe "Falcon HTTP/2 lifecycle campaign" do
  include_examples "soak churn and reuse", :falcon
  include_examples "soak goaway drains", :falcon
  include_examples "soak stream retirement", :falcon
  include_examples "soak peer failure and shutdown", :falcon
  include_examples "soak tls rotation", :falcon
  include_examples "soak server keepalive", :falcon
end
