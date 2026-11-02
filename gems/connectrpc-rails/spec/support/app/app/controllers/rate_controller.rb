# frozen_string_literal: true

class RateController < EchoController
  rate_limit to: 0, within: 60, store: ActiveSupport::Cache::MemoryStore.new
end
