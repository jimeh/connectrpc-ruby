# frozen_string_literal: true

require "spec_helper"
require_relative "../../connectrpc-async/spec/support/retirement_preemption"

RSpec.describe "Falcon local completion preemption" do
  def completion_join(task, **, &)
    ConnectRPC::Falcon.const_get(:HTTP1Resource).allocate.send(:join, task, **, &)
  end

  it_behaves_like "settled completion join"
end
