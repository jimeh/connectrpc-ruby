# frozen_string_literal: true

# Throwaway: a selected job must fail and turn CI result red.
RSpec.describe "CI scope throwaway failure" do
  it "fails on purpose" do
    expect(1).to eq(2)
  end
end
