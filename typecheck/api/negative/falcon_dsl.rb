# frozen_string_literal: true

class BadFalconConfiguration
  def configure(loader)
    loader.service "invalid" do
      count "two"
    end
  end
end
