# frozen_string_literal: true

class BadHeaderUse
  def self.apply(headers)
    headers.header.fetch(0)
  end
end
