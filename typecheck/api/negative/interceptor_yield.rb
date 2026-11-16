# frozen_string_literal: true

class BadYieldInterceptor
  def client_unary(call)
    yield(call)
  end
end
