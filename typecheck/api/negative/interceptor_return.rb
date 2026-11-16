# frozen_string_literal: true

class BadReturnInterceptor
  def client_unary(_call)
    yield
    "response"
  end
end
