# frozen_string_literal: true

class BadReturnInterceptor
  def client_unary(_call)
    yield
    "response"
  end

  # A valid stream method does not make the invalid unary one acceptable.
  def client_stream(_call)
    yield
  end
end
