# frozen_string_literal: true

class BadWrapInterceptor
  def server_unary(call)
    call.wrap_stream { |stream| stream }
    yield
  end
end
