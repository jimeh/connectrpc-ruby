# frozen_string_literal: true

class BadStreamRequestInterceptor
  def client_stream(call)
    call.request = "replaced"
    yield
  end
end
