# frozen_string_literal: true

module ConnectRPC
  module Generator
    module StreamingMethods
      private

      def declaration_type(method)
        return method.server_streaming ? "BidiStreamMethod" : "ClientStreamMethod" if method.client_streaming

        method.server_streaming ? "ServerStreamMethod" : "Method"
      end

      def handler_type(method)
        name = declaration_type(method)
        name == "Method" ? "_Handler" : "_#{name.delete_suffix('Method')}Handler"
      end

      def render_streaming_client(method, ruby, depth)
        operation = { "BidiStreamMethod" => "bidi_stream", "ClientStreamMethod" => "client_stream",
                      "ServerStreamMethod" => "server_stream" }.fetch(declaration_type(method))
        request = method.client_streaming ? "" : "request, "
        line(depth + 1, "def #{ruby}(#{request}**options, &)")
        line(depth + 2, "@client.#{operation}(#{ruby.upcase}, #{request}**options, &)")
      end
    end
    private_constant :StreamingMethods
  end
end
