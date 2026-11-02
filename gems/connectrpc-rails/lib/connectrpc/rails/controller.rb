# frozen_string_literal: true

module ConnectRPC
  module Rails
    module Controller
      extend ActiveSupport::Concern

      # Only this method is a Rails action. The generated handler map stays private.
      def connect
        handler = @connect_handlers[connect_method.path]
        raise Error.new(:unimplemented, "Unknown method") unless handler

        @connect_result = handler.call(connect_request, connect_context)
        @connect_result_set = true
        self.response_body = ""
      end

      private

      attr_reader :connect_request, :connect_context, :connect_method

      def connect_service = raise NotImplementedError, "Implement private connect_service"
      def connect_binding = raise NotImplementedError, "Implement private connect_binding"
      def connect_handlers = raise NotImplementedError, "Implement private connect_handlers"
      def connect_limits = {}

      def process_action(name, ...)
        return super unless name.to_s == "connect"

        @connect_handlers = connect_handlers
        connect_dispatch do |rpc_method, message, context|
          @connect_method = rpc_method
          @connect_request = message
          @connect_context = context
          connect_prepare_request
          connect_run_lifecycle { super(name, ...) }
        end
      end

      def connect_dispatch(&invoker)
        service = connect_service
        env = connect_env(service)
        server = Server.new(services: [service], binding: connect_binding, handlers: @connect_handlers,
                            invoker: invoker, **connect_limits)
        connect_apply_response(ConnectRPC::Rack.new(server: server).call(env))
      end

      def connect_env(service)
        prefix = connect_route_prefix(service)
        raw_path = request.get_header("PATH_INFO")
        boundary = "#{prefix}/#{service.name}"
        unless raw_path == boundary || raw_path.start_with?("#{boundary}/")
          raise ArgumentError, "Connect route does not match the configured service"
        end

        request.env.merge("PATH_INFO" => raw_path.delete_prefix(prefix))
      end

      def connect_route_prefix(service)
        # Use the matched route's literal prefix, never decoded wildcard parameters.
        pattern = request.route_uri_pattern.to_s
        match = %r{\A([^:*()]*)/#{Regexp.escape(service.name)}(?=/|\(|\z)}.match(pattern)
        raise ArgumentError, "Connect route must name the configured service after a literal scope" unless match

        match[1]
      end

      def connect_prepare_request
        request.request_parameters = ActiveSupport::HashWithIndifferentAccess.new
        request.formats = [:json]
      end

      def connect_run_lifecycle
        yield
        raise connect_status_error(response.status) unless @connect_result_set && response.status == 200

        @connect_result
      rescue Error
        raise
      rescue StandardError => e
        connect_report(e)
        raise connect_status_error(ActionDispatch::ExceptionWrapper.status_code_for_exception(e.class.name))
      end

      def connect_report(error)
        return unless defined?(::Rails) && ::Rails.respond_to?(:error)

        ::Rails.error.report(error, handled: false, source: "connectrpc.rails")
      end

      def connect_status_error(status)
        # HTTP_FALLBACK is a client proxy mapping and is deliberately unsuitable here.
        codes = Error::HTTP_STATUS.select { |_code, value| value == status }.keys
        code = codes.one? ? codes.first : "internal"
        Error.new(code, code == "internal" ? "Internal error" : code.tr("_", " ").capitalize)
      end

      def connect_apply_response(result)
        status, headers, body = result
        %w[content-length content-encoding etag last-modified location].each { |key| response.delete_header(key) }
        self.status = status
        headers.each do |key, values|
          # Rails parses Content-Type as a scalar. Repeated metadata remains Rack 3 arrays.
          response.set_header(key, key == "content-type" ? values.fetch(0) : values)
        end
        self.response_body = body
      end
    end
  end
end
