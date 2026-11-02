# frozen_string_literal: true

module ConnectRPC
  module Generator
    class Schema
      attr_reader :targets, :types, :ruby_types, :rbs

      def initialize(request)
        unless ["", "rbs=true"].include?(request.parameter)
          raise InvalidDescriptor, "Invalid plugin parameters #{request.parameter.inspect}; expected rbs=true or empty"
        end

        @rbs = request.parameter == "rbs=true"

        @files = unique_files(request.proto_file)
        @targets = target_files(request.file_to_generate)
        @types = {}
        @ruby_types = {}
        @symbols = {}
        @namespaces = Hash.new { |hash, key| hash[key] = [] }
        @files.each_value { |file| index_file(file) }
        targets.each { |file| validate_target(file) }
        validate_namespaces
        validate_outputs
      end

      private

      def unique_files(files)
        files.each_with_object({}) do |file, result|
          Names.path!(file.name)
          raise InvalidDescriptor, "Duplicate proto file #{file.name}" if result.key?(file.name)

          result[file.name] = file
        end
      end

      def target_files(names)
        raise InvalidDescriptor, "Duplicate generation target" unless names.uniq.size == names.size

        names.map do |name|
          @files.fetch(name) { raise InvalidDescriptor, "Missing generation target #{name.inspect}" }
        end
      end

      def symbol!(name, description)
        previous = @symbols[name]
        raise InvalidDescriptor, "Duplicate proto symbol #{name}: #{previous} and #{description}" if previous

        @symbols[name] = description
      end

      def node(parts, kind, origin, file)
        parts.each { |part| Names.constant!(part, origin) }
        @namespaces[parts] << [kind, origin, file.name]
      end

      def index_file(file)
        unless file.package.empty? || file.package.split(".", -1).all? { |part| Names::IDENTIFIER.match?(part) }
          raise InvalidDescriptor, "Invalid package #{file.package.inspect} in #{file.name}"
        end

        namespace = Names.namespace(file)
        namespace.each_index do |index|
          # Repeated declarations of the same package are legitimate.
          node(namespace.take(index + 1), :package, "package #{namespace.take(index + 1).join('::')}", file)
        end
        file.dependency.each do |name|
          raise InvalidDescriptor, "Missing dependency #{name} in #{file.name}" unless @files.key?(name)
        end
        index_types(file.message_type, file.enum_type, file.package, namespace, file)
        file.service.each do |service|
          Names.identifier!(service.name, file.name)
          full = qualified(file.package, service.name)
          symbol!(full, file.name)
          service["method"].each do |method|
            Names.identifier!(method.name, full)
            symbol!(qualified(full, method.name), file.name)
          end
        end
      end

      def index_types(messages, enums, prefix, namespace, file)
        messages.each do |message|
          Names.identifier!(message.name, file.name)
          full = qualified(prefix, message.name)
          symbol!(full, file.name)
          @types[".#{full}"] = [file, message]
          next if message.options&.map_entry

          parts = namespace + [Names.rubify(message.name)]
          @ruby_types[".#{full}"] = "::#{parts.join('::')}"
          node(parts, :message, full, file)
          index_types(message.nested_type, message.enum_type, full, parts, file)
        end
        enums.each do |enum|
          Names.identifier!(enum.name, file.name)
          full = qualified(prefix, enum.name)
          symbol!(full, file.name)
          node(namespace + [Names.rubify(enum.name)], :enum, full, file)
        end
      end

      def validate_target(file)
        unless ["", "proto2", "proto3"].include?(file.syntax) && !file.has_edition?
          raise InvalidDescriptor, "Unsupported syntax/editions in #{file.name}; use proto2 or proto3"
        end

        file.service.each do |service|
          origin = "#{file.name}: #{qualified(file.package, service.name)}"
          parts = Names.namespace(file) + [Names.rubify(service.name)]
          node(parts, :service, origin, file)
          node(parts + ["SERVICE"], :generated, "#{origin} SERVICE", file)
          node(parts + ["Client"], :generated, "#{origin} Client", file)
          methods = {}
          service["method"].each do |method|
            validate_method(method, origin)
            ruby = Names.rpc!(method.name, origin)
            if methods.key?(ruby)
              raise InvalidDescriptor, "Ruby RPC collision in #{origin}: #{methods[ruby]} and #{method.name} (#{ruby})"
            end

            methods[ruby] = method.name
            node(parts + [ruby.upcase], :generated, "#{origin}.#{method.name}", file)
          end
        end
      end

      def validate_method(method, origin)
        if method.client_streaming || method.server_streaming
          raise InvalidDescriptor, "Unsupported streaming RPC #{origin}.#{method.name}; only unary RPCs are supported"
        end

        [method.input_type, method.output_type].each do |name|
          entry = types[name]
          unless entry && !entry.last.options&.map_entry
            raise InvalidDescriptor, "Unresolved or non-message type #{name.inspect} for #{origin}.#{method.name}"
          end
        end
      end

      def validate_namespaces
        loaded = loaded_files
        loaded_names = loaded.map(&:name)
        relevant = loaded.flat_map do |file|
          namespace = Names.namespace(file)
          if namespace.empty?
            [*file.message_type, *file.enum_type, *file.service].map { |type| [Names.rubify(type.name)] }
          else
            [namespace]
          end
        end
        @namespaces.each do |parts, declarations|
          next unless relevant.any? { |prefix| parts.take(prefix.size) == prefix || prefix.take(parts.size) == parts }

          declarations.each do |kind, origin, file|
            RuntimeNames.validate!(parts, kind, origin, file) if loaded_names.include?(file)
          end
          unique = declarations.map { |kind, origin, _file| [kind, origin] }.uniq
          next if unique.size == 1
          next if unique.all? { |kind, _origin| kind == :package }

          raise InvalidDescriptor,
                "Ruby namespace collision at #{parts.join('::')}: #{unique.map(&:last).join(' and ')}"
        end
      end

      # Official target _pb files require their imports, including public imports.
      # Include explicitly resolved type files too for defensive indirect requests.
      def loaded_files
        required = targets.dup
        targets.each do |file|
          file.service.each do |service|
            service["method"].each do |method|
              [method.input_type, method.output_type].each { |type| required << types.fetch(type).first }
            end
          end
        end
        loaded = {}
        until required.empty?
          file = required.pop
          next if loaded.key?(file.name)

          loaded[file.name] = file
          file.dependency.each { |name| required << @files.fetch(name) }
        end
        loaded.values
      end

      def validate_outputs
        outputs = {}
        @files.each_value do |file|
          output = "#{Names.stem(file.name)}_pb.rb"
          if outputs[output]
            raise InvalidDescriptor,
                  "Output collision: #{outputs[output]} and #{file.name} -> #{output}"
          end

          outputs[output] = file.name
        end
        targets.reject { |file| file.service.empty? }.each do |file|
          extensions = rbs ? %w[rb rbs] : %w[rb]
          extensions.each do |extension|
            output = "#{Names.stem(file.name)}_connect.#{extension}"
            if outputs[output]
              raise InvalidDescriptor,
                    "Output collision: #{outputs[output]} and #{file.name} -> #{output}"
            end

            outputs[output] = file.name
          end
        end
      end

      def qualified(prefix, name) = [prefix, name].reject(&:empty?).join(".")
    end
  end
end
