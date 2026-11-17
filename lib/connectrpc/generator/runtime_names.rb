# frozen_string_literal: true

module ConnectRPC
  module Generator
    module RuntimeNames
      # google-protobuf 4.36.2: protobuf.rb, message_exts.rb, repeated_field.rb,
      # internal/object_cache.rb and ext/google/protobuf_c/{defs,map,protobuf}.c.
      # Protect helper subtrees too; package modules cannot reopen their classes.
      PROTOBUF_RUNTIME = %w[
        AbstractMessage Descriptor DescriptorPool EnumDescriptor Error FieldDescriptor FileDescriptor
        FFI IMPLEMENTATION Internal Map MessageExts MethodDescriptor OneofDescriptor ParseError PREFER_FFI
        RepeatedField ServiceDescriptor TypeError UnexpectedStructType
      ].freeze
      RUNTIME_PACKAGES = %w[Google Google::Protobuf Google::Protobuf::Compiler].freeze
      # Concrete assignments in the pinned gem's official *_pb.rb files. These
      # namespaces belong to those proto definitions, including nested types.
      PROTOBUF_FILES = {
        "google/protobuf/any.proto" => %w[Any],
        "google/protobuf/api.proto" => %w[Api Method Mixin],
        "google/protobuf/descriptor.proto" => %w[
          FileDescriptorSet FileDescriptorProto DescriptorProto ExtensionRangeOptions FieldDescriptorProto
          OneofDescriptorProto EnumDescriptorProto EnumValueDescriptorProto ServiceDescriptorProto MethodDescriptorProto
          FileOptions MessageOptions FieldOptions OneofOptions EnumOptions EnumValueOptions ServiceOptions MethodOptions
          UninterpretedOption FeatureSet FeatureSetDefaults SourceCodeInfo GeneratedCodeInfo Edition SymbolVisibility
        ],
        "google/protobuf/duration.proto" => %w[Duration],
        "google/protobuf/empty.proto" => %w[Empty],
        "google/protobuf/field_mask.proto" => %w[FieldMask],
        "google/protobuf/compiler/plugin.proto" => %w[
          Compiler::Version Compiler::CodeGeneratorRequest Compiler::CodeGeneratorResponse
        ],
        "google/protobuf/source_context.proto" => %w[SourceContext],
        "google/protobuf/struct.proto" => %w[Struct Value ListValue NullValue],
        "google/protobuf/timestamp.proto" => %w[Timestamp],
        "google/protobuf/type.proto" => %w[Type Field Enum EnumValue Option Syntax],
        "google/protobuf/wrappers.proto" => %w[
          DoubleValue FloatValue Int64Value UInt64Value Int32Value UInt32Value BoolValue StringValue BytesValue
        ]
      }.freeze

      RUBY_PACKAGE_HINT = "set the ruby_package file option to choose another Ruby namespace"

      def self.validate!(parts, kind, origin, file) # rubocop:disable Metrics/MethodLength
        name = parts.join("::")
        if reserved_root?(parts, kind)
          # Below a reopenable root, only a builtin child is ever reserved.
          constant = if parts.size > 1 && Names::REOPENABLE_ROOTS.include?(parts.first)
                       "builtin Ruby constant #{parts.take(2).join('::')}"
                     else
                       "top-level Ruby constant #{parts.first}"
                     end
          raise InvalidDescriptor, "Reserved #{constant} for #{file}: #{origin}; #{RUBY_PACKAGE_HINT}"
        end

        protected_name = protected_runtime(parts, kind) || protected_protobuf_type(parts, kind, origin, file)
        return unless protected_name

        raise InvalidDescriptor, "Reserved runtime Ruby constant #{protected_name} at #{name} for #{file}: #{origin}"
      end

      def self.protected_runtime(parts, kind)
        name = parts.join("::")
        return name if RUNTIME_PACKAGES.include?(name) && kind != :package
        return unless parts.take(2) == %w[Google Protobuf] && PROTOBUF_RUNTIME.include?(parts[2])

        parts.take(3).join("::")
      end

      # Google and the reopenable builtin roots accept package modules. Google
      # nesting is otherwise left to the protobuf runtime checks; a builtin root
      # also rejects replacing a module child and anything at or below a value
      # or class child.
      def self.reserved_root?(parts, kind) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        root, child = parts
        return false unless Names::TOP_LEVEL_RESERVED.include?(root)
        return true unless root == "Google" || Names::REOPENABLE_ROOTS.include?(root)
        return kind != :package if child.nil?
        return false if root == "Google"
        return parts.size == 2 && kind != :package if Names::BUILTIN_MODULE_CHILDREN.fetch(root, []).include?(child)

        Names::BUILTIN_VALUE_CHILDREN.fetch(root, []).any? { |entry| entry === child } # rubocop:disable Style/CaseEquality -- String or platform Regexp
      end

      def self.protected_protobuf_type(parts, kind, origin, file)
        return unless parts.take(2) == %w[Google Protobuf]

        suffix = parts.drop(2).take(parts[2] == "Compiler" ? 2 : 1).join("::")
        source, = PROTOBUF_FILES.find { |_source, names| names.include?(suffix) }
        return unless source

        return if source == file && official_type?(parts, kind, origin)

        "Google::Protobuf::#{suffix}"
      end

      def self.official_type?(parts, kind, origin)
        official = "google.protobuf.#{parts.drop(2).join('.').sub(/\ACompiler\./, 'compiler.')}"
        %i[message enum].include?(kind) && origin == official
      end
    end
  end
end
