# frozen_string_literal: true

module ConnectRPC
  module Generator
    module Names
      IDENTIFIER = /\A[A-Za-z_][A-Za-z_0-9]*\z/
      CONSTANT = /\A[A-Z][A-Za-z_0-9]*\z/
      KEYWORDS = %w[
        BEGIN END alias and begin break case class def defined do else elsif end ensure false for if in module
        next nil not or redo rescue retry return self super then true undef unless until when while yield
        __FILE__ __LINE__ __ENCODING__
      ].freeze
      CLIENT_RESERVED = %w[
        initialize handlers raise class clone define_singleton_method display dup enum_for
        extend freeze
        initialize_copy initialize_dup initialize_clone method_missing respond_to_missing
        singleton_method_added singleton_method_removed singleton_method_undefined
        to_str to_ary to_hash to_proc to_io to_path to_int to_sym to_regexp
        to_yaml to_json pretty_print_inspect pretty_print_cycle pretty_print
        pretty_print_instance_variables pretty_inspect
        hash inspect instance_eval instance_exec instance_variable_get
        instance_variable_set instance_variables itself method methods nil object_id private_methods
        protected_methods public_method public_methods public_send remove_instance_variable send
        singleton_class singleton_method singleton_methods tap then to_enum to_s yield_self __id__ __send__
      ].freeze
      TOP_LEVEL_RESERVED = %w[
        ARGF ARGV ArgumentError Array BasicObject Binding CROSS_COMPILING Class ClosedQueueError Comparable Complex
        ConditionVariable ConnectRPC Data DidYouMean Dir ENV EOFError Encoding EncodingError Enumerable Enumerator
        Errno ErrorHighlight Exception FalseClass Fiber FiberError File FileTest Float FloatDomainError FrozenError
        GC Gem Google Hash IO IOError IndexError Integer Interrupt JSON Kernel KeyError LoadError LocalJumpError
        Marshal MatchData Math Method Module Monitor MonitorMixin Mutex NameError NilClass NoMatchingPatternError
        NoMatchingPatternKeyError NoMemoryError NoMethodError NotImplementedError Numeric Object ObjectSpace Pathname
        Proc Process Queue RUBYGEMS_ACTIVATION_MONITOR RUBY_COPYRIGHT RUBY_DESCRIPTION RUBY_ENGINE RUBY_ENGINE_VERSION
        RUBY_PATCHLEVEL RUBY_PLATFORM RUBY_RELEASE_DATE RUBY_REVISION RUBY_VERSION Rack Ractor Random Range RangeError
        Rational RbConfig Refinement Regexp RegexpError Ruby RubyVM RuntimeError STDERR STDIN STDOUT ScriptError
        SecurityError Set Signal SignalException SizedQueue StandardError StopIteration String Struct Symbol SyntaxError
        SyntaxSuggest SystemCallError SystemExit SystemStackError TOPLEVEL_BINDING Thread ThreadError ThreadGroup Time
        TracePoint TrueClass TypeError UnboundMethod UncaughtThrowError UnicodeNormalize Warning ZeroDivisionError
      ].freeze

      def self.identifier!(name, description)
        return if IDENTIFIER.match?(name)

        raise InvalidDescriptor, "Invalid protobuf name #{name.inspect} in #{description}"
      end

      def self.constant!(name, description)
        return if CONSTANT.match?(name) && !%w[BEGIN END].include?(name)

        raise InvalidDescriptor, "Invalid Ruby constant #{name.inspect} for #{description}"
      end

      # Match protoc v36.2 RubifyConstant and PackageToModule, rather than
      # camelizing message/service names (which preserves their underscores).
      def self.rubify(name)
        name.sub(/\A[a-z]/, &:upcase).sub(/\A(?=[^A-Za-z])/, "PB_")
      end

      def self.namespace(file)
        option = file.options
        package = option&.has_ruby_package? ? option.ruby_package : file.package
        return [] if package.empty?

        parts = if package.include?("::")
                  package.split("::", -1)
                else
                  package.split(".", -1).map { |part| package_component(part) }
                end
        parts.each { |part| constant!(part, "namespace of #{file.name}") }
        parts
      end

      def self.package_component(part)
        part.split("_", -1).map { |word| word.sub(/\A[a-z]/, &:upcase) }.join
      end

      # gRPC Ruby's established underscore algorithm.
      def self.rpc(name)
        name.gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2').gsub(/([a-z\d])([A-Z])/, '\1_\2').tr("-", "_").downcase
      end

      def self.rpc!(name, description)
        identifier!(name, description)
        ruby = rpc(name)
        if name.start_with?("_") || KEYWORDS.include?(ruby) || CLIENT_RESERVED.include?(ruby) ||
           %w[BEGIN END SERVICE CLIENT].include?(ruby.upcase)
          raise InvalidDescriptor, "Reserved or unsupported Ruby RPC name #{name.inspect} (#{ruby}) in #{description}"
        end

        constant!(ruby.upcase, description)
        ruby
      end

      def self.path!(name)
        parts = name.split("/", -1)
        return if %r{\A[A-Za-z0-9_./-]+\.(?:proto|protodevel)\z}.match?(name) &&
                  parts.none? { |part| part.empty? || %w[. ..].include?(part) }

        raise InvalidDescriptor, "Unsafe relative proto path #{name.inspect}"
      end

      def self.stem(name) = name.sub(/\.(?:proto|protodevel)\z/, "")
    end
  end
end
