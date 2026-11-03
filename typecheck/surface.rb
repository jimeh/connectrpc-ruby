# frozen_string_literal: true

require "json"
require "prism"
require "rbs"

# Source syntax, rather than loaded constants, keeps optional integrations absent.
# The reviewed inventory classifies every definition; additions require a decision.
module APISurface
  class Reader
    attr_reader :entries

    def initialize
      @entries = {}
    end

    def scan(node, owner = "", visibility = :public)
      return unless node

      case node
      when Prism::ClassNode, Prism::ModuleNode
        name = node.constant_path.slice.delete_prefix("::")
        name = "#{owner}::#{name}" unless owner.empty? || node.constant_path.slice.start_with?("::")
        scan(node.body, name)
      when Prism::StatementsNode
        node.body.each do |statement|
          if statement.is_a?(Prism::CallNode) && statement.receiver.nil? && %i[public private
                                                                               protected].include?(statement.name)
            if statement.arguments
              statement.arguments.arguments.each do |argument|
                next unless argument.is_a?(Prism::SymbolNode)

                entry = @entries["#{owner}##{argument.unescaped}"]
                entry["visibility"] = statement.name.to_s if entry
              end
            else
              visibility = statement.name
            end
          elsif statement.is_a?(Prism::CallNode) && %i[private_constant public_constant].include?(statement.name)
            statement.arguments.arguments.each do |argument|
              entry = @entries["#{owner}::#{argument.unescaped}"]
              entry["visibility"] = statement.name == :private_constant ? "private" : "public" if entry
            end
          elsif statement.is_a?(Prism::CallNode) && statement.name == :private_class_method
            statement.arguments.arguments.each do |argument|
              entry = @entries["#{owner}.#{argument.unescaped}"]
              entry["visibility"] = "private" if entry
            end
          else
            scan(statement, owner, visibility)
          end
        end
      when Prism::DefNode
        key = "#{owner}#{node.receiver ? '.' : '#'}#{node.name}"
        @entries[key] = { "kind" => "method", "visibility" => visibility.to_s,
                          "parameters" => node.parameters&.slice&.gsub(/\s+/, " ") || "" }
      when Prism::ConstantWriteNode
        value = node.value
        if value.is_a?(Prism::CallNode) && value.receiver && %w[Struct Data].include?(value.receiver.slice)
          arguments = value.arguments.arguments
          members = arguments.grep(Prism::SymbolNode).map(&:unescaped)
          @entries["#{owner}::#{node.name}"] = {
            "kind" => value.receiver.slice, "visibility" => "public", "members" => members,
            "options" => arguments.grep_v(Prism::SymbolNode).map(&:slice)
          }
        else
          scan(value, owner, visibility)
        end
      when Prism::CallNode
        if node.receiver.nil? && %i[attr_reader attr_writer attr_accessor].include?(node.name)
          node.arguments.arguments.each do |argument|
            next unless argument.is_a?(Prism::SymbolNode)

            names = []
            names << argument.unescaped unless node.name == :attr_writer
            names << "#{argument.unescaped}=" unless node.name == :attr_reader
            names.each do |name|
              @entries["#{owner}##{name}"] = { "kind" => "attribute", "visibility" => visibility.to_s }
            end
          end
        else
          node.compact_child_nodes.each { |child| scan(child, owner, visibility) }
        end
      else
        node.compact_child_nodes.each { |child| scan(child, owner, visibility) }
      end
    end
  end

  def self.source(root)
    Dir.glob("{lib,gems/*/lib}/**/*.rb", base: root).sort.to_h do |file|
      parsed = Prism.parse_file(File.join(root, file))
      raise "Cannot inventory #{file}: #{parsed.errors.map(&:message)}" unless parsed.success?

      reader = Reader.new
      reader.scan(parsed.value)
      [file, reader.entries.sort.to_h]
    end
  end

  def self.signatures(root)
    declarations = {}
    visit = lambda do |nodes, owner|
      nodes.each do |node|
        case node
        when RBS::AST::Declarations::Class, RBS::AST::Declarations::Module, RBS::AST::Declarations::Interface
          name = node.name.to_s.delete_prefix("::")
          name = "#{owner}::#{name}" unless owner.empty? || node.name.absolute?
          declarations[name] = true
          visit.call(node.members, name)
        when RBS::AST::Members::MethodDefinition
          declarations["#{owner}#{node.kind == :singleton ? '.' : '#'}#{node.name}"] = true
        when RBS::AST::Members::AttrReader, RBS::AST::Members::AttrWriter, RBS::AST::Members::AttrAccessor
          declarations["#{owner}##{node.name}"] = true unless node.is_a?(RBS::AST::Members::AttrWriter)
          declarations["#{owner}##{node.name}="] = true unless node.is_a?(RBS::AST::Members::AttrReader)
        end
      end
    end
    Dir.glob("{sig,gems/*/sig}/**/*.rbs", base: root).each do |file|
      _, _, nodes = RBS::Parser.parse_signature(File.read(File.join(root, file)))
      visit.call(nodes, "")
    end
    declarations
  end

  def self.check(root)
    expected = JSON.parse(File.read(File.join(root, "typecheck/surface.json")))
    actual = source(root)
    declarations = signatures(root)
    problems = []
    (actual.keys | expected.keys).sort.each do |file|
      wanted = expected.fetch(file, {})
      found = actual.fetch(file, {})
      (wanted.keys | found.keys).each do |key|
        entry = wanted[key]
        classification = entry && entry["classification"]
        unless %w[supported spi internal].include?(classification)
          problems << "Unclassified source definition: #{file}: #{key}"
          next
        end
        shape = entry.except("classification")
        problems << "Source surface changed: #{file}: #{key}" unless shape == found[key]
        next if classification == "internal"

        if %w[Struct Data].include?(entry["kind"])
          entry.fetch("members").each do |member|
            problems << "Missing record reader: #{key}##{member}" unless declarations["#{key}##{member}"]
            if entry["kind"] == "Struct" && !declarations["#{key}##{member}="]
              problems << "Missing record writer: #{key}##{member}="
            end
          end
        end
        # Private accepted handles are promised through a structural capability.
        signature_key = key.sub("ConnectRPC::Async::ServerSession::Accepted#", "ConnectRPC::Async::_Accepted#")
        problems << "Missing public signature: #{key}" unless declarations[signature_key]
      end
    end
    raise problems.join("\n") unless problems.empty?

    counts = expected.values.flat_map(&:values).group_by do |entry|
      entry.fetch("classification")
    end.transform_values(&:size)
    puts "Source API inventory: #{actual.size} files, #{counts.inspect}; " \
         "definitions/options/visibility/records match signatures"
  end
end

APISurface.check(File.expand_path("..", __dir__)) if $PROGRAM_NAME == __FILE__
