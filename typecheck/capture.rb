# frozen_string_literal: true

require "digest"
require "fileutils"
require "find"
require "json"

module SourceCapture
  EXCLUDED = %w[.git .bundle .codegraph .rubocop_cache vendor pkg coverage].freeze

  def self.files(root)
    result = []
    Find.find(root) do |file|
      next if file == root

      next if EXCLUDED.include?(File.basename(file)) && !File.directory?(file)

      relative = file.delete_prefix("#{root}/")
      if File.directory?(file)
        Find.prune if relative.split("/").any? { |part| EXCLUDED.include?(part) }
      elsif File.file?(file)
        raise "Unexpected maintained source symlink: #{file}" if File.symlink?(file)

        result << relative
      end
    end
    result.sort
  end

  def self.snapshot(root)
    files(root).to_h do |file|
      absolute = File.join(root, file)
      [file, { "sha256" => Digest::SHA256.file(absolute).hexdigest, "mode" => File.stat(absolute).mode & 0o777 }]
    end
  end

  def self.create(root, destination)
    raise "Capture already exists: #{destination}" if File.exist?(destination)

    before = snapshot(root)
    before.each do |file, expected|
      source = File.join(root, file)
      target = File.join(destination, file)
      FileUtils.mkdir_p(File.dirname(target))
      FileUtils.copy_file(source, target, true)
      raise "Capture shares source inode: #{file}" if File.stat(source).ino == File.stat(target).ino
      raise "Capture byte mismatch: #{file}" unless Digest::SHA256.file(target).hexdigest == expected.fetch("sha256")
      raise "Capture mode mismatch: #{file}" unless File.stat(target).mode & 0o777 == expected.fetch("mode")
    end
    # Only the captured executable is normalized for the existing strict guard.
    File.chmod(0o755, File.join(destination, "exe/protoc-gen-connectrpc-ruby"))
    raise "Source moved during capture" unless snapshot(root) == before

    File.write("#{destination}.json", "#{JSON.pretty_generate(source: root, files: before)}\n")
    before
  end
end
