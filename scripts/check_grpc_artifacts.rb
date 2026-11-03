# frozen_string_literal: true

require "digest"
require "json"
require "open3"
require "time"

root = File.expand_path("..", __dir__)
cache = ENV.fetch("GOMODCACHE")
manifest = JSON.parse(File.read(File.join(root, "interop/grpc/artifacts.json")))
modules, status = Open3.capture2("go", "list", "-m", "-json", "all", chdir: File.join(root, "interop/grpc"))
raise "Cannot resolve frozen gRPC graph" unless status.success?

# Go emits adjacent JSON objects. Use the same complete record boundaries as its
# module downloader, without accepting partial paths or changing its graph.
used = modules.scan(/^\{\n.*?^\}/m).map { |record| JSON.parse(record) }.reject { |record| record["Main"] }
expected = manifest.to_h { |record| ["#{record.fetch('Path')}@#{record.fetch('Version')}", record] }
used.each { |record| expected.fetch("#{record.fetch('Path')}@#{record.fetch('Version')}") }
manifest.each do |record|
  module_name = record.fetch("Path").gsub(/[A-Z]/) { |char| "!#{char.downcase}" }
  base = File.join(cache, "cache/download", module_name, "@v", record.fetch("Version"))
  { "mod" => "GoModSHA256", "zip" => "ZipSHA256" }.each do |extension, key|
    unless Digest::SHA256.file("#{base}.#{extension}").hexdigest == record.fetch(key)
      raise "gRPC artifact mismatch: #{module_name} #{extension}"
    end
  end
  info = JSON.parse(File.read("#{base}.info"))
  unless Time.now.utc - Time.iso8601(info.fetch("Time")) >= 7 * 86_400
    raise "gRPC module cooldown not met: #{module_name}"
  end
end
puts "Frozen gRPC graph: #{used.size} resolved modules; " \
     "#{manifest.size} audited module and archive hashes; seven-day cooldown"
