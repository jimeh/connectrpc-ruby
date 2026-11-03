# frozen_string_literal: true

require "digest"
require "json"
require "time"

root = File.expand_path("..", __dir__)
records = JSON.parse(File.read(File.join(root, "browser/reference/artifacts.json")))
sums = File.readlines(File.join(root, "browser/reference/go.sum"), chomp: true)
records.each do |record|
  name, version = record.values_at("path", "version")
  unless Time.now.utc - Time.parse(record.fetch("published")) >= 604_800
    raise "Reference release is within cooldown: #{name}"
  end
  raise "Reference module checksum changed: #{name}" unless sums.include?("#{name} #{version} #{record.fetch('sum')}")
  unless sums.include?("#{name} #{version}/go.mod #{record.fetch('go_mod_sum')}")
    raise "Reference module declaration checksum changed: #{name}"
  end

  zip = File.join(ENV.fetch("GOMODCACHE"), "cache/download", name, "@v", "#{version}.zip")
  unless Digest::SHA256.file(zip).hexdigest == record.fetch("zip_sha256")
    raise "Reference ZIP differs from audited artifact: #{name}"
  end
end
puts "Independent Web reference: #{records.size} added module artifacts; " \
     "primary release ages, ZIP SHA256 and h1 pins verified"
