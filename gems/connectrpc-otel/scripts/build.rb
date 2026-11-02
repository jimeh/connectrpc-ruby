# frozen_string_literal: true

require "fileutils"
require "rubygems/package"
root = File.expand_path("../../..", __dir__)
output = File.join(root, "vendor/otel/pkg")
FileUtils.mkdir_p(output)
[root, *%w[rack rails async falcon testing otel].map do |name|
  File.join(root, "gems/connectrpc-#{name}")
end].each do |directory|
  Dir.chdir(directory) do
    specification = Gem::Specification.load(Dir.glob("*.gemspec").fetch(0))
    Gem::Package.build(specification, false, false, File.join(output, specification.file_name))
  end
end
