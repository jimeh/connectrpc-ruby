# frozen_string_literal: true

require "fileutils"
require "rubygems/package"
root = File.expand_path("../../..", __dir__)
output = File.join(root, "vendor/async/pkg")
FileUtils.mkdir_p(output)
[root, File.dirname(__dir__)].each do |directory|
  Dir.chdir(directory) do
    specification = Gem::Specification.load(Dir.glob("*.gemspec").fetch(0))
    Gem::Package.build(specification, false, false, File.join(output, specification.file_name))
  end
end
