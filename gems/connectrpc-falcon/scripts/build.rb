# frozen_string_literal: true

require_relative "../../../scripts/package_build"
root = File.expand_path("../../..", __dir__)
output = File.join(root, "vendor/falcon/pkg")
[root, File.join(root, "gems/connectrpc-async"), File.dirname(__dir__)].each do |directory|
  PackageBuild.build(directory: directory, output: output)
end
