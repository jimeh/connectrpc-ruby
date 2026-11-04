# frozen_string_literal: true

require_relative "../../../scripts/package_build"
root = File.expand_path("../../..", __dir__)
output = File.join(root, "vendor/otel/pkg")
[root, *%w[rack rails async falcon testing otel].map do |name|
  File.join(root, "gems/connectrpc-#{name}")
end].each do |directory|
  PackageBuild.build(directory: directory, output: output)
end
