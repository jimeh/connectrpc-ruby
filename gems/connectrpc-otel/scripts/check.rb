# frozen_string_literal: true

require "rbconfig"
root = File.expand_path("../../..", __dir__)
files = Dir.glob("{lib,spec,scripts}/**/*.rb") + Dir.glob("*.gemspec") + ["Gemfile"]
files.each { |file| system(RbConfig.ruby, "-c", file, exception: true) }
system("rubocop", ".", "--config", File.join(root, ".rubocop.yml"), exception: true)
