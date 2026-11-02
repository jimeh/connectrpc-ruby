# frozen_string_literal: true

require "rubygems"
require "rbconfig"

version = "4.0.23"
if Gem::Specification.find_all_by_name("bundler", "= #{version}").empty?
  system(
    RbConfig.ruby, "-S", "gem", "install", "--norc", "bundler", "--version", version, "--no-document", exception: true
  )
end
puts "Ruby #{RUBY_VERSION}, Bundler #{version} ready"
