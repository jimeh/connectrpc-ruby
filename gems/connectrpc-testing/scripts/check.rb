# frozen_string_literal: true

require "rbconfig"
files = Dir.glob("{lib,spec,scripts}/**/*.rb") + Dir.glob("*.gemspec") + ["Gemfile"]
files.each { |file| system(RbConfig.ruby, "-c", file, exception: true) }
system("rubocop", ".", "--config", ".rubocop.yml", exception: true)
