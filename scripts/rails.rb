# frozen_string_literal: true

require "fileutils"
require "rbconfig"
require "English"

root = File.expand_path("..", __dir__)
adapter = File.join(root, "gems/connectrpc-rails")
lane = ENV.fetch("RAILS_LANE", "8.1")
version = { "8.0" => "8.0.5.1", "8.1" => "8.1.4" }.fetch(lane)
# Check before Appraisal's legacy fallback can try installing Bundler globally.
abort "Bundler 4.0.23 is required" unless system(RbConfig.ruby, "-S", "gem", "list", "--silent", "-i", "bundler", "-v",
                                                 "4.0.23")
environment = {
  "BUNDLE_GEMFILE" => File.join(adapter, "gemfiles/rails_#{lane}.gemfile"),
  "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_FROZEN" => "true", "BUNDLE_COOLDOWN" => "7",
  "BUNDLE_PATH" => File.join(root, "vendor/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_VERSION" => "4.0.23", "BUNDLE_USER_HOME" => File.join(root, "vendor/rails/bundler"),
  "CONNECTRPC_RAILS_VERSION" => version, "CONNECTRPC_ROOT" => root
}
command = ARGV.shift
arguments = case command
            when "setup" then ["install"]
            when "test" then ["exec", "appraisal", "rails-#{lane}", "rspec", *ARGV]
            when "generation:check" then ["exec", "ruby", File.join(root, "scripts/check_appraisals.rb")]
            when "build" then ["exec", "ruby", "-rrubygems/package", "-rfileutils", "-e", <<~CODE]
              FileUtils.mkdir_p("../../vendor/rails/pkg")
              output = File.expand_path("../../vendor/rails/pkg")
              ["../..", "../connectrpc-rack", "."].each do |directory|
                Dir.chdir(directory) do
                  specification = Gem::Specification.load(Dir.glob("*.gemspec").fetch(0))
                  Gem::Package.build(specification, false, false, File.join(output, specification.file_name))
                end
              end
            CODE
            when "package" then ["exec", "appraisal", "rails-#{lane}", "ruby",
                                 File.join(root, "scripts/check_rails_package.rb")]
            else abort "Usage: ruby scripts/rails.rb setup|test|generation:check|build|package [RSpec options]"
            end
puts "Rails lane #{lane} (#{version}); Ruby #{RUBY_VERSION}; Bundler 4.0.23"
puts "Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
$stdout.flush
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: adapter)
