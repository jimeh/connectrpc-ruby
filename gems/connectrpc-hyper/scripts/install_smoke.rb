# frozen_string_literal: true

# Installs a built connectrpc-hyper gem (a platform gem from hyper:cross, or the
# source gem) with a core gem built from this checkout into a fresh GEM_HOME
# outside the checkout, offline, then runs scripts/smoke.rb against it. Needs
# only Ruby: CI runs it on native runners, in Alpine containers and on macOS
# and Windows.
#
#   ruby gems/connectrpc-hyper/scripts/install_smoke.rb vendor/hyper/cross/<platform>
require "fileutils"
require "rbconfig"
require "rubygems/package"
require "tmpdir"
require_relative "../../../scripts/package_build"

root = File.expand_path("../../..", __dir__)
source = ARGV.fetch(0) { abort "Usage: install_smoke.rb DIRECTORY|GEM" }
archives = File.directory?(source) ? Dir.glob(File.join(source, "connectrpc-hyper-*.gem")) : [source]
abort "Expected one connectrpc-hyper gem in #{source}, found #{archives}" unless archives.size == 1
hyper = Gem::Package.new(File.expand_path(archives.first))
platform = hyper.spec.platform.to_s
native = platform != "ruby"
if native && !Gem::Platform.match_gem?(hyper.spec.platform, hyper.spec.name)
  abort "#{platform} gem does not match this Ruby (#{Gem::Platform.local})"
end
# RubyGems builds nothing for a gem without extensions, so a platform gem
# installs without a compiler or Rust.
abort "#{platform} gem declares an extension build" if native && !hyper.spec.extensions.empty?

Dir.mktmpdir("connectrpc-hyper-install-") do |temporary|
  working = File.join(temporary, "cwd")
  gem_home = File.join(temporary, "gems")
  FileUtils.mkdir_p(working)
  core = PackageBuild.build(directory: root, output: working)
  FileUtils.cp([hyper.gem.path, File.join(__dir__, "smoke.rb")], working)
  environment = { "GEM_HOME" => gem_home, "GEM_PATH" => gem_home, "RUBYOPT" => nil, "RUBYLIB" => nil,
                  "RUBYGEMS_GEMDEPS" => nil }
  ENV.each_key { |key| environment[key] = nil if key.start_with?("BUNDLE_", "BUNDLER_") }
  run = ->(*args) { system(environment, RbConfig.ruby, *args, chdir: working, exception: true) }
  run.call("-S", "gem", "install", "--norc", "--local", "--no-document", File.basename(core),
           File.basename(hyper.gem.path))
  run.call("-e", <<~'CODE')
    require "connectrpc/hyper"
    spec = Gem.loaded_specs.fetch("connectrpc-hyper")
    abort "Loaded outside GEM_HOME" unless spec.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
    minor = RUBY_VERSION[/\A\d+\.\d+/]
    binary = Dir.glob(File.join(spec.full_gem_path, "lib/connectrpc/hyper/{#{minor}/,}connectrpc_hyper.{so,bundle}"))
    abort "Extension missing for Ruby #{minor}" if binary.empty?
    puts "Installed connectrpc-hyper #{spec.version} (#{spec.platform}) for Ruby #{RUBY_VERSION} " \
         "(#{RUBY_PLATFORM}): #{binary.first.delete_prefix("#{spec.full_gem_path}/")}"
  CODE
  run.call("smoke.rb")
end
