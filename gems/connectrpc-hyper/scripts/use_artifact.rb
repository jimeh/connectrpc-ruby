# frozen_string_literal: true

# Places the running Ruby's extension from a built platform gem into this
# package's lib/connectrpc/hyper/<minor>/ (an ignored path the loader tries
# first), so the specs exercise that artifact on hosts without Rust, such as
# the Windows CI job.
#
#   ruby gems/connectrpc-hyper/scripts/use_artifact.rb vendor/hyper/cross/<platform>
require "fileutils"
require "rubygems/package"
require "tmpdir"

source = ARGV.fetch(0) { abort "Usage: use_artifact.rb DIRECTORY|GEM" }
archives = File.directory?(source) ? Dir.glob(File.join(source, "connectrpc-hyper-*.gem")) : [source]
abort "Expected one connectrpc-hyper gem in #{source}, found #{archives}" unless archives.size == 1
gem = Gem::Package.new(archives.first)
abort "#{gem.spec.platform} gem does not match this Ruby" unless
  Gem::Platform.match_gem?(gem.spec.platform, gem.spec.name) && gem.spec.platform.to_s != "ruby"
minor = RUBY_VERSION[/\A\d+\.\d+/]
member = gem.contents.grep(%r{\Alib/connectrpc/hyper/#{Regexp.escape(minor)}/connectrpc_hyper\.(?:so|bundle)\z}).first
abort "#{File.basename(archives.first)} has no extension for Ruby #{minor}" unless member
Dir.mktmpdir("connectrpc-hyper-artifact-") do |extracted|
  gem.extract_files(extracted, member)
  target = File.join(File.expand_path("..", __dir__), member)
  FileUtils.mkdir_p(File.dirname(target))
  FileUtils.cp(File.join(extracted, member), target)
  puts "Using #{File.basename(archives.first)} extension for Ruby #{minor}: #{target}"
end
