# frozen_string_literal: true

# Builds one connectrpc-hyper gem into vendor/hyper/cross/<platform>/: a
# platform gem cross-compiled for both Ruby minors in the pinned rb-sys-dock
# image (rbsys/<platform>:<rb_sys version>), or the source gem for `ruby`.
#
# rb-sys-dock bind-mounts its working directory, writes pkg/ and the
# cross-built binaries into lib/, and lets Docker create root-owned mount
# points. It therefore runs in a disposable copy of the package's tracked
# files, so the checkout stays clean and several platforms can build in turn.
require "fileutils"
require "open3"
require "rubygems/package"
require_relative "platforms"
require_relative "../../../scripts/package_build"

root = File.expand_path("../../..", __dir__)
package = File.expand_path("..", __dir__)
platform = ARGV.first.to_s.empty? ? ENV.fetch("PLATFORM", "") : ARGV.first
supported = [*HyperPlatforms::NATIVE, "ruby"]
abort "Usage: mise run hyper:cross -- PLATFORM (or PLATFORM=...), one of: #{supported.join(', ')}" unless
  supported.include?(platform)

output = File.join(root, "vendor/hyper/cross", platform)
FileUtils.rm_rf(output)
FileUtils.mkdir_p(output)
version = Gem::Specification.load(File.join(package, "connectrpc-hyper.gemspec")).version.to_s

if platform == "ruby"
  archive = PackageBuild.build(directory: package, output: output)
  spec = Gem::Package.new(archive).spec
  abort "Source gem lost its extension" unless spec.extensions == ["ext/connectrpc_hyper/extconf.rb"]
  puts "Built #{archive} (#{File.size(archive)} bytes)"
  exit
end

tracked, status = Open3.capture2("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", ".",
                                 chdir: package)
abort "git ls-files failed" unless status.success?
work = File.join(root, "vendor/hyper/cross-work", platform)
FileUtils.rm_rf(work)
tracked.split("\0").each do |file|
  FileUtils.mkdir_p(File.join(work, File.dirname(file)))
  FileUtils.cp(File.join(package, file), File.join(work, file), preserve: true)
end
# Pre-create the directory rb-sys-dock mounts its build tree on, so Docker does
# not create it as root.
FileUtils.mkdir_p(File.join(work, "tmp", platform))
epoch, status = Open3.capture2("git", "log", "-1", "--pretty=%ct", chdir: root)
abort "git log failed" unless status.success?
environment = {
  "CARGO_HOME" => File.join(root, "vendor/hyper/cargo"),
  "RB_SYS_DOCK_CACHE_DIR" => File.join(root, "vendor/hyper/rb-sys-dock"),
  "SOURCE_DATE_EPOCH" => epoch.strip
}
# The image's rake and rb_sys build the gem; nothing installs into the image.
# extconf.rb passes --locked, so the image builds exactly Cargo.lock.
system(environment, "rb-sys-dock", "--platform", platform, "--ruby-versions", HyperPlatforms::RUBY_MINORS.join(","),
       "--", "rake native:#{platform} gem", chdir: work, exception: true)

built = Dir.glob(File.join(work, "pkg", "connectrpc-hyper-#{version}-*.gem"))
abort "Expected one platform gem, found #{built}" unless built.size == 1
archive = File.join(output, File.basename(built.first))
FileUtils.mv(built.first, archive)
gem = Gem::Package.new(archive)
spec = gem.spec
abort "Platform #{spec.platform} is not #{platform}" unless Gem::Platform.new(platform) == spec.platform
abort "Platform gem must not build an extension" unless spec.extensions.empty?
dependencies = spec.runtime_dependencies.map(&:name)
abort "Platform gem runtime dependencies changed: #{dependencies}" unless dependencies == ["connectrpc"]
binaries = gem.contents.grep(%r{\Alib/connectrpc/hyper/[^/]+/connectrpc_hyper\.(?:so|bundle|dll)\z})
minors = binaries.map { |file| file.split("/")[3] }.sort
abort "Expected extensions for Ruby #{HyperPlatforms::RUBY_MINORS}, found #{binaries}" unless
  minors == HyperPlatforms::RUBY_MINORS
FileUtils.rm_rf(work)
puts "Built #{archive} (#{File.size(archive)} bytes; Ruby #{minors.join(', ')}; requires Ruby " \
     "#{spec.required_ruby_version})"
