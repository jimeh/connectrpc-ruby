# frozen_string_literal: true

# Builds one connectrpc-hyper gem into vendor/hyper/cross/<platform>/: a
# platform gem cross-compiled for both Ruby minors in the digest-pinned
# rb-sys-dock image (HyperPlatforms::IMAGES), or the source gem for `ruby`.
#
# rb-sys-dock bind-mounts its working directory, writes pkg/ and the
# cross-built binaries into lib/, and lets Docker create root-owned mount
# points. It therefore runs in a disposable copy of the package's tracked
# files, so the checkout stays clean and several platforms can build in turn.
#
# HYPER_CROSS_TWICE=1 moves the first archive out, deletes the work copy,
# Cargo home and rb-sys-dock cache, builds again from empty caches at the same
# absolute paths, and fails unless both SHA256 match. The paths stay fixed
# because rb-sys-dock mounts the work copy at its host path and keeps the cargo
# target directory under it: darwin linkers embed those object paths (and an
# LC_UUID derived from them) in the binary. CI always builds at the same
# workspace path, which is what a release rebuild must reproduce. The
# hyper-gems workflow always sets it.
require "digest"
require "fileutils"
require "open3"
require "rb_sys/version"
require "rubygems/package"
require "tmpdir"
require_relative "platforms"
require_relative "../../../scripts/package_build"

root = File.expand_path("../../..", __dir__)
package = File.expand_path("..", __dir__)
platform = ARGV.first.to_s.empty? ? ENV.fetch("PLATFORM", "") : ARGV.first
supported = [*HyperPlatforms::NATIVE, "ruby"]
abort "Usage: mise run hyper:cross -- PLATFORM (or PLATFORM=...), one of: #{supported.join(', ')}" unless
  supported.include?(platform)
twice = ENV["HYPER_CROSS_TWICE"] == "1"
puts "Ruby #{RUBY_VERSION}; RubyGems #{Gem::VERSION}; Bundler #{Bundler::VERSION}; rb_sys #{RbSys::VERSION}"

output = File.join(root, "vendor/hyper/cross", platform)
FileUtils.rm_rf(output)
FileUtils.mkdir_p(output)
version = Gem::Specification.load(File.join(package, "connectrpc-hyper.gemspec")).version.to_s
epoch, status = Open3.capture2("git", "show", "-s", "--format=%ct", "HEAD", chdir: root)
abort "git show failed" unless status.success?
epoch = epoch.strip

if platform == "ruby"
  # The release candidate rebuilds the source gem on Ruby 3.4.12 with the
  # commit time as SOURCE_DATE_EPOCH; the tested archive must equal it.
  abort "The source gem must build on Ruby 3.4.12, as the release candidate does" unless RUBY_VERSION == "3.4.12"
  ENV["SOURCE_DATE_EPOCH"] = epoch
  archive = PackageBuild.build(directory: package, output: output)
  spec = Gem::Package.new(archive).spec
  abort "Source gem lost its extension" unless spec.extensions == ["ext/connectrpc_hyper/extconf.rb"]
  sha256 = Digest::SHA256.file(archive).hexdigest
  if twice
    Dir.mktmpdir("connectrpc-hyper-rebuild-") do |temporary|
      rebuilt = Digest::SHA256.file(PackageBuild.build(directory: package, output: temporary)).hexdigest
      abort "Non-reproducible source gem: #{sha256} then #{rebuilt}" unless rebuilt == sha256
    end
  end
  puts "Built #{archive} (#{File.size(archive)} bytes; SHA256 #{sha256}#{'; rebuilt identically' if twice})"
  exit
end

image = HyperPlatforms::IMAGES.fetch(platform)
tag = image[/:([^:@]+)@sha256:\h{64}\z/, 1]
abort "#{image} is not pinned to rb_sys #{RbSys::VERSION}; update HyperPlatforms::IMAGES" unless
  tag == RbSys::VERSION
tracked, status = Open3.capture2("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", ".",
                                 chdir: package)
abort "git ls-files failed" unless status.success?

# Builds the platform gem in a fresh copy and returns its path inside the copy.
build = lambda do |work, cargo_home, cache_dir|
  FileUtils.rm_rf(work)
  # rb-sys-dock bind-mounts the registry with --mount, which needs the source.
  FileUtils.mkdir_p(File.join(cargo_home, "registry"))
  tracked.split("\0").each do |file|
    FileUtils.mkdir_p(File.join(work, File.dirname(file)))
    FileUtils.cp(File.join(package, file), File.join(work, file), preserve: true)
  end
  # Pre-create the directory rb-sys-dock mounts its build tree on, so Docker
  # does not create it as root.
  FileUtils.mkdir_p(File.join(work, "tmp", platform))
  environment = { "CARGO_HOME" => cargo_home, "RB_SYS_DOCK_CACHE_DIR" => cache_dir, "SOURCE_DATE_EPOCH" => epoch,
                  "RCD_IMAGE" => image }
  # rb-sys-dock runs an unlocked `cargo metadata` in the copy before Docker
  # starts, which would rewrite a stale Cargo.lock that the locked compile then
  # accepts. Fail first on a lock that needs updating.
  abort "Cargo.lock does not match the crate graph; run mise run hyper:cargo:update" unless
    system(environment, "cargo", "metadata", "--locked", "--format-version", "1", "--manifest-path",
           File.join(work, "Cargo.toml"), out: File::NULL)
  # The image's rake and rb_sys build the gem; nothing installs into the image.
  # extconf.rb passes --locked, so the image builds exactly Cargo.lock.
  command = "rake native:#{platform} gem"
  # The MinGW linkers stamp PE headers with the build time and keep a COFF
  # symbol table that differs between builds; drop both so rebuilds match.
  # rb_sys appends RUSTFLAGS from the environment to its own flags.
  command = "RUSTFLAGS=\"#{HyperPlatforms::MINGW_RUSTFLAGS}\" #{command}" if platform.end_with?("mingw-ucrt")
  system(environment, "rb-sys-dock", "--platform", platform, "--ruby-versions",
         HyperPlatforms::RUBY_MINORS.join(","), "--", command, chdir: work, exception: true)
  drift = HyperPlatforms.locked_drift(package, work)
  abort "The build changed #{drift.join(', ')}; refusing an archive built from another crate graph" if drift.any?

  built = Dir.glob(File.join(work, "pkg", "connectrpc-hyper-#{version}-*.gem"))
  abort "Expected one platform gem, found #{built}" unless built.size == 1
  built.first
end

work = File.join(root, "vendor/hyper/cross-work", platform)
# Each platform keeps its own Cargo home and rb-sys-dock cache: the twice build
# deletes both while other platforms may be building, and hyper:package and
# rbs:package share vendor/hyper/cargo.
cargo_home = File.join(root, "vendor/hyper/cross-cargo", platform)
cache_dir = File.join(root, "vendor/hyper/rb-sys-dock", platform)
built = build.call(work, cargo_home, cache_dir)
archive = File.join(output, File.basename(built))
FileUtils.mv(built, archive)
FileUtils.rm_rf(work)
sha256 = Digest::SHA256.file(archive).hexdigest
if twice
  # rm_rf ignores errors, so a root-owned leftover would otherwise warm the rebuild.
  FileUtils.rm_rf([cargo_home, cache_dir])
  leftovers = [work, cargo_home, cache_dir].select { File.exist?(it) }
  abort "Could not empty #{leftovers.join(', ')} before the rebuild" if leftovers.any?

  rebuilt = build.call(work, cargo_home, cache_dir)
  second = Digest::SHA256.file(rebuilt).hexdigest
  abort "Non-reproducible #{File.basename(archive)}: #{sha256} then #{second}" unless
    File.basename(rebuilt) == File.basename(archive) && second == sha256
  FileUtils.rm_rf(work)
end
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
puts "Built #{archive} (#{File.size(archive)} bytes; SHA256 #{sha256}#{'; rebuilt identically' if twice}; Ruby " \
     "#{minors.join(', ')}; requires Ruby #{spec.required_ruby_version}; image #{image})"
