# frozen_string_literal: true

# Installs a built connectrpc-hyper gem (a platform gem from hyper:cross, or the
# source gem) with a core gem built from this checkout into a fresh GEM_HOME
# outside the checkout, then runs scripts/smoke.rb against it. A platform gem
# installs offline and needs only Ruby: CI runs it on native runners, in Alpine
# containers and on macOS and Windows. The source gem compiles with the active
# cargo (pinned Rust through `mise run hyper:smoke:source`), libclang and a C
# compiler, fetching its locked crates; its rb_sys closure comes from the hyper
# bundle's gem cache.
#
#   ruby gems/connectrpc-hyper/scripts/install_smoke.rb vendor/hyper/cross/<platform> [EVIDENCE.json]
#
# After a successful smoke, EVIDENCE.json records the archive's file name,
# SHA256 and gem platform with the Ruby that exercised it; the release
# candidate requires one record per archive and Ruby minor.
require "digest"
require "fileutils"
require "json"
require "rbconfig"
require "rubygems/package"
require "tmpdir"
require_relative "../../../scripts/package_build"

root = File.expand_path("../../..", __dir__)
source = ARGV.fetch(0) { abort "Usage: install_smoke.rb DIRECTORY|GEM [EVIDENCE.json]" }
evidence = ARGV[1] && File.expand_path(ARGV[1])
archives = File.directory?(source) ? Dir.glob(File.join(source, "connectrpc-hyper-*.gem")) : [source]
abort "Expected one connectrpc-hyper gem in #{source}, found #{archives}" unless archives.size == 1
hyper = Gem::Package.new(File.expand_path(archives.first))
platform = hyper.spec.platform.to_s
native = platform != "ruby"
sha256 = Digest::SHA256.file(hyper.gem.path).hexdigest
puts "Installing #{File.basename(hyper.gem.path)} (SHA256 #{sha256}) on Ruby #{RUBY_VERSION} (#{RUBY_PLATFORM})"
if native && !Gem::Platform.match_gem?(hyper.spec.platform, hyper.spec.name)
  abort "#{platform} gem does not match this Ruby (#{Gem::Platform.local})"
end
# RubyGems builds nothing for a gem without extensions, so a platform gem
# installs without a compiler or Rust.
abort "#{platform} gem declares an extension build" if native && !hyper.spec.extensions.empty?
closure = []
unless native
  # The source gem depends on rb_sys, which depends on rake-compiler-dock.
  require "bundler"
  closure = Bundler.load.specs.select { |spec| %w[rb_sys rake-compiler-dock].include?(spec.name) }.map(&:cache_file)
  abort "Missing cached rb_sys closure: #{closure}" unless closure.size == 2 && closure.all? { File.file?(it) }
end

Dir.mktmpdir("connectrpc-hyper-install-") do |temporary|
  working = File.join(temporary, "cwd")
  gem_home = File.join(temporary, "gems")
  FileUtils.mkdir_p(working)
  core = PackageBuild.build(directory: root, output: working)
  FileUtils.cp([hyper.gem.path, File.join(__dir__, "smoke.rb"), *closure], working)
  environment = { "GEM_HOME" => gem_home, "GEM_PATH" => gem_home, "RUBYOPT" => nil, "RUBYLIB" => nil,
                  "RUBYGEMS_GEMDEPS" => nil, "CONNECTRPC_HYPER_PLATFORM" => platform }
  unless native
    environment.merge!("CARGO_HOME" => File.join(temporary, "cargo"), "CARGO_TARGET_DIR" => nil,
                       "RB_SYS_CARGO_TARGET_DIR" => nil, "CONNECTRPC_HYPER_LIB_DIR" => nil)
  end
  ENV.each_key { |key| environment[key] = nil if key.start_with?("BUNDLE_", "BUNDLER_") }
  run = ->(*args) { system(environment, RbConfig.ruby, *args, chdir: working, exception: true) }
  run.call("-S", "gem", "install", "--norc", "--local", "--no-document", File.basename(core),
           File.basename(hyper.gem.path))
  # A file, not `-e`: Windows argument quoting does not keep a multi-line
  # program intact. Real paths, because macOS temporary directories resolve
  # through /private.
  File.write(File.join(working, "installed.rb"), <<~'CODE')
    require "connectrpc/hyper"
    spec = Gem.loaded_specs.fetch("connectrpc-hyper")
    expected = ENV.fetch("CONNECTRPC_HYPER_PLATFORM")
    abort "Installed #{spec.platform}, expected #{expected}" unless spec.platform.to_s == expected
    gem_path = File.realpath(spec.full_gem_path)
    abort "Loaded outside GEM_HOME" unless gem_path.start_with?("#{File.realpath(ENV.fetch('GEM_HOME'))}/")
    minor = RUBY_VERSION[/\A\d+\.\d+/]
    loaded = $LOADED_FEATURES.grep(%r{/connectrpc_hyper\.(?:so|bundle)\z})
    abort "Expected one loaded extension, found #{loaded}" unless loaded.size == 1
    # Platform gems carry one extension per minor; a wrong one must not load.
    if expected != "ruby" && File.basename(File.dirname(loaded.first)) != minor
      abort "Loaded #{loaded.first}, not the Ruby #{minor} extension"
    end
    puts "Installed connectrpc-hyper #{spec.version} (#{spec.platform}) for Ruby #{RUBY_VERSION} " \
         "(#{RUBY_PLATFORM}): #{File.realpath(loaded.first).delete_prefix("#{gem_path}/")}"
  CODE
  run.call("installed.rb")
  run.call("smoke.rb")
end

if evidence
  FileUtils.mkdir_p(File.dirname(evidence))
  record = { "file" => File.basename(hyper.gem.path), "sha256" => sha256, "platform" => platform,
             "ruby" => RUBY_VERSION, "ruby_platform" => RUBY_PLATFORM }
  File.write(evidence, "#{JSON.pretty_generate(record)}\n")
  puts "Recorded smoke evidence in #{evidence}"
end
