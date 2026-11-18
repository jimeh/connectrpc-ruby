# frozen_string_literal: true

require "rbconfig"
require_relative "../../../scripts/dependency_floors"

root = File.expand_path("../../..", __dir__)
package = File.join(root, "gems/connectrpc-hyper")
environment = {
  "RUBOCOP_CACHE_ROOT" => File.join(root, "vendor/hyper/rubocop"),
  "BUNDLE_GEMFILE" => DependencyFloors.gemfile(File.join(package, "Gemfile")),
  "BUNDLE_LOCKFILE" => nil, "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_COOLDOWN" => "3", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_PATH" => File.join(root, "vendor/hyper/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_USER_HOME" => File.join(root, "vendor/hyper/bundler"),
  # Development builds keep one extension per Ruby minor, as platform gems do.
  "CONNECTRPC_HYPER_LIB_DIR" => "lib/connectrpc/hyper/#{RUBY_VERSION[/\A\d+\.\d+/]}",
  "RB_SYS_CARGO_TARGET_DIR" => File.join(root, "vendor/hyper/target", RUBY_VERSION)
}
# Some distribution libclang packages omit clang's resource headers, which
# rb-sys's bindgen needs; the C compiler's own include directory supplies them.
if RbConfig::CONFIG["host_os"].include?("linux") && !ENV.key?("BINDGEN_EXTRA_CLANG_ARGS") &&
   Dir.glob("/usr/lib/llvm-*/lib/clang/*/include/stddef.h").empty?
  include_dir = IO.popen(%w[cc -print-file-name=include], err: File::NULL, &:read).to_s.strip
  environment["BINDGEN_EXTRA_CLANG_ARGS"] = "-I#{include_dir}" if File.file?(File.join(include_dir, "stddef.h"))
end
abort "Preinstall Bundler 4.0.23" if Gem::Specification.find_all_by_name("bundler", "= 4.0.23").empty?
command = ARGV.shift
arguments = case command
            when "setup" then ["exec", "ruby", "scripts/compile.rb"]
            when "install" then ["install"]
            when "floors" then ["exec", "ruby", File.join(root, "scripts/dependency_floors.rb")]
            when "test" then ["exec", "rspec", *ARGV]
            when "check" then ["exec", "ruby", "scripts/check.rb"]
            when "build", "package", "interop" then ["exec", "ruby", "scripts/#{command}.rb", *ARGV]
            else abort "Usage: run.rb install|setup|floors|test|check|build|package|interop"
            end
puts "Hyper package; Ruby #{RUBY_VERSION}; Bundler 4.0.23; Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
$stdout.flush
if command == "setup"
  system(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", "install", chdir: package, exception: true)
end
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: package)
