# frozen_string_literal: true

require "bundler"
require "rbconfig"

# Expected OpenSSL and JSON releases for the copied Async/Falcon integrations,
# derived from what the development bundle resolved.
module Activation
  NAMES = %w[openssl json].freeze

  # The development bundle activates exactly what it resolved. A fresh
  # GEM_HOME activates the highest installed release instead, so a newer Ruby
  # default gem wins over an older resolved floor there. `install_commands`
  # installs the resolved archives explicitly, because `gem install` treats a
  # default gem that satisfies a requirement as already installed.
  def self.environment(installed:)
    specs = Bundler.load.specs
    NAMES.to_h do |name|
      spec = specs.find { |entry| entry.name == name } || abort("Bundle did not resolve #{name}")
      versions = [spec.version]
      versions.concat(Gem::Specification.default_stubs("#{name}-*.gemspec").map(&:version)) if installed
      ["CONNECTRPC_EXPECTED_#{name.upcase}", versions.max.to_s]
    end
  end

  def self.archive(name)
    spec = Bundler.load.specs.find { |entry| entry.name == name }
    abort "Missing offline archive #{name}" unless spec && File.file?(spec.cache_file)

    spec.cache_file
  end

  # Offline installs of the resolved archives, copied into +working+ beforehand.
  def self.install_commands(working)
    NAMES.map do |name|
      [RbConfig.ruby, "-S", "gem", "install", "--norc", File.join(working, File.basename(archive(name))),
       "--local", "--no-document"]
    end
  end
end
