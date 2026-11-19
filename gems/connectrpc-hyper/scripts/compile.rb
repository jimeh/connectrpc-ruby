# frozen_string_literal: true

# Compiles the extension for the running Ruby with the locked crate graph.
require "rbconfig"

lib_dir = ENV.fetch("CONNECTRPC_HYPER_LIB_DIR")
system(RbConfig.ruby, "-S", "rake", "compile", exception: true)
binary = Dir.glob(File.join(lib_dir, "connectrpc_hyper.{so,bundle,dll}")).fetch(0)
puts "Compiled #{binary} for Ruby #{RUBY_VERSION}"
