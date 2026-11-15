# frozen_string_literal: true

# Runs one runnable example server or client in the bundle that owns it:
#   ruby scripts/example.rb server NAME
#   ruby scripts/example.rb client NAME [RPC [ARGS...]] [options]
#   ruby scripts/example.rb --list
require "rbconfig"
require_relative "../examples/demo/support"

root = DemoExample::ROOT
ruby = RbConfig.ruby
lane = ENV.fetch("RAILS_LANE", "8.1")
bundles = {
  root: { setup: "mise run setup", command: [ruby, "-S", "bundle", "_4.0.23_", "exec"] },
  rack: { setup: "mise run rack:setup", runner: "gems/connectrpc-rack/scripts/run.rb" },
  rails: { setup: "RAILS_LANE=#{lane} mise run setup:rails", runner: "scripts/rails.rb" },
  async: { setup: "mise run async:setup", runner: "gems/connectrpc-async/scripts/run.rb" },
  falcon: { setup: "mise run falcon:setup", runner: "gems/connectrpc-falcon/scripts/run.rb" }
}
# Each owning runner's `exec` reuses its bundle environment.
bundles.each_value { |bundle| bundle[:command] ||= [ruby, File.join(root, bundle[:runner]), "exec"] }
examples = {
  "server" => { "rack" => [:rack, "rack.rb"], "rails" => [:rails, "rails.rb"], "async" => [:async, "async.rb"],
                "falcon" => [:falcon, "falcon.rb"] },
  "client" => { "nethttp" => [:root, "nethttp.rb"], "async-h2" => [:async, "async_h2.rb"],
                "async-http1" => [:async, "async_http1.rb"] }
}
unless examples.fetch("server").keys == DemoExample::SERVERS.keys &&
       examples.fetch("client").keys == DemoExample::CLIENTS.keys
  abort "Launcher names differ from examples/demo/support.rb"
end

if ARGV.include?("--list")
  puts "Servers (mise run example:server -- NAME; PORT overrides #{DemoExample::DEFAULT_PORT}):"
  DemoExample::SERVERS.each do |name, served|
    puts "  #{name.ljust(12)} #{served.map { |http, protocols| "#{http} #{protocols.keys.join('+')}" }.join(', ')}"
  end
  puts "Clients (mise run example:client -- NAME [RPC [ARGS...]] [options]):"
  DemoExample::CLIENTS.each do |name, speaks|
    puts "  #{name.ljust(12)} #{speaks[:http]} #{speaks[:protocols].keys.join('+')}"
  end
  puts "RPCs: #{DemoExample::RPCS.map { |rpc, shape| "#{rpc} (#{DemoExample::SHAPES[shape]})" }.join(', ')}"
  puts "Options: --server NAME --url URL --codec proto|json --get --gzip --protocol connect|grpc"
  puts "See #{DemoExample::README}."
  exit
end

role, name, *arguments = ARGV
choices = examples[role]
abort "Usage: ruby scripts/example.rb server|client NAME [arguments] (or --list)" unless choices
unless choices.key?(name)
  abort "Unknown #{role} #{name.inspect}; choose one of: #{choices.keys.join(', ')} (see --list)"
end

bundle, file = choices.fetch(name)
command = bundles.fetch(bundle).fetch(:command)
# Loading bundler/setup fails when the owning bundle is missing or out of date.
unless system(*command, "ruby", "-e", "", out: File::NULL, err: File::NULL, chdir: root)
  abort "The #{bundle} bundle for the #{name} #{role} is not installed. Run: #{bundles.fetch(bundle).fetch(:setup)}"
end

exec(*command, "ruby", File.join(root, "examples/#{role}s", file), *arguments, chdir: root)
