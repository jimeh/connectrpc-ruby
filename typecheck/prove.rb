# frozen_string_literal: true

require "tmpdir"
require_relative "harness"
require_relative "capture"

root = File.expand_path("..", __dir__)
output = File.join(root, "vendor/rbs/proof", RUBY_VERSION)
FileUtils.mkdir_p(output)
Dir.mktmpdir("source-", output) do |temporary|
  capture = File.join(temporary, "capture")
  before = SourceCapture.create(root, capture)
  command = [RbConfig.ruby, "-S", "steep", "check", "--jobs=1", "--no-daemon", "--steepfile=typecheck/Steepfile"]
  puts RBSHarness.capture!([RbConfig.ruby, "typecheck/source.rb"], working: capture)
  perturbations = {
    "return" => ["lib/connectrpc/method.rb", "def cardinality = :unary", 'def cardinality = "unary"',
                 "Ruby::MethodBodyTypeMismatch"],
    "data" => ["lib/connectrpc/server.rb", 'headers: headers, body: "")', "headers: headers, body: 42)",
               "Ruby::UnresolvedOverloading"],
    "callback" => ["lib/connectrpc/instrumentation.rb", "callbacks.finished&.call(completion)",
                   'callbacks.finished&.call("bad callback data")', "Ruby::ArgumentTypeMismatch"]
  }
  perturbations.each do |name, (file, original, replacement, expected)|
    target = File.join(capture, file)
    bytes = File.binread(target)
    raise "Perturbation anchor not unique: #{name}" unless bytes.scan(original).size == 1

    begin
      File.binwrite(target, bytes.sub(original, replacement))
      log, status = Open3.capture2e(*command, chdir: capture)
      File.write(File.join(output, "#{name}.log"), log)
      actual = log.scan(/Diagnostic ID: (\S+)/).flatten
      unless status.exitstatus == 1 && actual == [expected] && log.include?(file.delete_prefix("lib/"))
        raise "Wrong #{name} perturbation diagnostic: #{actual.inspect}\n#{log}"
      end

      puts "#{name}: exactly #{expected} at #{file}"
    ensure
      File.binwrite(target, bytes)
    end
  end
  {
    "addition" => ["lib/connectrpc/method.rb", nil,
                   "\nmodule ConnectRPC\n  class Method\n    def unclassified_probe = nil\n  end\nend\n",
                   "Unclassified source definition"],
    "omission" => ["sig/connectrpc.rbs", "    def cardinality: () -> :unary\n", "", "Missing public signature"]
  }.each do |name, (file, original, replacement, expected)|
    target = File.join(capture, file)
    bytes = File.binread(target)
    begin
      File.binwrite(target, original ? bytes.sub(original, replacement) : bytes + replacement)
      log, status = Open3.capture2e(RbConfig.ruby, "typecheck/surface.rb", chdir: capture)
      File.write(File.join(output, "drift-#{name}.log"), log)
      unless status.exitstatus == 1 && log.include?(expected)
        raise "Drift probe #{name} did not fail at #{expected}: #{log}"
      end

      puts "drift #{name}: #{expected}"
    ensure
      File.binwrite(target, bytes)
    end
  end
  puts RBSHarness.capture!([RbConfig.ruby, "typecheck/source.rb"], working: capture)
  puts RBSHarness.capture!([RbConfig.ruby, "typecheck/surface.rb"], working: capture)
  after = SourceCapture.snapshot(capture)
  executable = "exe/protoc-gen-connectrpc-ruby"
  after.fetch(executable)["mode"] = before.fetch(executable).fetch("mode")
  raise "Capture was not restored exactly" unless after == before
  raise "Delivery source changed during probes" unless SourceCapture.snapshot(root) == before
end
puts "Independent-inode strict source and drift red/green proofs passed; delivery bytes/modes unchanged"
