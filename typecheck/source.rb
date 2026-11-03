# frozen_string_literal: true

require "yaml"
require_relative "harness"

module SourceCheck
  FILES = %w[
    grpc grpc_client grpc_server method metadata error cancellation deadline protocol client server instrumentation
    compression message_io get_query
    streaming/client streaming/client_execution streaming/client_scope streaming/framing streaming/framing_io
    streaming/strategy streaming/method streaming/server streaming/server_execution streaming/server_terminal
    streaming/session
    streaming/session_completion streaming/session_io streaming/session_messages streaming/streams streaming/terminal
  ].map do |name|
    "lib/connectrpc/#{name}.rb"
  end.push("lib/connectrpc.rb", "lib/connectrpc/version.rb").sort.freeze

  def self.check(root)
    configuration = File.read(File.join(root, "typecheck/Steepfile"))
    unless configuration.include?("configure_code_diagnostics(Steep::Diagnostic::Ruby.strict)")
      raise "Core implementation diagnostics must remain strict"
    end

    tools = [RbConfig.ruby, "-S", "steep"]
    project = RBSHarness.capture!([*tools, "project", "--steepfile=typecheck/Steepfile", "--print-files"],
                                  working: root)
    targets = YAML.safe_load(project).fetch("targets")
    files = targets.flat_map { |target| target.fetch("source_paths", []) }.map do |file|
      Pathname(File.expand_path(file, File.join(root, "typecheck"))).relative_path_from(Pathname(root)).to_s
    end.sort
    raise "Wrong strict source set: #{files.inspect}" unless files == FILES

    # Steep's parser can omit a syntax-invalid source; require Ruby parsing too.
    FILES.each { |file| RBSHarness.capture!([RbConfig.ruby, "-c", file], working: root) }
    puts RBSHarness.capture!([*tools, "check", "--jobs=1", "--no-daemon", "--steepfile=typecheck/Steepfile"],
                             working: root)
    puts "Strict implementation source set verified: #{FILES.join(', ')}"
  end
end

SourceCheck.check(File.expand_path("..", __dir__)) if $PROGRAM_NAME == __FILE__
