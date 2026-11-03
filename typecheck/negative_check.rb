# frozen_string_literal: true

require "open3"

module RBSNegativeCheck
  def self.check(command:, working:, directory:, cases:, log:)
    expected, files = inventory(working: working, directory: directory, cases: cases)

    # Steep constructs a fresh inference context for each source file. Keep the
    # files separate while sharing this invocation's signature loading/startup.
    output, status = Open3.capture2e(*command, "--no-daemon", "--format=github", *files, chdir: working)
    File.write(File.join(working, log), output)
    raise "Negative checker did not exit normally with 1: #{status.inspect}\n#{output}" unless status.exitstatus == 1

    actual = diagnostics(output, expected: expected)
    cases.map { |name, _| { case: name, diagnostics: actual.fetch("#{directory}/#{name}.rb") } }
  end

  def self.diagnostics(output, expected:)
    lines = output.lines(chomp: true)
    count = expected.size
    prefix = ["# Type checking files:", "", "F" * count, ""]
    summary = count == 1 ? "Detected 1 problem from 1 file" : "Detected #{count} problems from #{count} files"
    unless lines.shift(4) == prefix && lines.pop == summary && lines.size == count
      raise "Incomplete negative checker output\n#{output}"
    end

    actual = {}
    lines.each do |line|
      # Parse raw record boundaries before inspecting messages. The pinned
      # formatter escapes message newlines, but interpolates paths literally.
      record = line.match(%r{\A::error\x20file=([a-z_/]+\.rb),line=[1-9]\d*,endLine=[1-9]\d*,
                             col=\d+,endColumn=\d+::\[(Ruby::[A-Za-z]+)\]\x20[^\r\n]+\z}x)
      raise "Unrecognized negative diagnostic record: #{line.inspect}" unless record

      file, diagnostic = record.captures
      raise "Unexpected or duplicate negative source: #{file}" unless expected.key?(file) && !actual.key?(file)

      actual[file] = [diagnostic]
    end
    raise "Negative diagnostics mismatch: expected #{expected.inspect}, got #{actual.inspect}" unless actual == expected

    actual
  end

  def self.inventory(working:, directory:, cases:)
    unless directory.match?(%r{\A[a-z_]+/negative\z}) && !cases.empty? &&
           cases.keys.all? { |name| name.match?(/\A[a-z_]+\z/) }
      raise "Invalid negative consumer manifest"
    end

    expected = cases.to_h { |name, diagnostic| ["#{directory}/#{name}.rb", [diagnostic]] }
    files = Dir.glob("#{directory}/**/*.rb", base: working).sort
    unless files == expected.keys.sort && files.all? do |file|
      absolute = File.expand_path(file, working)
      File.file?(absolute) && File.realpath(absolute) == absolute
    end
      raise "Negative consumer inventory mismatch: expected #{expected.keys.sort.inspect}, found #{files.inspect}"
    end

    [expected, files]
  end
end
