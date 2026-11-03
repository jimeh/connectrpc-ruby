# frozen_string_literal: true

require "tmpdir"
require_relative "../typecheck/harness"

RSpec.describe RBSNegativeCheck do
  let(:expected) do
    { "api/negative/first.rb" => ["Ruby::NoMethod"],
      "api/negative/second.rb" => ["Ruby::ArgumentTypeMismatch"] }
  end
  let(:records) do
    expected.map do |file, diagnostics|
      "::error file=#{file},line=3,endLine=3,col=0,endColumn=5::[#{diagnostics.first}] Wrong type"
    end
  end

  def transcript(records, progress: "FF", summary: "Detected 2 problems from 2 files")
    ["# Type checking files:", "", progress, "", *records, summary, ""].join("\n")
  end

  it "requires each expected diagnostic at its exact source path, independent of notification order" do
    actual = described_class.diagnostics(transcript(records.reverse), expected: expected)
    expect(actual).to eq(expected)
  end

  it "keeps escaped newlines and annotation-looking diagnostic text inside its raw record" do
    forged = "%0A#{records.last}%0D%25"
    output = transcript([records.first + forged, records.last])
    expect(described_class.diagnostics(output, expected: expected)).to eq(expected)
    expect do
      described_class.diagnostics(transcript([records.first + forged]), expected: expected)
    end.to raise_error(/Incomplete/)
  end

  it "rejects missing, extra, duplicated, relocated and wrong diagnostics" do
    variants = [
      records.first(1), records + [records.first], [records.first, records.first],
      [records.first.sub("first.rb", "unknown.rb"), records.last],
      [records.first.sub("Ruby::NoMethod", "Ruby::ArgumentTypeMismatch"), records.last],
      [records.first.sub("first.rb", "../first.rb"), records.last],
      [records.first.sub("first.rb", "first,other.rb"), records.last],
      [records.first.sub("first.rb", "first%0A.rb"), records.last],
      [records.first.sub("::error", "::warning"), records.last],
      [records.first.sub("::error", "::notice"), records.last],
      [records.first.sub("::error", "::unknown"), records.last],
      [records.first.sub("col=0", "col=bad"), records.last],
      ["prefix #{records.first}", records.last], ["#{records.first}\rcrash", records.last]
    ]
    variants.each do |entries|
      expect do
        described_class.diagnostics(transcript(entries), expected: expected)
      end.to raise_error(RuntimeError), entries.inspect
    end
  end

  it "rejects incomplete progress, collection failures, crash output and mismatched summaries" do
    variants = [
      transcript(records, progress: "F."), transcript(records, summary: "Unexpected error reported."),
      transcript(records, summary: "Detected 3 problems from 2 files"),
      "#{transcript(records)}Steep shutdown with an error\n", "compiler failed\n#{transcript(records)}",
      transcript(records).delete_suffix("Detected 2 problems from 2 files\n")
    ]
    variants.each do |output|
      expect { described_class.diagnostics(output, expected: expected) }.to raise_error(/Incomplete/)
    end
  end

  def with_consumers
    Dir.mktmpdir("connectrpc-negative-spec-") do |working|
      FileUtils.mkdir_p(File.join(working, "api/negative"))
      expected.each_key { |file| File.write(File.join(working, file), "nil\n") }
      yield working
    end
  end

  def check(working, output: transcript(records), exit_code: 1, cases: nil)
    cases ||= expected.to_h { |file, diagnostics| [File.basename(file, ".rb"), diagnostics.first] }
    emitter = [RbConfig.ruby, "-e", "STDOUT.write(#{output.dump}); exit #{exit_code}", "--"]
    described_class.check(command: emitter, working: working, directory: "api/negative",
                          cases: cases, log: "negative.log")
  end

  it "requires a normal exit 1 and retains the complete failed checker output" do
    with_consumers do |working|
      [0, 2].each do |exit_code|
        expect { check(working, exit_code: exit_code) }.to raise_error(/did not exit normally with 1/)
      end
      output = "#{transcript(records)}crash\n"
      expect { check(working, output: output) }.to raise_error(/Incomplete/)
      expect(File.read(File.join(working, "negative.log"))).to eq(output)
      expect(check(working).map { |entry| entry.fetch(:case) }).to eq(%w[first second])
    end
  end

  it "rejects missing, extra, nested and symlinked consumer files before checking" do
    with_consumers do |working|
      first = File.join(working, "api/negative/first.rb")
      File.unlink(first)
      expect { check(working) }.to raise_error(/inventory mismatch/)
      File.symlink(File.join(working, "api/negative/second.rb"), first)
      expect { check(working) }.to raise_error(/inventory mismatch/)
      File.unlink(first)
      File.write(first, "nil\n")
      extra = File.join(working, "api/negative/extra.rb")
      File.write(extra, "nil\n")
      expect { check(working) }.to raise_error(/inventory mismatch/)
      File.unlink(extra)
      FileUtils.mkdir_p(File.join(working, "api/negative/nested"))
      File.write(File.join(working, "api/negative/nested/extra.rb"), "nil\n")
      expect { check(working) }.to raise_error(/inventory mismatch/)
      expect(File.exist?(File.join(working, "negative.log"))).to be(false)
    end
  end

  it "rejects empty or ambiguous controlled fixture names" do
    with_consumers do |working|
      [{}, { "../first" => "Ruby::NoMethod" }, { "first,second" => "Ruby::NoMethod" }].each do |cases|
        expect { check(working, cases: cases) }.to raise_error(/Invalid negative consumer manifest/)
      end
    end
  end
end
