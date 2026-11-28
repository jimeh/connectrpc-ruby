# frozen_string_literal: true

require "json"
require "stringio"
require "tmpdir"
require_relative "../scripts/ci_result"

RSpec.describe CIResult do
  let(:selected) { %w[static specs hyper hyper_gems] }
  let(:run_jobs) do
    [{ "name" => "CI scope", "status" => "completed", "conclusion" => "success" },
     { "name" => "Specs / Ruby 3.4.12", "status" => "completed", "conclusion" => "success" },
     { "name" => "Package / Ruby ${{ matrix.ruby }}", "status" => "completed", "conclusion" => "skipped" },
     { "name" => "Hyper gems / Verified", "status" => "completed", "conclusion" => "success" },
     { "name" => "CI result", "status" => "in_progress", "conclusion" => nil }]
  end

  def needs(selection = selected, results: {}, scope: nil, verified: "true")
    jobs = CIScope::JOBS.keys.to_h do |job|
      result = results.fetch(job) { selection.include?(job) ? "success" : "skipped" }
      [job, { "result" => result, "outputs" => job == "hyper_gems" && verified ? { "verified" => verified } : {} }]
    end
    { "scope" => scope || { "result" => "success", "outputs" => { "jobs" => JSON.generate(selection) } }, **jobs }
  end

  def evaluate(context, jobs = run_jobs) = described_class.evaluate(context, jobs)

  it "passes when every selected job succeeded and every other job was skipped" do
    verdict = evaluate(needs)
    expect(verdict.errors).to eq([])
    expect(verdict.rows.size).to eq(CIScope::JOBS.size)
    rack = verdict.rows.find { |row| row.job == "rack" }
    expect(rack).to have_attributes(selected: false, result: "skipped", ok: true)
  end

  it "accepts an unselected job that succeeded" do
    expect(evaluate(needs(selected, results: { "rack" => "success" }))).to be_ok
  end

  %w[failure cancelled skipped].each do |result|
    it "fails when a selected job's result is #{result}" do
      expect(evaluate(needs(selected, results: { "specs" => result })).errors).to eq(["specs: #{result}"])
    end
  end

  %w[failure cancelled].each do |result|
    it "fails when an unselected job's result is #{result}" do
      expect(evaluate(needs(selected, results: { "rack" => result })).errors).to eq(["rack: #{result}"])
    end
  end

  it "fails, treating every job as selected, unless the scope succeeded" do
    context = needs(selected, scope: { "result" => "failure", "outputs" => {} })
    verdict = evaluate(context)
    expect(verdict.errors).to include("CI scope did not succeed (failure)", "rack: skipped")
    expect(verdict.rows).to all(have_attributes(selected: true))
    expect(evaluate(needs.except("scope")).errors).to include("CI scope did not succeed (missing)")
  end

  it "fails for a scope output that is not a JSON job list including static" do
    ["", "not json", "{}", "[1]", '["specs"]'].each do |output|
      context = needs(selected, scope: { "result" => "success", "outputs" => { "jobs" => output } })
      expect(evaluate(context)).not_to be_ok, output
    end
    context = needs(selected, scope: { "result" => "success", "outputs" => {} })
    expect(evaluate(context).errors).to include("CI scope jobs output is not JSON")
  end

  it "fails when needs lacks a mapped job, has an unknown job, or misses a selected job" do
    expect(evaluate(needs.except("examples")).errors).to include("needs lacks examples")
    expect(evaluate(needs.merge("extra" => { "result" => "skipped" })).errors)
      .to include("needs has unknown jobs extra")
    expect(evaluate(needs([*selected, "ghost"])).errors).to include("selected jobs missing from needs: ghost")
  end

  it "requires the hyper archive workflow to report verified when it was selected" do
    expect(evaluate(needs(selected, verified: nil)).errors).to eq(["hyper_gems did not report verified=true"])
    expect(evaluate(needs(selected, verified: "false"))).not_to be_ok
    expect(evaluate(needs(%w[static], verified: nil))).to be_ok
  end

  it "fails for a run job that did not finish or ended other than success or skipped" do
    cancelled = run_jobs + [{ "name" => "Lowest allowed hyper / Ruby 3.4.12", "status" => "completed",
                              "conclusion" => "cancelled" }]
    expect(evaluate(needs, cancelled).errors).to eq(["Lowest allowed hyper / Ruby 3.4.12: cancelled"])
    queued = run_jobs + [{ "name" => "Hyper gems / Build", "status" => "queued", "conclusion" => nil }]
    expect(evaluate(needs, queued).errors).to eq(["Hyper gems / Build: queued"])
  end

  describe "re-run attempts" do
    let(:leg) { { "name" => "Lowest allowed hyper / Ruby 3.4.12", "status" => "completed" } }
    let(:first_attempt) do
      [*run_jobs.each_with_index.map { |job, index| job.merge("run_attempt" => 1, "id" => index + 1) },
       leg.merge("run_attempt" => 1, "id" => 10, "conclusion" => "cancelled")]
    end
    # A partial re-run of Specs alone: the cancelled leg has no attempt 2 record.
    let(:rerun) { [*first_attempt, run_jobs[1].merge("run_attempt" => 2, "id" => 20)] }

    it "fails for a leg cancelled in an earlier attempt that the re-run did not repeat" do
      expect(evaluate(needs, rerun).errors).to eq(["Lowest allowed hyper / Ruby 3.4.12: cancelled"])
    end

    it "passes once a re-run attempt of the cancelled leg succeeded" do
      retried = [*rerun, leg.merge("run_attempt" => 2, "id" => 21, "conclusion" => "success")]
      expect(evaluate(needs, retried)).to be_ok
    end

    it "keeps only each job name's records from its newest attempt" do
      jobs = [{ "name" => "A", "run_attempt" => 1, "id" => 1 }, { "name" => "A", "run_attempt" => 3, "id" => 7 },
              { "name" => "A", "run_attempt" => 2, "id" => 9 }, { "name" => "B", "run_attempt" => 1, "id" => 2 },
              { "name" => "B", "run_attempt" => 1, "id" => 3 }]
      expect(described_class.latest_attempts(jobs).map { |job| job["id"] }).to eq([7, 2, 3])
    end
  end

  describe ".settled_jobs" do
    let(:clock) do
      now = [0]
      Struct.new(:now) do
        def call = now.first
        def advance(seconds) = now[0] += seconds
      end.new(now)
    end

    def api(*pages)
      Struct.new(:pages, :calls) do
        def jobs
          self.calls += 1
          pages.size > 1 ? pages.shift : pages.first
        end
      end.new(pages, 0)
    end

    let(:pending) { run_jobs + [{ "name" => "Hyper gems / Build", "status" => "in_progress" }] }

    it "re-reads the job list until no other job is pending" do
      source = api(pending, pending, run_jobs)
      slept = []
      jobs = described_class.settled_jobs(source, clock: clock, sleeper: lambda { |seconds|
        slept << seconds
        clock.advance(seconds)
      })
      expect(jobs).to eq(run_jobs)
      expect(source.calls).to eq(3)
      expect(slept).to eq([5, 5])
    end

    it "returns the last observed state once the deadline passes" do
      source = api(pending)
      jobs = described_class.settled_jobs(source, clock: clock, sleeper: ->(seconds) { clock.advance(seconds) })
      expect(jobs).to eq(pending)
      expect(source.calls).to eq(13)
      expect(evaluate(needs, jobs).errors).to eq(["Hyper gems / Build: in_progress"])
    end

    it "ignores a pending record that a newer attempt of the same job replaced" do
      old = pending.last.merge("run_attempt" => 1, "id" => 1)
      source = api([old, old.merge("run_attempt" => 2, "id" => 2, "status" => "completed", "conclusion" => "success")])
      described_class.settled_jobs(source, clock: clock, sleeper: ->(seconds) { clock.advance(seconds) })
      expect(source.calls).to eq(1)
    end

    it "does not re-read when only CI result itself is still running" do
      source = api(run_jobs)
      described_class.settled_jobs(source, clock: clock, sleeper: ->(_seconds) { raise "slept" })
      expect(source.calls).to eq(1)
    end
  end

  describe CIResult::Api do
    def response(jobs, link = nil)
      Net::HTTPOK.new("1.1", "200", "OK").tap do |page|
        page.instance_variable_set(:@body, JSON.generate("jobs" => jobs))
        page.instance_variable_set(:@read, true)
        page["link"] = link if link
      end
    end

    it "lists every attempt's jobs across pages" do
      api = described_class.new("GH_TOKEN" => "token", "GITHUB_REPOSITORY" => "owner/repo", "GITHUB_RUN_ID" => "7")
      second = "https://api.github.com/repositories/1/actions/runs/7/jobs?filter=all&per_page=100&page=2"
      pages = { 1 => response([{ "id" => 1 }], "<#{second}>; rel=\"next\""), 2 => response([{ "id" => 2 }]) }
      requested = []
      api.define_singleton_method(:get) do |url|
        requested << url.to_s
        pages.fetch(requested.size)
      end
      expect(api.jobs).to eq([{ "id" => 1 }, { "id" => 2 }])
      expect(requested).to eq(["https://api.github.com/repos/owner/repo/actions/runs/7/jobs?filter=all&per_page=100",
                               second])
    end
  end

  describe ".next_link" do
    let(:base) { URI("https://api.github.com") }

    it "follows rel=next on the API host and stops without one" do
      header = '<https://api.github.com/repositories/1/actions/runs/2/jobs?page=2>; rel="next", ' \
               '<https://api.github.com/repositories/1/actions/runs/2/jobs?page=3>; rel="last"'
      expect(described_class.next_link(header, base).to_s).to end_with("jobs?page=2")
      expect(described_class.next_link('<https://api.github.com/x?page=1>; rel="prev"', base)).to be_nil
      expect(described_class.next_link(nil, base)).to be_nil
    end

    it "refuses a pagination link to another host" do
      expect { described_class.next_link('<https://example.invalid/jobs?page=2>; rel="next"', base) }
        .to raise_error(RuntimeError, /example\.invalid/)
    end
  end

  it "prints and summarizes the verdict from the NEEDS environment" do
    Dir.mktmpdir("connectrpc-ci-result-") do |directory|
      summary = File.join(directory, "summary")
      out = StringIO.new
      api = Struct.new(:jobs).new(run_jobs)
      env = { "NEEDS" => JSON.generate(needs(selected, results: { "specs" => "failure" })),
              "GITHUB_STEP_SUMMARY" => summary }
      verdict = described_class.main(env, out: out, api: api, clock: -> { 0 }, sleeper: ->(_seconds) {})
      expect(verdict).not_to be_ok
      expect(out.string).to include("## CI result: failed", "- specs: failure", "| specs | yes | failure | FAIL |",
                                    "| rack | no | skipped | ok |", "| Package / Ruby ${{ matrix.ruby }} | completed |")
      expect(out.string).not_to include("| CI result |")
      expect(File.read(summary)).to eq(out.string)
    end
  end
end
