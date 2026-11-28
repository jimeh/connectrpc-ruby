# frozen_string_literal: true

require "json"
require "net/http"
require "uri"
require_relative "ci_scope"

# Decides the aggregate `CI result` check. evaluate is pure: it takes the
# `needs` context and the run's job list. Api reads that list and main
# re-reads it, within a bounded deadline, until every other job completed.
module CIResult
  NAME = "CI result"
  ACCEPTED_UNSELECTED = %w[skipped success].freeze
  ACCEPTED_CONCLUSIONS = %w[success skipped].freeze

  Row = Struct.new(:job, :selected, :result, :ok, keyword_init: true)
  Verdict = Struct.new(:rows, :errors, keyword_init: true) do
    def ok? = errors.empty?
  end

  module_function

  # needs: the parsed toJSON(needs). jobs: the run's jobs from the REST API.
  def evaluate(needs, jobs)
    selected, scope_error = selected_jobs(needs["scope"])
    job_ids = needs.keys - ["scope"]
    rows = job_ids.map { |job| row(job, needs[job], selected) }
    errors = [*scope_error, *needs_errors(job_ids, selected)]
    errors += row_errors(rows) + verified_errors(needs, selected) + run_errors(jobs)
    Verdict.new(rows: rows, errors: errors)
  end

  # The selected job ids, or nil and the reason the selection is unusable.
  def selected_jobs(scope)
    result = scope.is_a?(Hash) ? scope["result"] : "missing"
    return [nil, "CI scope did not succeed (#{result})"] unless result == "success"

    jobs = JSON.parse(scope.dig("outputs", "jobs").to_s)
    valid = jobs.is_a?(Array) && jobs.all?(String) && jobs.include?("static")
    valid ? [jobs, nil] : [nil, "CI scope jobs output is not a job list that includes static"]
  rescue JSON::ParserError
    [nil, "CI scope jobs output is not JSON"]
  end

  def needs_errors(job_ids, selected)
    missing = CIScope::JOBS.keys - job_ids
    extra = job_ids - CIScope::JOBS.keys
    unknown = (selected || []) - job_ids
    [("needs lacks #{missing.join(', ')}" unless missing.empty?),
     ("needs has unknown jobs #{extra.join(', ')}" unless extra.empty?),
     ("selected jobs missing from needs: #{unknown.join(', ')}" unless unknown.empty?)].compact
  end

  # Without a valid selection every job is treated as selected.
  def row(job, need, selected)
    result = need.is_a?(Hash) ? need["result"] : nil
    chosen = selected.nil? || selected.include?(job)
    ok = chosen ? result == "success" : ACCEPTED_UNSELECTED.include?(result)
    Row.new(job: job, selected: chosen, result: result, ok: ok)
  end

  def row_errors(rows) = rows.reject(&:ok).map { |entry| "#{entry.job}: #{entry.result || 'no result'}" }

  # A reusable workflow can report success while one of its legs did not;
  # its own Verified job sets this output only when every leg succeeded.
  def verified_errors(needs, selected)
    return [] unless selected&.include?("hyper_gems")
    return [] if needs.dig("hyper_gems", "outputs", "verified") == "true"

    ["hyper_gems did not report verified=true"]
  end

  # needs.<job>.result can read success while an expanded matrix leg was
  # cancelled, so the run's own job list is checked too.
  def run_errors(jobs)
    others(jobs).filter_map do |job|
      if job["status"] != "completed"
        "#{job['name']}: #{job['status']}"
      elsif !ACCEPTED_CONCLUSIONS.include?(job["conclusion"])
        "#{job['name']}: #{job['conclusion']}"
      end
    end
  end

  def others(jobs) = jobs.reject { |job| job["name"] == NAME }

  def pending?(jobs) = others(jobs).any? { |job| job["status"] != "completed" }

  # Re-reads the job list until no other job is pending or the deadline passes.
  def settled_jobs(api, clock:, sleeper:, timeout: 60, interval: 5)
    deadline = clock.call + timeout
    jobs = api.jobs
    while pending?(jobs) && clock.call < deadline
      sleeper.call(interval)
      jobs = api.jobs
    end
    jobs
  end

  def table(verdict, jobs)
    lines = ["| Job | Selected | Result | Verdict |", "| --- | --- | --- | --- |"]
    lines += verdict.rows.map do |entry|
      "| #{entry.job} | #{entry.selected ? 'yes' : 'no'} | #{entry.result || 'none'} | #{entry.ok ? 'ok' : 'FAIL'} |"
    end
    lines += ["", "| Run job | Status | Conclusion |", "| --- | --- | --- |"]
    lines += others(jobs).map do |job|
      "| #{job['name'].to_s.gsub('|', '\|')} | #{job['status']} | #{job['conclusion']} |"
    end
    lines.join("\n")
  end

  def report(verdict, jobs)
    status = verdict.ok? ? "passed" : "failed"
    errors = verdict.errors.map { |error| "- #{error}" }
    ["## CI result: #{status}", "", *errors, ("" unless errors.empty?), table(verdict, jobs)].compact.join("\n")
  end

  # Lists a workflow run's latest job attempts through the REST API.
  class Api
    def initialize(env)
      @token = env.fetch("GH_TOKEN")
      @base = URI(env.fetch("GITHUB_API_URL", "https://api.github.com"))
      @first = URI("#{@base}/repos/#{env.fetch('GITHUB_REPOSITORY')}/actions/runs/" \
                   "#{env.fetch('GITHUB_RUN_ID')}/jobs?filter=latest&per_page=100")
    end

    def jobs
      url = @first
      jobs = []
      while url
        response = get(url)
        raise "GitHub API #{response.code} for #{url}" unless response.is_a?(Net::HTTPSuccess)

        jobs.concat(JSON.parse(response.body).fetch("jobs"))
        url = CIResult.next_link(response["link"], @base)
      end
      jobs
    end

    private

    def get(url)
      request = Net::HTTP::Get.new(url)
      request["Authorization"] = "Bearer #{@token}"
      request["Accept"] = "application/vnd.github+json"
      request["X-GitHub-Api-Version"] = "2022-11-28"
      Net::HTTP.start(url.host, url.port, use_ssl: url.scheme == "https", open_timeout: 10, read_timeout: 30) do |http|
        http.request(request)
      end
    end
  end

  # The rel="next" URL of a Link header, refusing other hosts so the token
  # stays with the API.
  def next_link(header, base)
    target = header.to_s.split(",").find { |part| part.match?(/;\s*rel="next"/) }
    return nil unless target

    url = URI(target[/<([^>]+)>/, 1])
    raise "Refusing pagination link to #{url.host}" unless url.scheme == base.scheme && url.host == base.host

    url
  end

  def main(env = ENV, out: $stdout, api: Api.new(env), clock: -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) },
           sleeper: ->(seconds) { sleep(seconds) })
    needs = JSON.parse(env.fetch("NEEDS"))
    jobs = settled_jobs(api, clock: clock, sleeper: sleeper)
    verdict = evaluate(needs, jobs)
    text = report(verdict, jobs)
    out.puts text
    summary = env["GITHUB_STEP_SUMMARY"]
    File.open(summary, "a") { |file| file.puts(text) } if summary && !summary.empty?
    verdict
  end
end

exit(CIResult.main.ok? ? 0 : 1) if $PROGRAM_NAME == __FILE__
