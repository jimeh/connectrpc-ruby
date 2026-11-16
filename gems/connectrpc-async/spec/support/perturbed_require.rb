# frozen_string_literal: true

require "rbconfig"

# Requires a library in a fresh process after a perturbation that simulates an
# upstream change. Bounded: a child that outlives the deadline is killed and
# the example fails with its output.
module PerturbedRequire
  def require_perturbed(library, perturbation, seconds: 60)
    script = <<~RUBY
      $stdout.sync = true
      require "async"
      require "async/http/protocol/http2"
      require "async/http/endpoint"
      #{perturbation}
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      begin
        require #{library.dump}
        puts "loaded"
      rescue StandardError => e
        puts e.class.name, e.message
      end
      puts "require_seconds=\#{Process.clock_gettime(Process::CLOCK_MONOTONIC) - started}"
    RUBY
    reader, writer = IO.pipe
    pid = Process.spawn(RbConfig.ruby, "-e", script, out: writer, err: writer)
    writer.close
    waiter = Process.detach(pid)
    output = +""
    deadline = perturbed_now + seconds
    loop do
      remaining = deadline - perturbed_now
      break if remaining <= 0 || !reader.wait_readable(remaining)

      chunk = reader.read_nonblock(16_384, exception: false)
      break if chunk.nil?

      output << chunk if chunk.is_a?(String)
    end
    unless waiter.join([deadline - perturbed_now, 0].max)
      Process.kill(:KILL, pid)
      waiter.join
      raise "Perturbed require did not finish within #{seconds} s:\n#{output}"
    end
    output
  ensure
    reader&.close
  end

  def expect_contract_error(output, package, version, *lines)
    expect(output).to include("ConnectRPC::Async::UpstreamContractError")
    expect(output).to include("#{package} #{version} is incompatible with the installed socketry gems:")
    lines.each { |line| expect(output).to include(line) }
    expect(output).not_to include("loaded")
    elapsed = output[/require_seconds=([\d.]+)/, 1]
    expect(elapsed).not_to be_nil, output
    expect(Float(elapsed)).to be < AsyncHelpers::INTERNAL.const_get(:UpstreamContract)::PROBE_SECONDS
  end

  private

  def perturbed_now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
end
