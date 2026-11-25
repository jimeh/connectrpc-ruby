# frozen_string_literal: true

# Each example runs a fresh process, so its first close is the runtime's first
# use. The pthread_create shim refuses chosen extension threads, which start
# in this order: the runtime start thread, then the two workers.
RSpec.describe "connectrpc-hyper runtime start" do
  # The first close (bound 2 s) fails; a call is refused while the transport
  # stays closing; a second close, no longer refused and with the default
  # bound again, starts the runtime.
  def close_twice(allow, fail)
    env, log = fail_threads_environment(allow, fail)
    output, status = ruby_child(<<~RUBY, env: env, timeout: 30)
      transport = ConnectRPC::Hyper::Transport.new
      driver = transport.send(:driver)
      native = ConnectRPC::Hyper.const_get(:Native)
      native.runtime_start_bound(2)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      first = begin
        transport.close(grace: 1)
      rescue Exception => e # rubocop:disable Lint/RescueException
        [e.class.name, e.message]
      end
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      threads = File.readlines(#{log.dump}, chomp: true)
      outcome = driver.close_outcome
      admission = begin
        transport.send(:driver)
      rescue ConnectRPC::Error => e
        e.message
      end
      native.runtime_start_bound(5)
      closer = Thread.new { transport.close(grace: 1) }
      second = closer.join(10) ? closer.value : :still_waiting
      p [first, elapsed, threads, outcome, admission, second, driver.close_outcome]
    RUBY
    expect(status).to be_success
    first, elapsed, threads, outcome, admission, second, closed = eval(output.lines.last) # rubocop:disable Security/Eval
    # The retry starts a fresh start thread and both workers.
    expect(File.readlines(log, chomp: true)).to eq(threads + %w[start start start])
    expect([outcome, admission, second, closed]).to eq([nil, "HTTP transport admission is closed", true, true])
    [first, elapsed, threads]
  end

  it "raises when the OS refuses the start thread, and a later close starts the runtime" do
    first, _, threads = close_twice(0, 1)
    expect(threads).to eq(%w[refuse])
    expect(first).to eq(["fatal",
                         "OS can't spawn runtime start thread: Resource temporarily unavailable (os error 11)"])
  end

  it "reports a refused first worker as tokio does, and a later close starts the runtime" do
    first, _, threads = close_twice(1, 1)
    expect(threads).to eq(%w[start refuse])
    expect(first).to eq(["fatal", "OS can't spawn worker thread: Resource temporarily unavailable (os error 11)"])
  end

  # tokio 1.54's build never returns here (tokio-rs/tokio#8408). The child
  # also exits normally with that start thread stuck.
  it "raises within the bound when the OS refuses only the second worker, and a later close starts the runtime" do
    first, elapsed, threads = close_twice(2, 1)
    expect(threads).to eq(%w[start start refuse])
    expect(first).to eq(["fatal", "tokio runtime did not start within 2s"])
    expect(elapsed).to be < 5
  end
end
