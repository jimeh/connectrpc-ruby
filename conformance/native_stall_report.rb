# frozen_string_literal: true

module Conformance
  # The runner kills a server adapter that has not exited 5s after SIGTERM,
  # which leaves no trace of where it stuck. Once armed by SIGTERM, this writes
  # the reactor thread's backtrace and every Async task's fiber backtrace to
  # stderr if the adapter has not recorded server_stopped within GRACE.
  class NativeStallReport
    GRACE = 3
    # Small enough that the reactor section survives the scripts' stderr tail
    # with a few dozen live tasks.
    TASK_FRAMES = 8

    attr_writer :scheduler, :phase

    def initialize
      @phase = :serving
    end

    # Runs inside the TERM trap, which may not take a Mutex.
    def arm(main)
      return if @armed

      @armed = true
      Thread.new do
        sleep GRACE
        $stderr.write(report(main)) unless @phase == :stopped
      end
    end

    private

    def report(main)
      out = "Native server adapter stalled #{GRACE}s after SIGTERM while #{@phase} (pid #{Process.pid})\n"
      out << "Reactor thread (#{main.status}):\n"
      Array(main.backtrace).first(30).each { |line| out << "  #{line}\n" }
      tasks(@scheduler, 0, out) if @scheduler
      out
    rescue StandardError => e
      "#{out}Stall report failed: #{e.class}: #{e.message}\n"
    end

    def tasks(node, depth, out)
      node.children&.each do |task|
        out << "#{'  ' * depth}- #{task.status} #{task.annotation}\n"
        fiber = task.fiber
        frames = fiber&.alive? ? Array(fiber.backtrace).first(TASK_FRAMES) : []
        frames.each { |line| out << "#{'  ' * depth}    #{line}\n" }
        tasks(task, depth + 1, out)
      end
    end
  end
end
