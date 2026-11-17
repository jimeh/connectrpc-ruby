# frozen_string_literal: true

require "async/http/version"
require "protocol/http2/version"

module ConnectRPC
  module Async
    # Raised by require when an installed socketry gem no longer provides a
    # behavior or hook that this package replaces or relies on.
    class UpstreamContractError < StandardError; end
    private_constant :UpstreamContractError

    module Internal
      # Checked once per process while the package loads, so an upstream
      # change fails require with a named reason instead of hanging later.
      # spec/upstream_contract_spec.rb pins the behavior behind each entry.
      module UpstreamContract
        ISSUE = "https://github.com/jimeh/connectrpc-ruby/issues/68"
        TESTED = { "async" => "2.46.0", "async-http" => "0.105.0", "protocol-http2" => "0.29.2" }.freeze
        VERSIONS = {
          "async" => "Async::VERSION", "async-http" => "Async::HTTP::VERSION",
          "protocol-http2" => "Protocol::HTTP2::VERSION", "protocol-http1" => "Protocol::HTTP1::VERSION"
        }.freeze
        PROBE_SECONDS = 5
        STEP_SECONDS = 2
        KILL_SECONDS = 0.5

        H2 = "Async::HTTP::Protocol::HTTP2"
        CONNECTION_HOOKS = [
          ["async-http", :read_in_background, :public, "(parent: ?)", "#{H2}::Connection"],
          ["async-http", :synchronize, :public, "(&)", "#{H2}::Connection"],
          ["async-http", :write_frame, :public, "(_)", "#{H2}::Connection"],
          ["async-http", :write_frames, :public, "()", "#{H2}::Connection"]
        ].freeze
        STREAM_HOOKS = [
          ["async-http", :prepare_input, :public, "(_)", "#{H2}::Stream"],
          ["async-http", :process_data, :public, "(_)", "#{H2}::Stream"],
          ["async-http", :send_body, :public, "(_, _ = ?)", "#{H2}::Stream"],
          ["async-http", :send_headers, :public, "(*, **, &)", "#{H2}::Stream"],
          ["async-http", :finish_output, :public, "(_ = ?)", "#{H2}::Stream"],
          ["protocol-http2", :request_window_update, :public, "()", "Protocol::HTTP2::FlowControlled"]
        ].freeze
        # gem, receiver, method, visibility, parameter shape, defining owner.
        # Positional names are ignored; "?" marks a default value.
        HOOKS = [
          ["async-http", "#{H2}::Output", :start, :public, "(parent: ?)", "#{H2}::Output"],
          ["async-http", "#{H2}::Output", :stream, :private, "(_)", "#{H2}::Output"],
          ["async-http", "#{H2}::Output", :passthrough, :private, "(_)", "#{H2}::Output"],
          ["async-http", "#{H2}::Output", :write, :public, "(_)", "#{H2}::Output"],
          ["async-http", "#{H2}::Output", :send_data, :private, "(_, _, _)", "#{H2}::Output"],
          *%w[Client Server].flat_map { |side| CONNECTION_HOOKS.map { |gem, *hook| [gem, "#{H2}::#{side}", *hook] } },
          ["async-http", "#{H2}::Client", :close, :public, "(_ = ?)", "#{H2}::Connection"],
          ["async-http", "#{H2}::Server", :close, :public, "(_ = ?)", "#{H2}::Server"],
          ["async-http", "#{H2}::Client", :create_response, :public, "()", "#{H2}::Client"],
          ["async-http", "#{H2}::Server", :each, :public, "(task: ?)", "#{H2}::Server"],
          ["protocol-http2", "#{H2}::Server", :create_stream, :public, "(_ = ?, &)", "Protocol::HTTP2::Connection"],
          ["protocol-http2", "#{H2}::Server", :send_goaway, :public, "(_ = ?, _ = ?)", "Protocol::HTTP2::Connection"],
          *%w[Client Server].map do |side|
            ["protocol-http2", "#{H2}::#{side}", :update_local_settings, :public, "(_)", "Protocol::HTTP2::Connection"]
          end,
          *%w[Request Response].flat_map do |side|
            STREAM_HOOKS.map { |gem, *hook| [gem, "#{H2}::#{side}::Stream", *hook] }
          end
        ].freeze

        def self.verify!(package: "connectrpc-async", version: VERSION, hooks: HOOKS, tested: TESTED,
                         finalization: true)
          failures = hook_failures(hooks)
          failures.concat(finalization_failures) if finalization
          return if failures.empty?

          raise UpstreamContractError, report(package, version, failures, tested)
        end

        def self.report(package, version, failures, tested)
          verified = tested.map { |gem, release| "#{gem} #{release}" }.join(", ")
          ["#{package} #{version} is incompatible with the installed socketry gems:",
           *failures.map { |failure| "  - #{failure}" },
           "Verified with #{verified}. See #{ISSUE}."].join("\n")
        end

        def self.failure(gem, text) = "#{gem} #{installed(gem)}: #{text}"

        def self.installed(gem)
          name = VERSIONS[gem]
          name ? Object.const_get(name) : "(unknown version)"
        rescue NameError
          "(unknown version)"
        end

        def self.hook_failures(hooks)
          hooks.filter_map do |gem, receiver, name, *expected|
            expected = Hook.format(name, *expected)
            found = Hook.describe(receiver, name)
            failure(gem, "#{receiver}##{name} expected #{expected}, found #{found}") unless found == expected
          end
        end

        # Visibility, parameter shape and defining owner, ignoring positional names.
        module Hook
          def self.format(name, visibility, parameters, owner) = "#{visibility} #{name}#{parameters} from #{owner}"

          def self.describe(receiver, name)
            target = Object.const_get(receiver)
            method = target.instance_method(name)
            visibility = if target.public_method_defined?(name) then :public
                         elsif target.protected_method_defined?(name) then :protected
                         else :private
                         end
            format(name, visibility, shape(method.parameters), method.owner)
          rescue NameError
            "nothing"
          end

          def self.shape(parameters)
            tokens = parameters.map do |kind, name|
              case kind
              when :req then "_"
              when :opt then "_ = ?"
              when :rest then "*"
              when :keyreq then "#{name}:"
              when :key then "#{name}: ?"
              when :keyrest then "**"
              when :nokey then "**nil"
              else "&"
              end
            end
            "(#{tokens.join(', ')})"
          end
        end
        private_constant :Hook

        # The probe runs in its own thread and reactor, so it never touches
        # the caller's scheduler or task tree. It returns within the budget,
        # but a probe thread that Async cannot stop stays asleep.
        def self.finalization_failures(seconds = PROBE_SECONDS)
          probe = FinalizationProbe.new
          thread = Thread.new do
            Thread.current.report_on_exception = false
            probe.call
          rescue Exception => e # rubocop:disable Lint/RescueException -- report any probe crash as a broken contract
            [*probe.failures,
             failure("async", "Task finalization probe raised #{e.class} while #{probe.crashed_step || probe.step}: " \
                              "#{e.message}")]
          end
          grace = [KILL_SECONDS, seconds / 10.0].min
          return thread.value if thread.join(seconds - grace)

          step = probe.step
          failures = probe.failures
          thread.kill
          begin
            thread.join(grace)
          rescue Exception # rubocop:disable Lint/RescueException -- the timeout below is the reported failure
            nil
          end
          leaked = ", and Async could not stop the probe thread" if thread.alive?
          [*failures,
           failure("async", "Task finalization probe did not finish within #{seconds} s while #{step}#{leaked}")]
        end

        # Matches CompletionJoin's per-instance hook.
        module ConsumeSpy
          def consume
            result = super
            @connectrpc_contract_observer&.call(self)
            result
          end
        end
        private_constant :ConsumeSpy

        # A duck-typed scheduler entry, like CompletionJoin's Check.
        class Pushed
          attr_reader :inline, :fiber_alive

          def initialize(fiber, done)
            @fiber = fiber
            @done = done
            @consuming = true
          end

          def consumed = @consuming = false
          def alive? = !@done.resolved?

          def transfer
            @inline = @consuming
            @fiber_alive = @fiber.alive?
            @done.resolve(true)
          end
        end
        private_constant :Pushed

        class ProbeError < StandardError; end
        private_constant :ProbeError

        # CompletionJoin relies on these Async behaviors: Task#finish! clears
        # the fiber and then calls a per-instance #consume, Node#consume
        # calls each ancestor's #consume when a descendant leaves, and
        # Scheduler#push transfers a duck-typed object on a later iteration.
        class FinalizationProbe
          EXITS = { return: "normal return", raise: "a raised failure", cancel: "cancellation" }.freeze

          # crashed_step keeps the step an exception left, before teardown relabels it.
          attr_reader :step, :crashed_step

          def initialize
            @step = "starting its reactor"
            @failures = []
            @tasks = []
            @gates = []
          end

          # A snapshot the timeout path reports while the probe may still run.
          def failures = @failures.dup

          def call
            Sync do |root|
              @root = root
              EXITS.each_key do |exit|
                leaf(exit)
                ancestor(exit)
              end
              deferred_push
            rescue Exception # rubocop:disable Lint/RescueException -- recorded only; re-raised to the thread's report
              @crashed_step = @step
              raise
            ensure
              @step = "stopping leftover probe tasks"
              release
              @step = "closing its reactor"
            end
            @failures
          end

          private

          def leaf(exit)
            @step = "finalizing a task after #{EXITS[exit]}"
            gate = self.gate
            task = ::Async::Task.new(@root, finished: false) { body(exit, gate) }
            seen = observe(task)
            task.run
            finish(task, exit)
            return if task.finished? && cleared?(seen)

            fail!("Task finalization after #{EXITS[exit]} no longer calls a per-instance #consume once the " \
                  "task's fiber is cleared (observed #{describe(seen)})")
          end

          def ancestor(exit)
            @step = "finalizing a parent after #{EXITS[exit]} while its child waits"
            child_gate = gate
            parent_gate = gate
            started = gate
            child = nil
            parent = ::Async::Task.new(@root, finished: false) do |task|
              child = task.async(finished: false) { |inner| inner.defer_cancel { child_gate.wait } }
              @tasks << child
              started.resolve(true)
              body(exit, parent_gate)
            end
            seen = observe(parent)
            parent.run
            # The child's eager start queues the parent behind this task.
            started.wait
            finish(parent, exit)
            if parent.finished? || seen.any? { |_, finished| finished }
              return fail!("a parent whose block ended by #{EXITS[exit]} is finished while its child still waits")
            end

            @step = "finishing the waiting child of a parent that ended by #{EXITS[exit]}"
            child_gate.resolve(true)
            child&.wait
            return if parent.finished? && cleared?(seen)

            fail!("Task finalization no longer calls a parent's per-instance #consume when its last child " \
                  "finishes after the parent ended by #{EXITS[exit]} (observed #{describe(seen)})")
          end

          def deferred_push
            @step = "running an object pushed to the scheduler from #consume"
            done = ::Async::Promise.new
            fiber = pushed = nil
            task = ::Async::Task.new(@root, finished: false) { fiber = Fiber.current }
            observe(task) do |target|
              next if pushed || !target.fiber.nil?

              pushed = Pushed.new(fiber, done)
              Fiber.scheduler.push(pushed)
              pushed.consumed
            end
            task.run
            # Without a cleared-fiber #consume the finalization steps report it.
            return unless pushed

            begin
              @root.with_timeout(STEP_SECONDS) { done.wait }
            rescue ::Async::TimeoutError
              return fail!("Fiber.scheduler.push no longer transfers a pushed object")
            end
            if pushed.inline
              fail!("Fiber.scheduler.push ran a pushed object inline during #consume, not on a later iteration")
            elsif pushed.fiber_alive
              fail!("a pushed object ran while the finished task's original fiber was still alive")
            end
          end

          def body(exit, gate)
            case exit
            when :raise then raise ProbeError, "probe failure"
            when :cancel then gate.wait
            end
          end

          def finish(task, exit)
            if exit == :cancel
              raise ProbeError, "cancellation probe task did not suspend" unless task.alive?

              task.cancel
            end
            begin
              task.wait
            rescue ProbeError
              nil
            end
          end

          def gate
            gate = ::Async::Promise.new
            @gates << gate
            gate
          end

          def observe(task, &observer)
            seen = []
            task.instance_variable_set(:@connectrpc_contract_observer, lambda do |target|
              seen << [target.fiber.nil?, target.finished?]
              observer&.call(target)
            end)
            task.extend(ConsumeSpy)
            @tasks << task
            seen
          end

          def cleared?(seen) = seen.any? { |fiber_cleared, finished| fiber_cleared && finished }

          def describe(seen)
            return "no #consume call" if seen.empty?

            seen.map do |fiber_cleared, finished|
              "#{fiber_cleared ? 'cleared' : 'live'} fiber, #{finished ? 'finished' : 'unfinished'}"
            end.join("; ")
          end

          def fail!(text)
            @failures << UpstreamContract.failure("async", text)
            nil
          end

          # Lets the probe reactor end after a detected failure.
          def release
            @gates.each { |gate| gate.resolve(nil) unless gate.resolved? }
            @tasks.each do |task|
              task.stop unless task.finished?
            rescue StandardError
              nil
            end
          end
        end
        private_constant :FinalizationProbe
      end
    end
  end
end
