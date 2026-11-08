# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # A result is distinct from task finalization and retained child ownership.
      class CompletionJoin
        # Installed only on a selected instance, with independent state per gem.
        module Consume
          def consume
            result = super
            @connectrpc_async_completion&.consumed
            result
          end
        end
        private_constant :Consume

        class Observation
          attr_reader :required, :generation, :promise, :violation

          def initialize(target)
            @target = target
            @fiber = target.fiber
            @scheduler = Fiber.scheduler
            @required = {}.compare_by_identity
            @generation = 0
            @promise = ::Async::Promise.new
            @active = true
            @terminal = false
            @queued = nil
          end

          def terminal? = @terminal
          def fiber_alive? = @fiber&.alive?

          def retain(children)
            added = false
            children.each do |child|
              next if @required.key?(child)

              @required[child] = true
              added = true
            end
            advance if added
          end

          # Node#consume can expose work before Task#finished? becomes true.
          # Capture identities here, before any deferred check or tree mutation.
          def consumed
            return unless @active

            retain(CompletionJoin.children(@target))
            return if @terminal || @queued

            no_work = @required.each_key.none? { |child| CompletionJoin.pending?(child) }
            return unless @target.finished? || (@target.fiber.nil? && no_work)

            raise "Completion scheduler closed before finalization" if @scheduler.closed?

            @queued = Check.new(self)
            @scheduler.push(@queued)
          rescue StandardError => e
            violate(e)
          end

          def check
            @queued = nil
            retain(CompletionJoin.children(@target))
            unless @target.finished?
              if !fiber_alive? && @required.each_key.none? { |child| CompletionJoin.pending?(child) }
                raise CompletionJoin.residual_violation(@target)
              end

              return
            end

            raise "Finished task still has its captured live fiber" if fiber_alive?

            @terminal = true
            # The original fiber is gone, but retained joins can still expose
            # new membership at consume. Keep that hook active until joined.
            @fiber = @scheduler = nil
            advance
          end

          def violate(error)
            return if @violation

            @violation = error
            release
            advance
          rescue StandardError
            # Keep the violation inspectable even if shutdown prevents waking.
            nil
          end

          def release
            @active = false
            @fiber = @scheduler = nil
          end

          private

          def advance
            previous = @promise
            @generation += 1
            @promise = ::Async::Promise.new
            previous.resolve(nil)
          end
        end
        private_constant :Observation

        # Scheduler callbacks run after finish!'s non-yielding tail. They are
        # neither Tasks nor children, and never execute application work.
        class Check
          def initialize(observation)
            @observation = observation
          end

          def alive? = !@observation.nil?

          def transfer
            observation = @observation
            return unless observation

            @observation = nil
            observation.check
          rescue StandardError => e
            observation.violate(e)
          end
        end
        private_constant :Check

        def self.observation(target)
          target.instance_variable_get(:@connectrpc_async_completion)
        end

        def self.children(target)
          target.children&.to_a&.reject { |child| child.transient? || child.finished? } || []
        end

        def self.pending?(target, seen = {}.compare_by_identity)
          return false if seen.key?(target)
          return true unless target.finished?

          seen[target] = true
          state = observation(target)
          return false unless state

          !state.terminal? || state.violation || state.required.each_key.any? { |child| pending?(child, seen) }
        end

        def self.scan(target, retained)
          actual = children(target)
          state = observation(target)
          state&.retain(actual)
          actual.each { |child| retained[child] = true }
          state&.required&.each_key { |child| retained[child] = true }
          retained.keys.select { |child| pending?(child) }
        end

        def self.residual_violation(target)
          attached = target.children&.to_a&.map do |child|
            { status: child.status, finished: child.finished?, transient: child.transient? }
          end
          RuntimeError.new("Completion observation violation: unfinished task without a live fiber or required work; " \
                           "status=#{target.status}; attached_children=#{attached.inspect}")
        end

        def self.call(task, failure: false, on_interrupt: nil)
          interruption = stored_failure = nil
          pending = [[task, false, {}.compare_by_identity]]
          begin
            yield if block_given?
          rescue Exception => e # rubocop:disable Lint/RescueException -- finish the join before propagating caller interruption
            interruption = e
          end
          while (entry = pending.last)
            target, known, retained = entry
            raise ArgumentError, "Cannot join the current task" if target.current?

            begin
              if interruption && on_interrupt
                request = on_interrupt
                on_interrupt = nil
                request.call
              end
              target.wait unless known
              entry[1] = true
            rescue Exception => e # rubocop:disable Lint/RescueException -- repeated interruption cannot abandon retirement
              if target.failed? && target.result.equal?(e)
                stored_failure ||= e if (failure && target.equal?(task)) || !e.is_a?(StandardError)
                entry[1] = true
              else
                interruption ||= e
              end
              next
            end

            work = scan(target, retained)
            state = observation(target)
            state.consumed if state && target.finished? && !state.terminal?
            raise state.violation if state&.violation

            unless work.empty?
              pending.concat(work.map { |child| [child, false, {}.compare_by_identity] })
              next
            end
            if target.finished? && (!state || state.terminal?)
              state&.release
              pending.pop
              next
            end

            unless state
              raise residual_violation(target) unless target.fiber&.alive?

              state = Observation.new(target)
              target.instance_variable_set(:@connectrpc_async_completion, state)
              target.extend(Consume)
              # Installation, capture and the first rescan have no yielding gap.
              state.consumed
              next
            end

            generation = state.generation
            signal = state.promise
            next unless scan(target, retained).empty?
            raise state.violation if state.violation
            next if target.finished? && state.terminal?
            raise residual_violation(target) if !target.finished? && !state.fiber_alive?
            next unless generation == state.generation && signal.equal?(state.promise)

            begin
              signal.wait
            rescue Exception => e # rubocop:disable Lint/RescueException -- latch interruption owns no completion credit
              interruption ||= e
            end
          end
          raise interruption if interruption
          raise stored_failure if stored_failure

          task.result
        end
      end
      private_constant :CompletionJoin
    end
  end
end
