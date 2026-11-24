# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # Joins a task and its non-transient descendants until every one has
      # settled: its promise resolved, so its block, ensure included, has
      # returned. Async resolves a cancelled task's promise before it stops
      # that task's children and clears its fiber, and Task#finished? turns
      # true with no signal to wait on. Past the settled boundary only that
      # bookkeeping remains (stop_children raising into children the walk
      # still joins, then finish!), which runs no application code and
      # touches no descriptor. So a descriptor closed after a join is no
      # longer in use by any fiber the join saw.
      #
      # The boundary assumes a task's block never starts after its promise
      # resolved. Async 2.46.0 allows that only when an initialized task that
      # already has children is cancelled, the cancel cascade into those
      # children yields, and something then runs the task. No task here gains
      # children before it runs, and a task that may be cancelled before it
      # runs is started only behind a finished? or status guard.
      class CompletionJoin
        def self.settled?(task) = task.completed? || task.failed? || task.cancelled?

        # One non-yielding pass over the target and every identity already
        # retained, returning each unsettled task. Every non-transient child
        # seen is retained, so it stays joined after it is reparented away or
        # flagged transient. A transient child's subtree is skipped.
        def self.unsettled(target, retained)
          pending = []
          seen = {}.compare_by_identity
          stack = [target, *retained.keys]
          while (node = stack.pop)
            next if seen.key?(node)

            seen[node] = true
            pending << node unless settled?(node)
            node.children&.each do |child|
              next if child.transient?

              retained[child] = true
              stack << child
            end
          end
          pending
        end

        # A join retains its caller's first interruption until every task it
        # joins has settled, then raises it. An interruptible join raises it
        # at once and leaves the tasks running on their own. A task's own
        # failure is never raised: the task that failed reports it.
        def self.call(task, interruptible: false)
          interruption = nil
          begin
            yield if block_given?
          rescue Exception => e # rubocop:disable Lint/RescueException -- finish the join before propagating caller interruption
            raise if interruptible

            interruption = e
          end
          retained = {}.compare_by_identity
          until (pending = unsettled(task, retained)).empty?
            raise ArgumentError, "Cannot join the current task" if pending.any?(&:current?)

            # A task that has not run yet is waited on: another fiber may
            # start it.
            waiting = pending.first
            begin
              waiting.wait
            rescue Exception => e # rubocop:disable Lint/RescueException -- repeated interruption cannot abandon retirement
              next if waiting.failed? && waiting.result.equal?(e)
              raise if interruptible

              interruption ||= e
            end
          end
          raise interruption if interruption

          task.result
        end
      end
      private_constant :CompletionJoin
    end
  end
end
