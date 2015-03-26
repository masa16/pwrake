module Pwrake

  module TaskAlgorithm

    attr_reader :pw_task

    def pw_invoke
      @lock.synchronize do
        return if @already_invoked
        @already_invoked = true
      end
      @pw_task = PwrakeTask.new(self)
      @pw_task.execute if needed?
    end

    def pw_enq_subsequents
      t = Time.now
      #h = application.pwrake_options['HALT_QUEUE_WHILE_SEARCH']
      #application.task_queue.synchronize(h) do
        @subsequents.each do |t|        # <<--- competition !!!
          if t && t.check_prereq_finished(self.name)
            application.task_queue.enq(t)
          end
        end
      #end
      @already_finished = true        # <<--- competition !!!
    end

    def check_prereq_finished(preq_name=nil)
      @unfinished_prereq.delete(preq_name)
      @unfinished_prereq.empty?
    end

  end

end # module Pwrake
