module Pwrake

  InvocationChain = Rake::InvocationChain
  TaskArguments = Rake::TaskArguments

  module TaskAlgorithm

    attr_reader :subsequents
    attr_reader :arguments
    attr_reader :property
    attr_reader :unfinished_prereq

    def wrapper
      if @wrapper.nil?
        raise "TaskWrapper is not defined for #{self.class}[#{name}]"
      end
      @wrapper
    end

    def pw_search_tasks(args)
      Log.debug "#{self.class}[#{name}]#pw_search_tasks start, args=#{args.inspect}"
      cl = Pwrake.clock
      TaskWrapper.clear_rank
      task_args = TaskArguments.new(arg_names, args)
      # not synchronize owing to fiber
      search_with_call_chain(nil, task_args, InvocationChain::EMPTY)
      #
      Log.debug "#{self.class}[#{name}]#pw_search_tasks end #{Pwrake.clock-cl}"
    end

    # Same as search, but explicitly pass a call chain to detect
    # circular dependencies.
    def search_with_call_chain(subseq, task_args, invocation_chain) # :nodoc:
      new_chain = InvocationChain.append(self, invocation_chain)
      @lock.synchronize do
        if application.options.trace
          #Log.debug "** Search #{name}#{format_search_flags}"
          application.trace "** Search #{name}#{format_search_flags}"
        end

        return true if @already_finished # <<--- competition !!!
        @subsequents ||= []
        @subsequents << subseq if subseq # <<--- competition !!!

        if ! @already_searched
          @already_searched = true
          @arguments = task_args
          @wrapper = TaskWrapper.new(self,task_args)
          if @prerequisites.empty?
            @unfinished_prereq = {}
          else
            search_prerequisites(task_args, new_chain)
          end
          #check_and_enq
          if !@already_finished && @unfinished_prereq.empty?
            application.task_queue.enq(@wrapper)
          end
        end
        return false
      end
    rescue Exception => ex
      add_chain_to(ex, new_chain)
      raise ex
    end

    # Search all the prerequisites of a task.
    def search_prerequisites(task_args, invocation_chain) # :nodoc:
      @unfinished_prereq = {}
      @prerequisites.each{|t| @unfinished_prereq[t]=true}
      prerequisite_tasks.each { |prereq|
        prereq_args = task_args.new_scope(prereq.arg_names)
        if prereq.search_with_call_chain(self, prereq_args, invocation_chain)
          @unfinished_prereq.delete(prereq.name)
        end
      }
    end

    # Format the trace flags for display.
    def format_search_flags
      flags = []
      flags << "finished" if @already_finished
      flags << "first_time" unless @already_searched
      flags << "not_needed" unless needed?
      flags.empty? ? "" : " (" + flags.join(", ") + ")"
    end
    private :format_search_flags

    def pw_enq_subsequents
      # not synchronize owing to fiber
      @subsequents.each do |t|        # <<--- competition !!!
        if t && t.check_prereq_finished(self.name)
          application.task_queue.enq(t.wrapper)
        end
      end
      @already_finished = true        # <<--- competition !!!
    end

    def check_prereq_finished(preq_name=nil)
      @unfinished_prereq.delete(preq_name)
      !@already_finished && @unfinished_prereq.empty?
    end

    def pw_set_property(property)
      if @property
        @property.merge(property)
      else
        @property = property
      end
      self
    end

  end # TaskAlgorithm


  module TaskInvoke

    def invoke(*args)
      Rake.application.invoke(self,*args)
    end

    def reenable
      @already_invoked = false
      @already_searched = false
    end
  end
end
