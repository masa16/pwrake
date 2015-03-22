module Pwrake

  InvocationChain = Rake::InvocationChain
  TaskArguments = Rake::TaskArguments

  module TaskSearch

    def pw_search_tasks(args)
      task_args = TaskArguments.new(arg_names, args)
      #timer = Timer.new("search_task")
      #h = application.pwrake_options['HALT_QUEUE_WHILE_SEARCH']
      #application.task_queue.synchronize(h) do
	search_with_call_chain(nil, task_args, InvocationChain::EMPTY)
      #end
      #timer.finish
    end

    # Same as search, but explicitly pass a call chain to detect
    # circular dependencies.
    def search_with_call_chain(subseq, task_args, invocation_chain) # :nodoc:
      new_chain = InvocationChain.append(self, invocation_chain)
      @lock.synchronize do
        if application.options.trace
          #Log.info "** Search #{name} #{format_search_flags}"
        end

        return true if @already_finished # <<--- competition !!!
        @subsequents ||= []
        @subsequents << subseq if subseq # <<--- competition !!!

        if ! @already_searched
          @already_searched = true
          @arg_data = task_args
          @lock_rank = Monitor.new
          if @prerequisites.empty?
            @unfinished_prereq = {}
          else
            search_prerequisites(task_args, new_chain)
          end
          #@task_id = application.task_id_counter
          #check_and_enq
          if @unfinished_prereq.empty?
            application.task_queue.enq(self)
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
        #prereq_args = task_args.new_scope(prereq.arg_names) # in vain
        if prereq.search_with_call_chain(self, task_args, invocation_chain)
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
      flags.empty? ? "" : "(" + flags.join(", ") + ")"
    end
    private :format_search_flags
  end
end

