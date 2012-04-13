module Rake

  class Task
    #include Pwrake::Log
    attr_accessor :already_invoked

    alias invoke_orig :invoke

    def invoke(*args)
      Pwrake::Log.log "--- Task#invoke(#{args.inspect}) Pwrake.manager.threads=#{Pwrake.manager.threads}"
      #if Pwrake.manager.threads == 1
      #  invoke_orig(*args)
      #else
        task_args = TaskArguments.new(arg_names, args)
        Pwrake.manager.operator.invoke(self,task_args)
      #end
    end

    alias execute_orig :execute

    # Execute the actions associated with this task.
    def execute(args=nil)
      args ||= EMPTY_TASK_ARGS
      if application.options.dryrun
        Pwrake::Log.log "** Execute (dry run) #{name}"
        return
      end
      if application.options.trace
        Pwrake::Log.log "** Execute #{name}"
      end
      application.enhance_with_matching_rule(name) if @actions.empty?
      @actions.each do |act|
        case act.arity
        when 1
          act.call(self)
        else
          act.call(self, args)
        end
      end
    end

    def resource
      0
    end

  end

end # module Rake
