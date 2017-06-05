require "pwrake/task/task_property"

module Pwrake

  module TaskManager

    def initialize
      @property_by_block = {}
      @last_property = TaskProperty.new
      super
    end

    def last_description=(description)
      @last_property.parse_description(description)
      super
    end

    def define_task(task_class, *args, &block) # :nodoc:
      prop = @property_by_block[block.object_id]
      if prop.nil?
        prop = @last_property
        @last_property = TaskProperty.new
      end
      if !@disable_subflow
        @subflow_prefix = Rake.application.current_flow[Fiber.current]
      end
      task = super
      task.pw_set_property(prop)
      if !@disable_subflow
        task.property.subflow = @subflow_prefix
      end
      task
    ensure
      @subflow_prefix = nil
    end

    def create_rule(*args, &block) # :nodoc:
      @property_by_block[block.object_id] = @last_property
      @last_property = TaskProperty.new
      super
    end

    def resolve_args(args)
      task_name, arg_names, deps = super(args)
      if @subflow_prefix
        # fix prerequisites
        deps.map! do |task_name|
          add_prefix(task_name)
        end
      end
      [task_name, arg_names, deps]
    end

    def intern(task_class, task_name)
      if @subflow_prefix
        base_name = task_name
        # fix task name
        task_name = add_prefix(task_name)
        if base_name != task_name && task_class == Rake::Task
          sbfl = @subflow_prefix
          begin
            @subflow_prefix = nil
            @disable_subflow = true
            Rake::Task.define_task(base_name => task_name)
          ensure
            @subflow_prefix = sbfl
            @disable_subflow = false
          end
        end
      end
      super(task_class, task_name)
    end

    def add_prefix(task_name)
      task_name = task_name.to_s
      if /^\.?\.?\// =~ task_name
        task_name
      else
        File.join(@subflow_prefix, task_name)
      end
    end
  end
end
