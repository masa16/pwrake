module Rake

  class Task
    attr_accessor :already_invoked
    attr_accessor :already_fetched
    attr_accessor :footprint

    def initialize(task_name, app)
      @name = task_name.to_s
      @prerequisites = []
      @already_invoked = false
      @full_comment = nil
      @comment = nil
      if $test==:orig
        @lock = Monitor.new
        @actions = []
      end
      @application = app
      @scope = app.current_scope
      @arg_names = nil
      @locations = []
    end

    def enhance(deps=nil, &block)
      @prerequisites |= deps if deps
      if $test==:orig
        @actions << block if block_given?
      end
      self
    end
  end

  # The TaskManager module is a mixin for managing tasks.
  module TaskManager
    def clear_footprint
      @tasks.each_value{|tsk| tsk.footprint=false}
    end
  end

end
