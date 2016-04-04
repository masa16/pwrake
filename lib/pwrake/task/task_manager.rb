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
      super.pw_set_property(prop)
    end

    def create_rule(*args, &block) # :nodoc:
      @property_by_block[block.object_id] = @last_property
      @last_property = TaskProperty.new
      super
    end

  end
end
