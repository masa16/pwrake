module Pwrake

  module TaskManager

    def initialize
      @ncore_by_block = {}
      super
    end

    def last_description=(description)
      if /\bn_?cores?[=:]([+-]?\d+)/ =~ description
        @last_ncore = $1.to_i
      end
      super
    end

    def define_task(task_class, *args, &block) # :nodoc:
      nc = @ncore_by_block[block.object_id] || @last_ncore
      @last_ncore = nil
      super.pw_set_ncore(nc)
    end

    def create_rule(*args, &block) # :nodoc:
      @ncore_by_block[block.object_id] = @last_ncore || 1
      @last_ncore = nil
      super
    end

  end
end
