require "pwrake/queue/task_queue"
require "pwrake/queue/queue_array"
require "pwrake/queue/no_action_queue"

module Pwrake

  class NonLocalityQueue

    def initialize(hostinfo_by_id, array_class, group_map=nil)
      @hostinfo_by_id = hostinfo_by_id
      @array_class = array_class

      @q_input = @array_class.new(0)
      @q_no_input = FifoQueueArray.new
      @n_turn = 1
    end

    attr_reader :n_turn

    def enq_impl(tw)
      if tw.has_input_file?
        @q_input.push(tw)
      else
        @q_no_input.push(tw)
      end
    end

    def turn_empty?(turn)
      empty?
    end

    def deq_impl(host_info=nil, turn=nil)
      @q_input.shift(host_info) ||
      @q_no_input.shift(host_info)
    end

    def size
      @q_input.size +
      @q_no_input.size
    end

    def clear
      @q_input.clear
      @q_no_input.clear
    end

    def empty?
      @q_input.empty? &&
      @q_no_input.empty?
    end

    def inspect_q
      TaskQueue._qstr("input",   @q_input) +
      TaskQueue._qstr("no_input",@q_no_input)
    end

    def drop_host(host_info)
    end

  end
end
