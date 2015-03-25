module Pwrake

  class TaskQueue

    def initialize(host_list)
      @q = []
      @empty = []
      #@fibuf = []
      @finished = false

      #@halt = false
      #@mutex = Mutex.new
      #@th_end = {}

      @enable_steal = true
      @q_no_action = Array.new

      #pri = Pwrake.application.pwrake_options['QUEUE_PRIORITY'] || "FIFO"#"RANK"
      #case pri
      #when /prio/i
      #  @array_class = PriorityQueueArray
      #when /fifo/i
      #  @array_class = FifoQueueArray # Array # Fifo
      #when /lifo/i
      #  @array_class = LifoQueueArray
      #when /lihr/i
      #  @array_class = LifoHrfQueueArray
      #when /prhr/i
      #  @array_class = PriorityHrfQueueArray
      #when /rank/i
      #  @array_class = RankQueueArray
      #else
      #  raise RuntimeError,"unknown option for QUEUE_PRIORITY: "+pri
      #end
      ##Log.debug "--- TQ#initialize @array_class=#{@array_class.inspect}"
      @array_class = Array
      init_queue(host_list)
    end

    def init_queue(host_list)
      # @q_input = @array_class.new(host_list.size)
      @q_input = Array.new
      @q_no_input = Array.new
    end

    #attr_reader :mutex
    attr_accessor :enable_steal

    def halt
      @halt = true
    end

    def resume
      @halt = false
    end

    def synchronize(condition)
      yield
    end

    # enq
    def enq(item)
      if item.nil? || item.actions.empty?
        @q_no_action.push(item)
      else
        enq_body(item)
      end
    end

    def enq_body(item)
      enq_impl(item)
    end

    def enq_impl(t)
      if t.kind_of?(Rake::FileTask) and !t.prerequisites.empty?
        @q_input.push(t)
      else
        @q_no_input.push(t)
      end
    end

    # deq
    def deq(hint=nil)
      if empty? # no item in queue
        if @finished
          return false
        end
      end
      if !@q_no_action.empty?
        return @q_no_action.shift
      end
      if t = deq_impl(hint)
        #t_inspect = t.inspect[0..1000]
        return t
      end
      #puts "deq: no items"
      nil
    end

    def deq_impl(hint)
      @q_input.shift || @q_no_input.shift
    end

    def clear
      @q_no_action.clear
      @q_input.clear
      @q_no_input.clear
    end

    def empty?
      @q_no_action.empty? &&
        @q_input.empty? &&
        @q_no_input.empty?
    end

    def finish
      @finished = true
    end

    def stop
      #@mutex.synchronize do
        clear
        finish
      #end
    end

    def thread_end(th)
      @th_end[th] = true
    end

    def after_check(tasks)
      # implimented at subclass
    end

  end
end
