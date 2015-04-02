module Pwrake

  class TaskQueue

    def initialize(core_map, group_map=nil)
      @q = []
      @empty = []
      @finished = false

      @enable_steal = true
      @q_no_action = Array.new

      @idle_cores = core_map.dup

      pri = Rake.application.pwrake_options['QUEUE_PRIORITY'] || "LIHR"
      case pri
      when /prio/i
        @array_class = PriorityQueueArray
      when /fifo/i
        @array_class = FifoQueueArray # Array # Fifo
      when /lifo/i
        @array_class = LifoQueueArray
      when /lihr/i
        @array_class = LifoHrfQueueArray
      when /prhr/i
        @array_class = PriorityHrfQueueArray
      when /rank/i
        @array_class = RankQueueArray
      else
        raise RuntimeError,"unknown option for QUEUE_PRIORITY: "+pri
      end
      Log.debug "@array_class=#{@array_class.inspect}"
      init_queue(core_map, group_map)
    end

    def init_queue(core_map, group_map=nil)
      @q_input = @array_class.new(core_map.size)
      #@q_input = Array.new
      @q_no_input = Array.new
    end

    #attr_reader :mutex
    attr_accessor :enable_steal

    # enq
    def enq(tw)
      if tw.nil? || tw.actions.empty?
        @q_no_action.push(tw)
      else
        enq_body(tw)
      end
    end

    def enq_body(tw)
      enq_impl(tw)
    end

    def enq_impl(tw)
      if tw.task.kind_of?(Rake::FileTask) and !tw.prerequisites.empty?
        @q_input.push(tw)
      else
        @q_no_input.push(tw)
      end
    end


    def deq_task(&block) # simple version
      queued = deq_loop(&block)
      if queued>0
        Log.debug "queued:#{queued} @idle_cores:#{@idle_cores.inspect}"
      end
    end

    def deq_loop(steal,&block)
      queued = 0
      while true
        count = 0
        @idle_cores.keys.each do |hid|
          if empty?
            return queued
          #if t = deq(@workers[hid].host)
          elsif tw = deq_impl(hid,steal)
            Log.debug "deq: #{tw.name}"
            if @idle_cores[hid] < tw.n_used_cores
              enq(tw) # check me
            else
              @idle_cores.decrease(hid, tw.n_used_cores)
              yield(tw,hid)
              count += 1
              queued += 1
            end
          end
        end
        break if count == 0
      end
      queued
    end

    def deq_impl(hint=nil, steal=nil)
      @q_no_action.shift ||
        @q_input.shift ||
        @q_no_input.shift
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

    def task_end(tw, hid)
      @idle_cores.increase(hid, tw.n_used_cores)
    end

  end
end
