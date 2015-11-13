module Pwrake

  class TaskQueue

    def initialize(host_map, group_map=nil)
      @q = []
      @empty = []

      @enable_steal = true
      @q_no_action = NoActionQueue.new

      @host_map = host_map

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
      init_queue(group_map)
    end

    def init_queue(group_map=nil)
      @q_input = @array_class.new(0)
      @q_no_input = FifoQueueArray.new
      @n_turn = 1
    end

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
      if tw.has_input_file?
        @q_input.push(tw)
      else
        @q_no_input.push(tw)
      end
    end

    def deq_noaction_task(&block)
      Log.debug "deq_task:"+(empty? ? " empty" : "\n#{inspect_q}")
      while tw = @q_no_action.shift
        Log.debug "deq_noaction: #{tw.name}"
        yield(tw,nil)
      end
    end

    def deq_task(&block) # locality version
      Log.debug "deq_task:"+(empty? ? " empty" : "\n#{inspect_q}")
      queued = 0
      @n_turn.times do |turn|
        next if turn_empty?(turn)
        queued += deq_turn(turn,&block)
      end
      if queued>0
        Log.debug "queued:#{queued}"
      end
    end

    def deq_turn(turn,&block)
      queued = 0
      while true
        count = 0
        @host_map.by_id.each do |host_info|
          if host_info.idle_cores > 0
          if turn_empty?(turn)
            return queued
          elsif tw = deq_impl(host_info,turn)
            n_task_cores = tw.n_used_cores(host_info)
            Log.debug "deq: #{tw.name} n_task_cores=#{n_task_cores}"
            if host_info.idle_cores < n_task_cores
              m = "task.n_used_cores=#{n_task_cores} must be "+
                "<= host_info.idle_cores=#{host_info.idle_cores}"
              Log.fatal m
              raise RuntimeError,m
            else
              host_info.decrease(n_task_cores)
              yield(tw,host_info.id)
              count += 1
              queued += 1
            end
          end
          end
        end
        break if count == 0
      end
      queued
    end

    def turn_empty?(turn)
      empty?
    end

    def deq_impl(host_info=nil, turn=nil)
      @q_no_action.shift ||
        @q_input.shift(host_info) ||
        @q_no_input.shift(host_info)
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
      host_info = @host_map.by_id[hid]
      host_info.increase(tw.n_used_cores(host_info))
    end

    def _qstr(h,q)
      s = " #{h}: size=#{q.size} "
      case q.size
      when 0
        s << "[]\n"
      when 1
        s << "[#{q.first.name}]\n"
      when 2
        s << "[#{q.first.name}, #{q.last.name}]\n"
      else
        s << "[#{q.first.name},..,#{q.last.name}]\n"
      end
      s
    end

    def inspect_q
      _qstr("noaction",@q_no_action) +
      _qstr("input",   @q_input) +
      _qstr("no_input",@q_no_input)
    end

  end
end
