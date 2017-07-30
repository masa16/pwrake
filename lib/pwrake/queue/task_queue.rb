require "pwrake/queue/queue_array"
require "pwrake/queue/no_action_queue"

module Pwrake

  class TaskQueue

    def initialize(hostinfo_by_id, group_map=nil)
      @enable_steal = true
      @q_no_action = NoActionQueue.new
      @q_reserved = Hash.new

      @hostinfo_by_id = hostinfo_by_id

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
      Log.debug "deq_noaction_task:"+(empty? ? " (empty)" : "\n#{inspect_q}")
      while tw = @q_no_action.shift
        Log.debug "deq_noaction: #{tw.name}"
        yield(tw)
      end
    end

    def deq_task(&block) # locality version
      Log.debug "deq_task from:"+(empty? ? " (empty)" : "\n#{inspect_q}")
      queued = 0
      @n_turn.times do |turn|
        next if turn_empty?(turn)
        queued += deq_turn(turn,&block)
      end
    end

    def deq_turn(turn,&block)
      queued = 0
      while true
        count = 0
        @hostinfo_by_id.each_value do |host_info|
          #Log.debug "TaskQueue#deq_turn host_info=#{host_info.name}"
          if turn_empty?(turn)
            return queued
          elsif (n_idle = host_info.idle_cores) && n_idle > 0
            if tw = @q_reserved[host_info]
              n_use = tw.n_used_cores(host_info)
              if n_idle < n_use
                next
              end
              @q_reserved.delete(host_info)
              Log.debug "deq_reserve: #{tw.name} n_use_cores=#{n_use}"
            elsif tw = deq_impl(host_info,turn)
              n_use = tw.n_used_cores(host_info)
              if n_idle < n_use
                @q_reserved[host_info] = tw
                Log.debug "reserve host: #{host_info.name} for #{tw.name} (#{n_use} cores)"
                next
              end
              Log.debug "deq: #{tw.name} n_use_cores=#{n_use}"
            end
            if tw
              yield(tw,host_info,n_use)
              count += 1
              queued += 1
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
      @q_reserved.clear
      @q_input.clear
      @q_no_input.clear
    end

    def empty?
      @q_no_action.empty? &&
        @q_reserved.empty? &&
        @q_input.empty? &&
        @q_no_input.empty?
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

    def drop_host(host_info)
    end

  end
end
