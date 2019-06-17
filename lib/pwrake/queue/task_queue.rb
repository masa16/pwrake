require "pwrake/queue/queue_array"
require "pwrake/queue/no_action_queue"
require "pwrake/queue/non_locality_queue"

module Pwrake

  class TaskQueue

    def initialize(queue_class, hostinfo_by_id, group_map=nil)
      @queue_class = Pwrake.const_get(queue_class)
      @hostinfo_by_id = hostinfo_by_id
      @q_no_action = NoActionQueue.new
      @q_reserved = Hash.new
      def @q_reserved.first
        super.last
      end
      def @q_reserved.last
        self[keys.last]
      end

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

      # median number of cores
      a = @hostinfo_by_id.map{|id,host_info| host_info.ncore}.sort
      n = a.size
      @n_core = (n%2==0) ? (a[n/2-1]+a[n/2])/2 : a[(n-1)/2]

      case Rake.application.pwrake_options['MULTICORE_TASK_PRIORITY']
      when /^half/i, nil
        @core_threshold = @n_core/2
      when /^n(o(ne)?)?/i
        @core_threshold = 0
      else
        raise RuntimeError,"unknown option for MULTICORE_TASK_PRIORITY: "+
          Rake.application.pwrake_options['MULTICORE_TASK_PRIORITY']
      end
      @q = ((@core_threshold==0)?1:2).times.map do
        @queue_class.new(hostinfo_by_id, @array_class, group_map)
      end
    end

    # enq
    def enq(tw)
      if tw.nil? || tw.actions.empty?
        @q_no_action.push(tw)
      else
        n = tw.use_cores
        n += @n_core if n <= 0
        i = (n > @core_threshold) ? 0 : 1
        @q[i].enq_impl(tw)
      end
    end

    def deq_task(&block)
      Log.debug "deq_task from:"+(empty? ? " (empty)" : "\n#{inspect_q}")
      deq_noaction_task(&block)
      deq_reserve(&block)
      @q.each do |q|
        unless q.empty?
          q.n_turn.times{|turn| deq_turn(q,turn,&block) }
        end
      end
    end

    def deq_noaction_task(&block)
      while tw = @q_no_action.shift
        Log.debug "deq_noaction: #{tw.name}"
        yield(tw)
      end
    end

    def deq_reserve(&block)
      @q_reserved.each do |host_info,tw|
        n_idle = host_info.idle_cores || 0
        n_core = tw.n_used_cores(host_info)
        if n_idle >= n_core
          @q_reserved.delete(host_info)
          Log.debug "deq_reserve: #{tw.name} n_use_cores=#{n_core}"
          yield(tw,host_info,n_core)
        end
      end
    end

    def deq_turn(q,turn,&block)
      begin
        count = 0
        @hostinfo_by_id.each_value do |host_info|
          return if q.turn_empty?(turn)
          n_idle = host_info.idle_cores || 0
          next if n_idle == 0 || @q_reserved[host_info]
          if tw = q.deq_impl(host_info,turn)
            n_core = tw.n_used_cores(host_info)
            if n_idle >= n_core
              Log.debug "deq: #{tw.name} n_use_cores=#{n_core}"
              yield(tw,host_info,n_core)
              count += 1
            else
              @q_reserved[host_info] = tw
              Log.debug "reserve host: #{host_info.name} for #{tw.name} (#{n_core} cores)"
            end
          end
        end
      end while count > 0
    end

    def clear
      @q_no_action.clear
      @q_reserved.clear
      @q.each{|q| q.clear}
    end

    def empty?
      @q_no_action.empty? &&
      @q_reserved.empty? &&
      @q.all?{|q| q.empty?}
    end

    def self._qstr(h,q)
      s = " #{h}: size=#{q.size} "
      case q.size
      when 0
        s << "[]\n"
      when 1
        s << "[#{q.first.name}]\n"
      when 2
        s << "[#{q.first.name}, #{q.last.name}]\n"
      else
        s << "[#{q.first.name}, .., #{q.last.name}]\n"
      end
      s
    end

    def inspect_q
      [TaskQueue._qstr("noaction",@q_no_action),
       *@q.map{|q| q.inspect_q},
       TaskQueue._qstr("reserved",@q_reserved),
      ].inject(&:+)
    end

    def drop_host(host_info)
      @q.each{|q| q.drop_host(host_info)}
    end

  end
end
