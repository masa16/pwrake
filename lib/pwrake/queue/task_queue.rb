require "pwrake/queue/queue_array"
require "pwrake/queue/no_action_queue"
require "pwrake/queue/non_locality_queue"

module Pwrake

  class TaskQueue

    def initialize(queue_class, hostinfo_by_id, group_map=nil)
      @queue_class = Pwrake.const_get(queue_class)
      @hostinfo_by_id = hostinfo_by_id
      @lock = Monitor.new
      @q_no_action = NoActionQueue.new
      @q_reserved = Hash.new
      @nenq = 0
      @ndeq = 0
      def @q_reserved.first
        super.last
      end
      def @q_reserved.last
        self[keys.last]
      end

      pri = Rake.application.pwrake_options['QUEUE_PRIORITY'] || "LIFO"
      case pri
      when /^fifo$/i
        @array_class = FifoQueueArray
      when /^lifo$/i
        @array_class = LifoQueueArray
      when /^lihr$/i
        @array_class = LifoHrfQueueArray
      else
        raise RuntimeError,"unknown option for QUEUE_PRIORITY: "+pri
      end
      Log.debug "@array_class=#{@array_class.inspect}"

      # median number of cores
      a = @hostinfo_by_id.map{|id,host_info| host_info.ncore}.sort
      n = a.size
      @median_core = (n%2==0) ? (a[n/2-1]+a[n/2])/2 : a[(n-1)/2]

      @q = @queue_class.new(hostinfo_by_id,@array_class,@median_core,group_map)
    end

    def enq(tw)
      @lock.synchronize do
      if tw.nil? || tw.actions.empty?
        @q_no_action.push(tw)
      else
        @q.enq_impl(tw)
      end
      @nenq += 1
      end
    end

    def deq_task(&block)
      @lock.synchronize do
      if @nenq > 0
        Log.debug "deq_task nenq=#{@nenq}:"+(empty? ? " (empty)" : "\n"+inspect_q)
        @nenq = 0
      end
      deq_noaction_task(&block)
      deq_reserve(&block)
      @q.deq_start
      unless @q.empty?
        @q.turns.each{|turn| deq_turn(turn,&block) }
      end
      if @ndeq > 0
        Log.debug "deq_task ndeq=#{@ndeq}:"+(empty? ? " (empty)" : "\n"+inspect_q)
        @ndeq = 0
      end
      end
    end

    def deq_noaction_task(&block)
      while tw = @q_no_action.shift
        yield(tw)
        @ndeq += 1
      end
    end

    def deq_reserve(&block)
      @q_reserved.each do |host_info,tw|
        n_idle = host_info.idle_cores || 0
        n_core = tw.use_cores(host_info)
        if n_idle >= n_core
          @q_reserved.delete(host_info)
          Log.debug "deq_reserve: #{tw.name} n_use_cores=#{n_core}"
          yield(tw,host_info,n_core)
          @ndeq += 1
        end
      end
    end

    def deq_turn(turn,&block)
      begin
        count = 0
        @hostinfo_by_id.each_value do |host_info|
          return if @q.turn_empty?(turn)
          n_idle = host_info.idle_cores || 0
          next if n_idle == 0 || @q_reserved[host_info]
          if tw = @q.deq_impl(host_info,turn)
            n_core = tw.use_cores(host_info)
            if n_idle >= n_core
              Log.debug "deq: #{tw.name} n_use_cores=#{n_core}"
              yield(tw,host_info,n_core)
              count += 1
              @ndeq += 1
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
      @q.clear
    end

    def empty?
      @q_no_action.empty? &&
      @q_reserved.empty? &&
      @q.empty?
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
      TaskQueue._qstr("noaction",@q_no_action) +
      @q.inspect_q +
      TaskQueue._qstr("reserved",@q_reserved)
    end

    def drop_host(host_info)
      @q.drop_host(host_info)
    end
  end
end
