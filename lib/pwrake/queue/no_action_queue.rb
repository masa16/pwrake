module Pwrake

  class NoActionQueue

    def initialize
      @que = []
      prio = Rake.application.pwrake_options['NOACTION_QUEUE_PRIORITY'] || 'fifo'
      case prio
      when /fifo/i
        @prio = 0
        Log.debug "NOACTION_QUEUE_PRIORITY=FIFO"
      when /lifo/i
        @prio = 1
        Log.debug "NOACTION_QUEUE_PRIORITY=LIFO"
      when /rand/i
        @prio = 2
        Log.debug "NOACTION_QUEUE_PRIORITY=RAND"
      else
        raise RuntimeError,"unknown option for NOACTION_QUEUE_PRIORITY: "+prio
      end
    end

    def push(obj)
      @que.push obj
    end

    alias << push
    alias enq push

    def pop
      case @prio
      when 0
        x = @que.shift
      when 1
        x = @que.pop
      when 2
        x = @que.delete_at(rand(@que.size))
      end
      return x
    end

    alias shift pop
    alias deq pop

    def empty?
      @que.empty?
    end

    def clear
      @que.clear
    end

    def length
      @que.length
    end
    alias size length

    def first
      @que.first
    end

    def last
      @que.last
    end

  end
end
