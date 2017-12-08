require 'fiber'

module Pwrake

  class FiberQueueError < StandardError
  end

  class FiberQueue

    def initialize(log=nil)
      @q = []
      @waiter = []
      @finished = false
      @log = log
    end

    def enq(x)
      if @finished
        raise FiberQueueError,"cannot enq to already finished queue"
      end
      @q.push(x)
      f = @waiter.shift
      f.resume if f
    end

    def deq
      while @q.empty?
        return nil if @finished
        @waiter.push(Fiber.current)
        Fiber.yield
      end
      return @q.shift
    end

    def deq_nonblock
      @q.shift
    end

    def finish
      @finished = true
      while f = @waiter.shift
        begin
          f.resume
        rescue => exc
          if @log
            @log.error(([exc.to_s]+exc.backtrace).join("\n"))
          end
        end
      end
    end

  end
end
