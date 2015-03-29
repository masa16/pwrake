require 'fiber'

module Pwrake

  class FiberQueue

    def initialize
      @q = []
      @waiter = []
      @finished = false
    end

    def enq(x)
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

    def finish
      @finished = true
      while f = @waiter.shift
        f.resume
      end
    end

  end
end
