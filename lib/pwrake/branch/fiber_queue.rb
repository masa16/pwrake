require 'fiber'

module Pwrake

  class FiberQueue

    def initialize
      @q = []
      @empty = []
      @finished = false
    end

    def enq(x)
      @q.push(x)
      f = @empty.shift
      f.resume if f
    end

    def deq
      while @q.empty?
        return nil if @finished
        @empty.push(Fiber.current)
        Fiber.yield
      end
      return @q.shift
    end

    def finish
      @finished = true
      f = @empty.shift
      f.resume if f
    end

  end
end
