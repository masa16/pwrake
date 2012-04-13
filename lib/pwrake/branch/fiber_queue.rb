require 'fiber'

module Pwrake
  class FiberQueue
    def initialize
      @q = []
      @empty = []
      @fibuf = []
      @finished = false
    end

    def enq(a)
      a.each do |x|
        @q.push(x)
        f = @empty.shift
        f.resume if f
      end

      while f = @fibuf.shift
        f.resume
      end
    end

    def deq
      if @finished and @q.empty?
        return nil
      end

      if @q.empty?
        @empty.push(Fiber.current)
        Fiber.yield
      end

      return @q.shift
    end

    def resume
      while f = @fibuf.shift
        f.resume
      end
    end

    def finish
      @finished = true
    end

    def release(a)
    end
  end
end
