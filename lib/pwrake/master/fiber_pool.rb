module Pwrake

  class FiberPool

    def initialize(max_fiber=2,&block)
      @new_fiber_block = block
      @max_fiber = max_fiber
      @count = 0
      @fibers = []
      @idle_fiber = []
      @q = []
      @new_fiber_start_time = Pwrake.clock-10
    end

    def enq(x)
      @q.push(x)
      @count += 1
      if @idle_fiber.empty? and @fibers.size < @max_fiber and
          Pwrake.clock - @new_fiber_start_time > 0.1
        @idle_fiber << new_fiber
      end
      f = @idle_fiber.shift
      f.resume if f
      @finished
    end

    def deq
      while @q.empty?
        return nil if @finished
        @idle_fiber.push(Fiber.current)
        Fiber.yield
      end
      @q.shift
    end

    def count_down
      @count -= 1
    end

    def empty?
      @count == 0
    end

    def finish
      @finished = true
      run
      while f = @fibers.shift
        if f.alive?
          $stderr.puts "FiberPool#finish: fiber is still alive."
        end
      end
    end

    def run
      cond = !@idle_fiber.empty?
      while f = @idle_fiber.shift
        f.resume
      end
      cond
    end

    def new_fiber
      @fibers.push(fb = @new_fiber_block.call(self))
      fb
    end

  end
end
