module Pwrake

  class FiberPool

    def initialize(max_fiber=2,&block)
      @new_fiber_block = block
      @max_fiber = max_fiber
      @n_fiber = 0
      @count = 0
      @idle_fiber = []
      @q = []
      @new_fiber_start_time = Time.now-10
    end

    def set_block(&block)
      @block = block
    end

    def enq(x)
      @q.push(x)
      @count += 1
      if @idle_fiber.empty? and @n_fiber < @max_fiber and
          Time.now - @new_fiber_start_time > 0.1
        @idle_fiber << new_fiber
        @n_fiber += 1
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
    end

    def run
      r = !@idle_fiber.empty?
      while f = @idle_fiber.shift
        f.resume
      end
      r
    end

    def new_fiber
      @new_fiber_block.call(self)
    end

  end
end

