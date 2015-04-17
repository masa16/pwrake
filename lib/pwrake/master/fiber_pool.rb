module Pwrake

  class FiberPool

    def initialize(handler_class,max_fiber,dispatcher)
      @handler_class = handler_class
      @max_fiber = max_fiber
      @dispatcher = dispatcher
      @waiter = []
      @q = []
      @handlers = []
      @new_fiber_start_time = Time.now-10
    end

    def set_block(&block)
      @block = block
    end

    def enq(x)
      @q.push(x)
      if @waiter.empty? and
          @handlers.size < @max_fiber and
          Time.now - @new_fiber_start_time > 0.001
        @waiter << new_fiber
      end
      f = @waiter.shift
      f.resume if f
      @finished
    end

    def deq
      while @q.empty?
        return nil if @finished
        @waiter.push(Fiber.current)
        Fiber.yield
      end
      @q.shift
    end

    def finish
      @finished = true
      while f=@waiter.shift
        f.resume
      end
    end

    def new_fiber
      handler = @handler_class.new
      @handlers << handler
      @dispatcher.attach_handler(handler.io,handler)
      #
      Log.debug "fiber_pool new_fiber count=#{@handlers.size}"
      @new_fiber_start_time = Time.now
      Fiber.new do
        while t = deq()
          r = handler.run(t)
          @block.call(t,r)
          if @finished && @q.empty?
            @dispatcher.detach_io(handler.io)
            handler.close
          end
        end
      end
    end

  end
end

