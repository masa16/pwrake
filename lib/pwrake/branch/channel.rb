module Pwrake

  class Channel

    def initialize(io,id)
      @io = io
      @id = id
      @queue = []
      @queue_err = []
      @fiber = nil
    end

    attr_reader :io, :id, :queue

    def puts(s)
      @io.puts("#{@id}:#{s}")
      @io.flush
    end

    def flush
      @io.flush
    end

    def gets
      @fiber = Fiber.current
      while @queue.empty?
        Fiber.yield
      end
      @fiber = nil
      @queue.shift
    end

    def gets_err
      @fiber = Fiber.current
      while @queue_err.empty?
        Fiber.yield
      end
      @fiber = nil
      @queue_err.shift
    end

    def resume
      @fiber.resume if @fiber
    end

    def enq(item)
      @queue.push(item)
      resume
    end

    def enq_err(item)
      @queue_err.push(item)
      resume
    end

  end
end
