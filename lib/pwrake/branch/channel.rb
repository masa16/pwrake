module Pwrake

  class Channel

    def initialize(io,id)
      @io = io
      @id = id
      @queue = FiberQueue.new
      @fiber = nil
    end

    attr_reader :io, :id, :queue

    def puts(s)
      @io.puts("#{@id}:#{s}")
      @io.flush
    end

    def start
      @io.puts("start:#{@id}")
      @io.flush
    end

    def close
      puts "exit"
    end

    def closed?
      @io.closed?
    end

    def enq(x)
      @queue.enq(x)
    end

    def deq
      @queue.deq
    end

  end
end
