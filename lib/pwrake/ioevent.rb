module Pwrake

  class IOEvent

    def initialize(time_out=nil)
      @time_out = time_out
      @io_set = []
      @closed = []
      @data_by_io = {}
    end

    def add_io(io,data=nil)
      data = io if data.nil?
      @data_by_io[io] = data
      @io_set.push(io)
    end

    def close(io)
      Util.puts "closing #{io.inspect}"
      #puts "close called"
      io.close
      @io_set.delete(io)
      @closed << io
      @data_by_io[io] = nil
    end

    def each(&block)
      @data_by_io.values.each(&block)
    end

    def each_io(&block)
      @io_set.each(&block)
    end

    def event_for_io(io,&block)
      if io.eof?
        self.close(io)
      elsif s = io.gets
        #print "##{s.chomp.inspect}\n"
        block.call(@data_by_io[io],s)
      end
    end

    def event_each(timeout=10,&block)
      io_set = @io_set.dup
      while !io_set.empty? and io_sel = select(io_set,nil,nil,timeout)
        for io in io_sel[0]
          event_for_io(io,&block)
          io_set.delete(io)
        end
      end
      # worker timeout
      io_set.each do |io|
        Util.puts "timeout: #{io.inspect}"
      end
      # error check
      if !io_set.empty?
        raise "connect to worker error"
      end
    end

    def event_loop(&block)
      while !@io_set.empty? and io_sel = select(@io_set,nil,nil,@time_out)
        # io_sel=nil when timeout
        for io in io_sel[0]
          event_for_io(io,&block)
        end
      end
    end
  end
end
