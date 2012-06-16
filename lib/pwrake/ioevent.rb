module Pwrake

  class IOEvent

    def initialize(time_out=nil)
      @time_out = time_out
      @io_set = []
      @closed = []
      @data_by_io = {}
    end

    attr_reader :closed

    def add_io(io,data=nil)
      data = io if data.nil?
      @data_by_io[io] = data
      @io_set.push(io)
    end

    def delete_io(io)
      @data_by_io.delete(io)
      @io_set.delete(io)
    end

    def close(io)
      io.close
      @io_set.delete(io)
      @data_by_io.delete(io)
      @closed << io
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
        # print "##{s.chomp.inspect}\n"
        block.call(@data_by_io[io],s)
      end
    end

    def event_each(timeout=10,&block)
      io_set = @io_set.dup
      #io_set.each{|io| $stderr.puts "#{io.inspect} #{io.closed?}"}
      #$stderr.flush
      while !io_set.empty? and io_sel = select(io_set,nil,nil,timeout)
        #$stderr.puts "pass #{io_sel.inspect}"
        #$stderr.flush
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
        raise "Error in connecting to worker"
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

    def finish(exit_cmd)
      each do |conn|
        if conn.respond_to?(:send_cmd)
          # Util.puts "send #{exit_cmd} to #{conn.inspect}"
          conn.send_cmd exit_cmd
        end
      end
      # Util.puts "# pass 1"
      event_loop do |data,s|
        Util.print s
      end
      # Util.puts "# pass 2"
    end
  end
end
