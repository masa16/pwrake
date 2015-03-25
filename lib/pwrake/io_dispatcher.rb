module Pwrake

  class IODispatcher

    def initialize
      @rd_io = []
      @rd_hdl = {}
      @rd_hdl = {}
    end

    def attach_handler(io,handler=nil)
      @rd_hdl[io] = handler
      @rd_io.push(io)
    end

    def detach_io(io)
      @rd_hdl.delete(io)
      @rd_io.delete(io)
    end

    def attach_communicator(comm)
      @rd_hdl[comm.ior] = comm
      @rd_hdl[comm.ioe] = comm
      @rd_io.push(comm.ior)
      @rd_io.push(comm.ioe)
    end

    def detach_communicator(comm)
      @rd_hdl.delete(comm.ior)
      @rd_hdl.delete(comm.ioe)
      @rd_io.delete(comm.ior)
      @rd_io.delete(comm.ioe)
    end

    def close_all
      @rd_io.each{|io| io.close}
    end

    def event_loop(&block)
      if block_given?
        b = block
      else
        #b = proc{|io| @rd_hdl[io].read_handler(io)}
        b = proc{|io| @rd_hdl[io].on_read(io)}
      end
      while !@rd_io.empty?
        io_sel = IO.select(@rd_io,nil,nil)
        for io in io_sel[0]
          if io.eof?
            detach_io(io)
          else
            return if b.call(io)
          end
        end
      end
    end

    def event_once(timeout=10,&block)
      ios = @rd_io.dup
      $stderr.puts ios.inspect
      while !ios.empty? and io_sel = select(ios,nil,nil,timeout)
        for io in io_sel[0]
          if io.eof?
            detach_read(io)
          else
            yield(io)
          end
          ios.delete(io)
        end
      end
      $stderr.puts "pass1"
      # worker timeout
      ios.each do |io|
        Util.puts "timeout: #{io.inspect}"
      end
      # error check
      if !ios.empty?
        raise RuntimeError,"Error in connecting to worker"
      end
      $stderr.puts "end: event_once"
    end

  end
end
