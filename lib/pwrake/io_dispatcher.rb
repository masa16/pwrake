module Pwrake

  class IODispatcher

    class ExitHandler
      def on_read(io)
        true
      end
    end

    def initialize
      @rd_io = []
      @rd_hdl = {}
      @rd_hdl = {}
      @ior,@iow = IO.pipe
      attach_handler(@ior,ExitHandler.new)
    end

    def finish
      @iow.puts("")
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

    def event_loop
      while !@rd_io.empty?
        io_sel = IO.select(@rd_io,nil,nil)
        for io in io_sel[0]
          if io.eof?
            detach_io(io)
          else
            return if @rd_hdl[io].on_read(io)
          end
        end
      end
    end

    def event_loop_block
      while !@rd_io.empty?
        io_sel = IO.select(@rd_io,nil,nil)
        for io in io_sel[0]
          if io.eof?
            detach_io(io)
          else
            return if yield(io)
          end
        end
      end
    end

    def self.event_once(io_list,timeout)
      while !io_list.empty? and io_sel = select(io_list,nil,nil,timeout)
        for io in io_sel[0]
          yield(io)
          io_list.delete(io)
        end
      end
    end

  end
end
