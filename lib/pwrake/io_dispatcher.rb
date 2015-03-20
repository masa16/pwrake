module Pwrake

  class IODispatcher

    def initialize
      @rd_io = []
      @rd_hdl = {}
      @wr_io = []
      @wr_hdl = {}
    end

    def attach_read(io,handler)
      @rd_hdl[io] = handler
      @rd_io.push(io)
    end

    def detach_read(io)
      @rd_hdl.delete(io)
      @rd_io.delete(io)
    end

    def close_all
      @rd_io.each{|io| io.close}
    end

    def event_loop
      while !(@rd_io.empty? and @wr_io.empty?)
        io_sel = IO.select(@rd_io,@wr_io,nil)
        for io in io_sel[0]
          if io.eof?
            detach_read(io)
          else
            if @rd_hdl[io].on_read
              p :close_all
              close_all
              return
            end
          end
        end
      end
    end

  end
end
