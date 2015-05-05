module Pwrake

  class TimeoutError < IOError; end

  class IODispatcher

    class ExitHandler
      def on_read(io)
        true
      end
    end

    def initialize
      @rd_io = []
      @rd_hdl = {}
      @hb_time = {}
      @ior,@iow = IO.pipe
      attach(@ior,ExitHandler.new)
    end

    def finish
      Log.debug "#{self.class}.finish"
      @iow.puts("")
    end

    def attach(io,hdl=nil)
      @rd_hdl[io] = hdl
      @rd_io.push(io)
      @hb_earliest ||= @hb_time[io] = Time.now
    end

    def detach(io)
      @rd_hdl.delete(io)
      @rd_io.delete(io)
    end

    def close_all
      @rd_io.each{|io| io.close}
    end

    def heartbeat(io)
      @hb_time[io] = Time.now
      @hb_earliest = @hb_time.values.min
    end

    def event_loop(timeout=nil)
      while !@rd_io.empty?
        io_sel = IO.select(@rd_io,nil,nil,timeout)
        if io_sel
          for io in io_sel[0]
            if io.eof?
              detach(io)
            else
              return if @rd_hdl[io].on_read(io)
            end
          end
        else
          raise TimeoutError,"timeout(#{timeout} s)"
        end
        if timeout && (Time.now - @hb_earliest > timeout) # || rand < 0.05)
          raise TimeoutError,"heartbeat timeout(#{timeout}s)"
        end
      end
    end

    def event_loop_block
      while !@rd_io.empty?
        io_sel = IO.select(@rd_io,nil,nil)
        for io in io_sel[0]
          if io.eof?
            detach(io)
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
