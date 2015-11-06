module Pwrake

  class TimeoutError < IOError; end

  class Runner

    def initialize(timeout=nil)
      @timeout = timeout
      @handler = {}
      @hb_time = {}
    end

    attr_reader :handler

    def add_handler(hdl)
      @handler[hdl.ior] ||= hdl
    end

    def delete_handler(hdl)
      @handler.delete(hdl.ior)
    end

    def run
      while !(io_set = @handler.keys).empty?
        sel, = IO.select(io_set,nil,nil,@timeout)
        if sel.nil?
          raise TimeoutError,"Timeout (#{@timeout} s) in IO.select"
        end
        sel.each do |io|
          @handler[io].process_line
        end
        if @timeout && @hb_earliest
          if Time.now - @hb_earliest > @timeout
            io = @hb_time.key(@hb_earliest)
            raise TimeoutError,"Timeout (#{@timeout}s) "+
              "in Heartbeat from host=#{get_host(io)}"
          end
        end
      end
    end

    def finish
      @handler.each do |io,hdl|
        hdl.finish
      end
    end

    # used to print an error message
    def get_host(io)
      hdl = @handler[io]
      h = hdl.respond_to?(:host) ? hdl.host : nil
    end

    # called when IO start and receive heartbeat
    def heartbeat(io)
      @hb_time[io] = Time.now
      @hb_earliest = @hb_time.values.min
      #Log.debug "heartbeat: host=#{get_host(io)}"
    end

  end
end
