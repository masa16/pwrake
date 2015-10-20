module Pwrake

  class TimeoutError < IOError; end

  class Runner

    def initialize
      @handler = {}
      @channel = {}
      @hb_time = {}
    end

    attr_reader :handler

    def check(arg,cls)
      if !arg.kind_of?(cls)
        raise TypeError, "Argument must be #{cls} but #{arg.class}"
      end
    end

    def set_handler(handler)
      check(handler,Handler)
      @handler[handler.ior] ||= handler
    end

    def add_channel(chan)
      check(chan,Channel)
      io = chan.ior
      check(io,IO)
      ch = chan.id
      (@channel[io] ||= {})[ch] = chan
    end

    def delete_channel(chan)
      check(chan,Channel)
      io = chan.ior
      check(io,IO)
      ch = chan.id
      @channel[io].delete(ch)
      @channel.delete(io) if @channel[io].empty?
    end

    def run(timeout=nil)
      while !(io_set = @channel.keys).empty?
        sel, = IO.select(io_set,nil,nil,timeout)
        if sel.nil?
          raise TimeoutError,"Timeout (#{timeout} s) in IO.select"
        end
        sel.each do |io|
          @handler[io].process_line
        end
        if timeout && @hb_earliest
          if Time.now - @hb_earliest > timeout
            io = @hb_time.key(@hb_earliest)
            raise TimeoutError,"Timeout (#{timeout}s) in Heartbeat from host=#{get_host(io)}"
          end
        end
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
