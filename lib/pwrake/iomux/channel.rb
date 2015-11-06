module Pwrake

  class Channel

    def initialize(handler,id=nil)
      if @id = id
        @pre = "#{@id}:"
      else
        @pre = ""
      end
      if !handler.kind_of?(Handler)
        raise TypeError, "Argument must be Handler but #{handler.class}"
      end
      @handler = handler
    end

    attr_reader :id, :handler, :fiber

    def ior
      @handler.ior
    end

    def run_fiber(*args)
      if @fiber.nil?
        m = "Channel#run_fiber: @fiber is nil,"+
          " args=#{args.inspect} @id=#{@id}"
        $stderr.puts m
        Log.debug m
      else
        @fiber.resume(*args)
      end
    end

    def finish
      if !@fiber.nil?
        @fiber.resume(nil)
      end
    end

    def get_line
      @handler.add_channel(self)
      @fiber = Fiber.current
      line = Fiber.yield
      @fiber = nil
      @handler.delete_channel(self)
      return line
    end

    def put_line(line)
      @handler.put_line "#{@pre}#{line}"
    end

    def puts(line)
      @handler.puts "#{@pre}#{line}"
    end

    def gets
      if @id
        raise RuntimeError,"gets is invalid when @id is non-nil"
      else
        @handler.gets
      end
    end

    def inspect
      "#<#{self.class} io=#{ior.inspect} id=#{id.inspect}>"
    end
  end

end
