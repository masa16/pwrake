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
      @handler.set_channel(self)
    end

    attr_reader :id, :handler, :fiber

    def ior
      @handler.ior
    end

    def run_fiber(*args)
      @fiber.resume(*args)
    end

    def get_line
      @handler.runner.add_channel(self)
      @fiber = Fiber.current
      line = Fiber.yield
      @fiber = nil
      @handler.runner.delete_channel(self)
      return line
    end

    def put_line(line)
      @handler.put_line "#{@pre}#{line}"
    end

    def inspect
      "#<#{self.class} io=#{ior.inspect} id=#{id.inspect}>"
    end
  end

end
