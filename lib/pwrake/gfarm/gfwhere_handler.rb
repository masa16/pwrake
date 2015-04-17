module Pwrake

  class GfwhereHandler

    def initialize
      @io = IO.popen('gfwhere-pipe','r+')
      @io.sync = true
    end
    attr_reader :io

    def close
      @finished = true
      @io.close_read
    end

    def run(t)
      @fiber = Fiber.current
      @file = t.name # t.last
      @io.puts(@file)
      #puts "t=#{t} @fiber=#{@fiber.inspect}"
      Fiber.yield
    end

    def on_read(io)
      #puts "@fiber=#{@fiber.inspect}"
      f = @fiber
      @fiber = nil
      f.resume(gfwhere_result)
      @finished
    end

    def gfwhere_result
      s = @io.gets
      if s.nil?
        raise "gfwhere: unexpected end"
      end
      s.chomp!
      if s != @file
        raise "gfwhere: file=#{@file}, result=#{s}"
      end
      @file = nil
      while s = @io.gets
        s.chomp!
        case s
        when /^gfarm:\/\//
          next
        when /^Error:/
          return []
        else
          return s.split(/\s+/)
        end
      end
    end

  end
end
