module Pwrake

  class GfarmPostprocess

    def initialize(runner)
      io = IO.popen('gfwhere-pipe','r+')
      io.sync = true
      @hdl = Handler.new(runner,io,io)
      @chan = Channel.new(@hdl)
    end

    def run(filename)
      begin
        @chan.put_line(filename)
      rescue Errno::EPIPE
        Log.warn "GfwhereHandler#run: Errno::EPIPE for #{filename}"
        return []
      end
      s = @chan.get_line
      if s.nil?
        raise "gfwhere: unexpected end"
      end
      s.chomp!
      if s != filename
        raise "gfwhere: file=#{filename}, result=#{s}"
      end
      while s = @chan.get_line
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

    def close
      @hdl.close
    end
  end
end
