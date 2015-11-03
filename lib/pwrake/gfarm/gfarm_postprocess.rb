module Pwrake

  class GfarmPostprocess

    def initialize(runner)
      @io = IO.popen('gfwhere-pipe','r+')
      @io.sync = true
      @hdl = Handler.new(runner,@io,@io)
      @chan = Channel.new(@hdl)
    end

    def run(filename)
      begin
        @hdl.iow.puts(filename)
        @hdl.iow.flush
      rescue Errno::EPIPE
        Log.warn "GfarmPostprocess#run: Errno::EPIPE for #{filename}"
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
          a = []
          break
        else
          a = s.split(/\s+/)
          break
        end
      end
      Log.debug "Gfarm file=#{filename} nodes=#{a.join("|")}"
      a
    end

    def close
      @io.close
    end
  end
end
