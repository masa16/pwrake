module Pwrake

  class GfarmPostprocess

    def initialize(runner)
      @io = IO.popen('gfwhere-pipe','r+')
      @io.sync = true
      @hdl = AIO::Handler.new(runner,@io,@io)
    end

    def run(task_wrap)
      if !task_wrap.is_file_task?
        return []
      end
      filename = task_wrap.name
      begin
        @hdl.put_line(filename)
      rescue Errno::EPIPE
        Log.warn "GfarmPostprocess#run: Errno::EPIPE for #{filename}"
        return []
      end
      s = @hdl.get_line
      if s.nil?
        raise "gfwhere: unexpected end"
      end
      s.chomp!
      if s != filename
        raise "gfwhere: file=#{filename}, result=#{s}"
      end
      while s = @hdl.get_line
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
