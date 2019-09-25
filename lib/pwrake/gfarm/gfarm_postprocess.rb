module Pwrake

  class GfwhereError < StandardError; end

  class GfarmPostprocess

    def initialize(selector)
      @io = IO.popen('gfwhere-pipe','r+')
      @io.sync = true
      @reader = NBIO::Reader.new(selector,@io)
      @writer = NBIO::Writer.new(selector,@io)
    end

    def run(task_wrap)
      if !task_wrap.is_file_task?
        return []
      end
      filename = task_wrap.name
      begin
        @writer.put_line(filename)
      rescue Errno::EPIPE
        Log.warn "GfarmPostprocess#run: Errno::EPIPE for #{filename}"
        return []
      end
      s = @reader.get_line
      if s.nil?
        raise GfwhereError,"lost connection to gfwhere-pipe"
      end
      s.chomp!
      if s != filename
        raise GfwhereError,"path mismatch: send=#{filename}, return=#{s}"
      end
      while s = @reader.get_line
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
      #Log.debug "Gfarm file=#{filename} nodes=#{a.join("|")}"
      a
    end

    def close
      @writer.halt
      @reader.halt
      @io.close
    end
  end
end
