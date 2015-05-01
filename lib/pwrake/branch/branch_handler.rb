
module Pwrake

  class BranchHandler

    RE_ID='\d+'

    def initialize(queue,iow,comm_set)
      @queue = queue
      @iow = iow
      @comm_set = comm_set
      @tasks = []
    end

    def on_read(io)
      s = io.gets
      # receive command from main pwrake
      #Log.debug "#{self.class.to_s}#on_read: #{s.chomp}"
      case s
      when /^(\d+):(.+)$/o
        id, tname = $1,$2
        @queue[id].enq(tname)

      when /^exit_branch$/
        Log.debug "#{self.class.to_s}#on_read: exit_branch"
        @queue.each_value{|q| q.finish}
        @comm_set.close_all
        #return true

      when /^kill:(.*)$/o
        sig = $1
        Log.warn "#{self.class.to_s}#on_read: kill #{sig}"
        @comm_set.terminate(sig)

      else
        puts "Invalid item for BranchHandler#on_read: #{s}"
      end
      return false
    end

  end
end
