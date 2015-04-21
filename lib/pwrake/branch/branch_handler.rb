
module Pwrake

  class BranchHandler

    RE_ID='\d+'

    def initialize(queue,iow)
      @queue = queue
      @iow = iow
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
        @queue.each_value{|q| q.finish}
        #return true

      when /^kill:(.*)$/o
        sig = $1
        $stderr.puts "#{self.class.to_s}#on_read: kill #{sig}"
        WorkerCommunicator.kill(sig)

      else
        puts "Invalid item for BranchHandler#on_read: #{s}"
      end
      return false
    end

  end
end
