
module Pwrake

  class BranchHandler

    RE_ID='\d+'

    def initialize(queue)
      @queue = queue
      @tasks = []
    end

    def on_read(io)
      s = io.gets
      #$stderr.puts "BH#on_read: #{s}"
      # receive command from main pwrake
      case s
      when /^(\d+):(.+)$/o
        id, tname = $1,$2
        task = Rake.application[tname]
        @queue.enq(task)
        #$stderr.puts @queue.inspect

      when /^exit_branch$/
        @queue.finish
        #return true

      when /^kill:(.*)$/o
        sig = $1
        # Util.puts "branch:kill:#{sig}"
        Kernel.exit
        return true # ?
      else
        puts "Invalid item for BranchHandler#on_read: #{s}"
      end
      #resume
      return false
    end

  end
end
