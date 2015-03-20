
module Pwrake

  class BranchHandler

    RE_ID='\d+'

    def initialize(io,queue)
      @io = io
      @channel = {}
      @queue = queue
      @tasks = []
    end

    attr_reader :io

    def add_channel(id,channel)
      @channel[id] = channel
    end

    def resume
      @channel.each{|k,ch| ch.resume}
    end

    def on_read
      s = @io.gets
      #p s
      # receive command from main pwrake
      case s
      when /^(\d+):(.+)$/o
        id, tname = $1,$2
        task = Rake.application[tname]
        @tasks.push(task)

      when /^end_task_list$/o
        @queue.enq(@tasks)
        @tasks.clear

      when /^exit_connection$/o
        p s
        #@queue.enq([false])
        #@queue.finish
        Util.dputs "branch:exit_connection"
        return true

      when /^kill:(.*)$/o
        sig = $1
        # Util.puts "branch:kill:#{sig}"
        Communicator.kill(sig)
        Kernel.exit
        return true
      else
        puts "Invalild item: #{s}"
      end
      resume
      return false
    end

  end
end
