
module Pwrake

  class MasterHandler

    RE_ID='\d+'

    def initialize(taskq,iow,comm_set)
      @taskq_by_host = taskq
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
        @taskq_by_host[id].enq(tname)

      when /^exit_branch$/
        Log.debug "#{self.class.to_s}#on_read: exit_branch"
        @taskq_by_host.each_value{|q| q.finish}
        @comm_set.close_all
        #return true

      when /^kill:(.*)$/o
        sig = $1
        Log.warn "#{self.class.to_s}#on_read: kill #{sig}"
        @comm_set.terminate(sig)

      else
        puts "Invalid item for MasterHandler#on_read: #{s}"
      end
      return false
    end

    def _puts(s)
      @iow.puts(s)
      @iow.flush
    end

    def ncore(comm)
      _puts "ncore:#{comm.id}:#{comm.ncore}"
    end

    def ncore_done
      _puts "ncore:done"
    end

    def branch_setup_done
      _puts "branch_setup:done"
    end

    def branch_end
      _puts "branch_end"
    end

    def taskfail(id,name)
      _puts "taskfail:#{id}:#{name}"
    end

    def taskend(id,name)
      _puts "taskend:#{id}:#{name}"
    end

    def close
      @iow.close
    end

  end
end
