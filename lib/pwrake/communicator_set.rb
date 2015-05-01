module Pwrake

  class CommunicatorSet < Array

    def initialize
      super
      @signal_procs = []
      @killed = 0
    end

    def signal_trap(&block)
      @signal_procs << block
    end

    def kill(sig)
      each{|comm| comm.kill(sig)}
    end

    def close_all
      each{|comm| comm.close}
      clear
    end

    def kill_procs(sig)
      @signal_procs.each{|b| b.call(sig)}
    end

    def terminate(sig)
      # log writing failed. can't be called from trap context
      if Rake.application.options.debug
        $stderr.puts "\nSignal trapped. (sig=#{sig} pid=#{Process.pid}"+
          " thread=#{Thread.current} ##{@killed})"
        $stderr.puts caller
      else
        $stderr.puts "\nSignal trapped. (sig=#{sig} pid=#{Process.pid} ##{@killed})"
      end
      case @killed
      when 0
        $stderr.puts "Exiting..."
        kill(sig)
        close_all
        kill_procs(sig)
      when 1
        $stderr.puts "Once more Ctrl-C (SIGINT) for exit."
      else
        Kernel.exit # must wait for nomral exit
      end
      @killed += 1
    end

  end
end
