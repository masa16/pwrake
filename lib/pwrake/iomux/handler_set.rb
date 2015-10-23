module Pwrake

  class HandlerSet < Array

    def initialize
      super
      @killed = 0
    end

    def kill_all(sig)
      each{|hdl| hdl.kill(sig)}
    end

    def close_all
      each{|hdl| hdl.close}
    end

    def wait_close(meth_name, end_msg)
      each do |hdl|
        while line = hdl.ior.gets
          line.chomp!
          m = "#{meth_name}: #{line} host=#{hdl.host}"
          if line == end_msg
            Log.debug m
          else
            Log.error m
          end
        end
      end
      clear
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
        kill_all(sig)
        close_all()
      when 1
        $stderr.puts "Once more Ctrl-C (SIGINT) for exit."
      else
        Kernel.exit # must wait for nomral exit
      end
      @killed += 1
    end

  end
end
