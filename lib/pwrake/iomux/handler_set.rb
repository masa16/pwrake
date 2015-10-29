module Pwrake

  class HandlerSet < Array

    def kill(sig)
      each do |hdl|
        hdl.iow.puts("kill:#{sig}")
        hdl.iow.flush
      end
    end

    def exit
      each{|hdl| hdl.put_line("exit")}
      each{|hdl| hdl.wait_message("exited")}
    end

  end
end
