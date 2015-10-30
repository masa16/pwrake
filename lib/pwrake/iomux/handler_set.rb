module Pwrake

  class HandlerSet < Array

    def kill(sig)
      each do |hdl|
        hdl.iow.puts("kill:#{sig}")
        hdl.iow.flush
      end
    end

    def exit
      a = []
      each do |hdl|
        iow = hdl.iow
        begin
          iow.puts "exit"
          iow.flush
          a << hdl
          Log.debug "HandlerSet#exit iow=#{iow.inspect}"
        rescue Errno::EPIPE => e
          if Rake.application.options.debug
            $stderr.puts "Errno::EPIPE in #{self.class}.exit iow=#{iow.inspect}"
            $stderr.puts e.backtrace.join("\n")
          end
          Log.error "Errno::EPIPE in #{self.class}.exit iow=#{iow.inspect}\n"+
            e.backtrace.join("\n")
        end
      end
      a.each{|hdl| hdl.wait_message("exited")}
      Log.debug "HandlerSet#exit end"
    end

  end
end
