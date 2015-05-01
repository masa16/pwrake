module Pwrake

  class Communicator

    attr_reader :ior, :ioe, :iow, :host

    def initialize(host,opts={})
      @host = host
      @opts = opts
      @ior,w0 = IO.pipe
      @ioe,w1 = IO.pipe
      r2,@iow = IO.pipe
      setup_connection(w0,w1,r2)
      @close_command = "change_me"
    end

    def setup_connection(w0,w1,r2)
    end

    def send_cmd(cmd)
      print cmd.to_str+"\n"
      flush
    end

    def print(x)
      #$stderr.puts "<"+x
      begin
        @iow.print x
      rescue Errno::EPIPE => e
        $stderr.puts "Errno::EPIPE in #{self.class}.print '#{x.chomp}'"
        if Rake.application.options.debug
          $stderr.puts e.backtrace.join("\n")
        end
      end
    end

    def puts(x)
      print x+"\n"
    end

    def flush
      begin
        @iow.flush
      rescue Errno::EPIPE => e
        $stderr.puts "Errno::EPIPE in #{self.class}.flush"
        if Rake.application.options.debug
          $stderr.puts e.backtrace.join("\n")
        end
      end
    end

    def gets
      @ior.gets
    end

    def kill(sig)
      send_cmd "kill:#{sig}"
    end

    def close
      begin
        @iow.puts @close_command
        @iow.flush
      rescue Errno::EPIPE => e
        $stderr.puts "Errno::EPIPE in #{self.class}.close"
        if Rake.application.options.debug
          $stderr.puts e.backtrace.join("\n")
        end
      end
    end

  end
end
