module Pwrake

  class Communicator

    @@communicators = []
    @@killed = 0
    attr_reader :ior, :ioe, :iow, :host

    def initialize(host,opts={})
      @host = host
      @opts = opts
      @ior,w0 = IO.pipe
      @ioe,w1 = IO.pipe
      r2,@iow = IO.pipe
      setup_connection(w0,w1,r2)
      @@communicators << self
      @close_command = "change_me"
    end

    def setup_connection(w0,w1,r2)
    end

    def send_cmd(cmd)
      print cmd.to_str+"\n"
      flush
    end

    def print(x)
      #puts "<"+x
      @iow.print x
    end

    def puts(x)
      #puts "<"+x
      @iow.print x+"\n"
    end

    def flush
      @iow.flush
    end

    def gets
      @ior.gets
    end

    def kill(sig)
      send_cmd "kill:#{sig}"
    end

    def close
      if !@iow.closed?
        @iow.puts @close_command if @@killed==0
        @iow.flush
      end
      @@communicators.delete(self)
    end

    def closed?
      @iow.closed? #and @ior.closed?
    end

    class << self
      def kill(sig)
        @@communicators.each{|comm| comm.kill(sig)}
      end

      def close_all
        @@communicators.each{|comm| comm.close}
      end
    end

    [:TERM,:INT].each do |sig|
      Signal.trap(sig) do
        # log writing failed. can't be called from trap context
        $stderr.puts "\nSignal trapped. (sig=#{sig} pid=#{Process.pid} thread=#{Thread.current}) @@killed=#{@@killed}"
        if Rake.application.options.debug
          $stderr.puts caller
        end
        case @@killed
        when 0
          $stderr.puts "Exiting. It may take a time..."
          self.kill(sig)
          $stderr.puts "Kill request is sent."
          self.close_all
          $stderr.puts "Close request is sent."
        when 1
          $stderr.puts "Once more Ctrl-C (SIGINT) for exit"
        else
          Kernel.exit # must wait for nomral exit
        end
        @@killed += 1
      end
    end

  end
end
