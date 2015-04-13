module Pwrake

  class Communicator

    @@communicators = []
    @@killed = false
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
      Log.warn "#{self.class.to_s}#kill sig=#{sig} pid=#{Process.pid} thread=#{Thread.current}"
      send_cmd "kill:#{sig}"
      Process.kill(sig,@pid) if @pid
    end

    def close
      if !@iow.closed?
        @iow.puts @close_command if !@@killed
        #@iow.close
        @iow.flush
      end
      @@communicators.delete(self)
    end

    def closed?
      @iow.closed? #and @ior.closed?
    end

    class << self
      def kill(sig)
        @@killed = true
        @@communicators.each{|comm| comm.kill(sig)}
      end

      def close_all
        @@communicators.each{|comm| comm.close}
      end
    end

    if true
      [:TERM,:INT].each do |sig|
        Signal.trap(sig) do
          $stderr.puts "\nSignal trapped. (sig=#{sig} pid=#{Process.pid} thread=#{Thread.current})"
          $stderr.puts caller
          self.kill(sig)
          Kernel.exit
        end
      end
    end

  end
end
