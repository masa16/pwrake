module Pwrake

  class Communicator

    @@communicators = []
    @@killed = false

    def initialize(host,cmd=nil,ncore=nil)
      @ncore = ncore
      @host = host
      @ior,w1 = IO.pipe
      r2,@iow = IO.pipe
      if block_given?
        @thread = Thread.new(r2,w1){|r,w| yield(r,w); puts "end yield" }
      else
        @pid = spawn(cmd,:pgroup=>true,:out=>w1,:err=>$stderr,:in=>r2)
        w1.close
        r2.close
        sleep 0.01
      end
      @@communicators.push(self)
    end

    attr_reader :ior, :iow, :host
    attr_accessor :ncore

    def send_cmd(cmd)
      @iow.print cmd.to_str+"\n"
      @iow.flush
    end

    def print(s)
      Fiber.yield if @@killed
      @iow.print s
    end

    def puts(s)
      Fiber.yield if @@killed
      @iow.puts s
    end

    def flush
      Fiber.yield if @@killed
      @iow.flush
    end

    def kill(sig)
      $stderr.puts "#{self.class.to_s}#kill sig=#{sig} pid=#{Process.pid} thread=#{Thread.current} self=#{self.inspect}"
      #$stderr.puts "#{self.class.to_s}#kill:#{sig}"
      send_cmd "kill:#{sig}"
      #@iow.flush
    end

    def close
      @iow.puts "exit_connection" if !@@killed
      @iow.close
      @@communicators.delete(self)
    end

    class << self
      def kill(sig)
        @@killed = true
        @@communicators.each{|comm| comm.kill(sig)}
      end
    end

    if true
      [:TERM,:INT].each do |sig|
        Signal.trap(sig) do
          $stderr.puts "\nSignal trapped. (sig=#{sig} pid=#{Process.pid} thread=#{Thread.current})"
          self.kill(sig)
          Process.kill(sig,@pid) if @pid
          Kernel.exit
        end
      end
    end

  end # Communicator

end
