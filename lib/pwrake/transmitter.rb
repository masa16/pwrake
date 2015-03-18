module Pwrake

  class Transmitter

    @@transmitters = []
    @@killed = false

    def initialize(host,cmd,ncore=nil)
      @ncore = ncore
      @host = host
      @ior,w1 = IO.pipe
      r2,@iow = IO.pipe
      pid = spawn(cmd,:pgroup=>true,:out=>w1,:err=>$stderr,:in=>r2)
      w1.close
      r2.close
      @@transmitters.push(self)
      sleep 0.01
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
      @iow.puts "kill:#{sig}"
      @iow.flush
    end

    def close
      @iow.puts "exit_connection" if !@@killed
      @iow.close
      @@transmitters.delete(self)
    end

    class << self
      def kill(sig)
        @@killed = true
        @@transmitters.each{|trs| trs.kill(sig)}
      end
    end

    [:TERM,:INT].each do |sig|
      Signal.trap(sig) do
        self.kill(sig)
      end
    end

  end # Transmitter

end
