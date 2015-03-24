module Pwrake

  class BranchCommunicator

    @@communicators = []
    @@killed = false

    def initialize(host,opts,*args)
      @ior,w0 = IO.pipe
      @ioe,w1 = IO.pipe
      r2,@iow = IO.pipe
      if host != "localhost" || /^(n|f)/i =~ ENV['T']
        dir = File.absolute_path(File.dirname($PROGRAM_NAME))
        args = Shellwords.shelljoin(ARGV)
        cmd = "ssh -x -T -q #{host} '" +
          "cd \"#{Dir.pwd}\";"+
          "PATH=#{dir}:${PATH} exec pwrake_branch'"
        Log.debug("BranchCommunicator cmd=#{cmd}")
        @pid = spawn(cmd,:pgroup=>true,:out=>w0,:err=>w1,:in=>r2)
        w0.close
        w1.close
        r2.close
        #sleep 0.01
        wait_branch
        Marshal.dump(opts,@iow)
      else
        @thread = Thread.new(r2,w0,opts) do |r,w,o|
          Rake.application.run_branch_in_thread(r,w,o)
        end
      end
      @@communicators << self
    end

    attr_reader :ior, :ioe, :iow, :host

    def wait_branch
      s = @ior.gets
      if !s or s.chomp != "pwrake_branch start"
        p s
        raise "pwrake_branch start failed: conn=#{self.inspect}"
      end
    end

    def send_cmd(cmd)
      print cmd.to_str+"\n"
      flush
    end

    def print(x)
      #puts "<"+x
      @iow.print x
    end

    def flush
      @iow.flush
    end

    def gets
      @ior.gets
    end

    def kill(sig)
      $stderr.puts "#{self.class.to_s}#kill sig=#{sig} pid=#{Process.pid} thread=#{Thread.current} self=#{self.inspect}"
      #$stderr.puts "#{self.class.to_s}#kill:#{sig}"
      send_cmd "kill:#{sig}"
      @iow.flush
      Process.kill(sig,@pid) if @pid
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

      def close_all
        @@communicators.each{|comm| comm.close}
      end
    end

    if true
      [:TERM,:INT].each do |sig|
        Signal.trap(sig) do
          $stderr.puts "\nSignal trapped. (sig=#{sig} pid=#{Process.pid} thread=#{Thread.current})"
          self.kill(sig)
          Kernel.exit
        end
      end
    end

  end
end
