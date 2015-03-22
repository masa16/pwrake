module Pwrake

  class BranchCommunicator

    @@communicators = []
    @@killed = false

    def initialize(host,opts,*args)
      @ior,w1 = IO.pipe
      r2,@iow = IO.pipe
      if host != "localhost" || /^(n|f)/i =~ ENV['T']
        dir = File.absolute_path(File.dirname($PROGRAM_NAME))
        args = Shellwords.shelljoin(ARGV)
        cmd = "ssh -x -T -q #{host} '" +
          "cd \"#{Dir.pwd}\";"+
          "PATH=#{dir}:${PATH} exec pwrake_branch #{args}'"
        @pid = spawn(cmd,:pgroup=>true,:out=>w1,:err=>$stderr,:in=>r2)
        w1.close
        r2.close
        sleep 0.01
      else
        @thread = Thread.new(r2,w1) do |r,w|
          Rake.application.run_branch(r,w)
        end
      end
      @@communicators << self
      start_branch(opts)
    end

    attr_reader :ior, :iow, :host

    def start_branch(opts)
      s = @ior.gets
      if !s or s.chomp != "pwrake_branch started"
        p s
        raise "pwrake_branch start failed: conn=#{self.inspect}"
      end
      Marshal.dump(opts,@iow)
      send_cmd "begin_worker_list"
    end

    def send_cmd(cmd)
      print cmd.to_str+"\n"
      flush
    end

    def print(x)
      puts "<"+x
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
