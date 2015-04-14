module Pwrake

  class BranchCommunicator < Communicator

    def initialize(host,opts={})
      super
      @close_command = "exit_branch"
    end

    def setup_connection(w0,w1,r2)
      if @host != "localhost" || /^(n|f)/i =~ ENV['T']
        dir = File.absolute_path(File.dirname($PROGRAM_NAME))
        #args = Shellwords.shelljoin(@args)
        cmd = "ssh -x -T -q #{@host} '" +
          "cd \"#{Dir.pwd}\";"+
          "PATH=#{dir}:${PATH} exec pwrake_branch'"
        #Log.debug("BranchCommunicator cmd=#{cmd}")
        Log.puts "BranchCommunicator: #{cmd}"
        @pid = spawn(cmd,:pgroup=>true,:out=>w0,:err=>w1,:in=>r2)
        w0.close
        w1.close
        r2.close
        #sleep 0.01
        Marshal.dump(@opts,@iow)
        wait_branch
      else
        @thread = Thread.new(r2,w0,@opts) do |r,w,o|
          Rake.application.run_branch_in_thread(r,w,o)
        end
      end
    end

    def wait_branch
      while s = @ior.gets
        break if s.chomp == "pwrake_branch start"
        Kernel.puts s
      end
      #s = @ior.gets
      #if !s or s.chomp != "pwrake_branch start"
      #  raise RuntimeError,"pwrake_branch start failed: "+
      #    "conn=#{self.inspect} gets=#{s.inspect}"
      #end
    end

  end
end
