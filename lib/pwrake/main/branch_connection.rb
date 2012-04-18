module Pwrake

  class BranchConnection

    @@connections = []

    def initialize(host,workers)
      @host = host
      @worker_names = workers
      cmd = "ssh -x -T -q #{host} 'cd #{Dir.pwd}; exec ./pwrake_branch -t'"
      # puts cmd

      @ior,w1 = IO.pipe
      r2,@iow = IO.pipe
      pid = spawn(cmd,:pgroup=>true,:out=>w1,:in=>r2)
      w1.close
      r2.close

      @@connections << self
    end

    attr_reader :ior, :iow, :worker_set, :host

    def send_cmd(cmd)
      @iow.print cmd.to_str+"\n"
      @iow.flush
    end

    def close
      # @io.puts "exit_branch"
      @iow.close if !@iow.closed?
      @@connections.delete(self)
    end

    def kill(sig)
      #Util.dputs "send_cmd kill:#{sig}"
      send_cmd "kill:#{sig}"
    end

    def self.kill(sig)
      #Util.dputs "brcn:signal trapped:#{sig}"
      @@connections.each{|conn| conn.kill(sig)}
    end

    [:TERM,:INT,:KILL].each do |sig|
      Signal.trap(sig) do
        self.kill(sig)
        Kernel.exit
      end
    end
  end
end
