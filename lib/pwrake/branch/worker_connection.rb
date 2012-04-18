module Pwrake

  class WorkerConnection

    @@connections = []
    @@wk_id = "0"

    def initialize(id,host,ncore)          # parent
      @id = id
      @host = host
      @ncore = ncore
      prog = "../lib/pwrake/worker/worker.rb"
      cmd = "ssh -x -T -q #{@host} 'cd #{Dir.pwd};"+
        "exec ruby #{prog} #{@id} #{@ncore}'"

      @ior,w1 = IO.pipe
      r2,@iow = IO.pipe
      pid = spawn(cmd,:pgroup=>true,:out=>w1,:in=>r2)
      w1.close
      r2.close

      @@connections.push(self)
    end

    attr_reader :ior, :iow, :host
    attr_accessor :ncore

    def send_cmd(cmd)
      @iow.print cmd.to_str+"\n"
      @iow.flush
    end

    def close
      @iow.puts "exit:"
      @iow.close
      @@connections.delete(self)
      Util.puts "exited #{@id}"
    end

    def kill(sig)
      @iow.puts "kill:#{sig}"
      @iow.close
    end

    class << self
      def kill(sig)
        Util.puts "wkcn:signal trapped:#{sig}"
        # open("/tmp/sig-#{ENV['USER']}-#{Process.pid}","w"){|f| f.puts "signal trapped:"}
        @@connections.each{|conn| conn.kill(sig)}
        Kernel.exit
      end
    end

    [:TERM,:INT,:KILL].each do |sig|
      Signal.trap(sig) do
        self.kill(sig)
        exit
      end
    end

  end # Connection

end
