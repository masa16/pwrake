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
      # $stderr.puts "cmd=#{cmd}"
      @io = IO.popen(cmd, "r+")
      @@connections.push(self)
    end

    attr_reader :io, :host
    attr_accessor :ncore

    def send(cmd)
      @io.print cmd.to_str+"\n"
      @io.flush
    end

    def close
      @io.puts "exit:"
      @io.close
      @@connections.delete(self)
      Util.puts "exited #{@id}"
    end

  end # Connection

end
