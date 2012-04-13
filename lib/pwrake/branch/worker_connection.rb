module Pwrake

  class WorkerConnection

    @@wk_id = "0"

    def initialize(id,host,ncore)          # parent
      @id = id
      @host = host
      @ncore = ncore
      prog = "../../pwrake/lib/pwrake/worker/worker.rb"
      cmd = "ssh -x -T -q #{@host} 'cd #{Dir.pwd};"+
        "exec ruby #{prog} #{@id} #{@ncore}'"
      # $stderr.puts "cmd=#{cmd}"
      @io = IO.popen(cmd, "r+")
    end

    attr_reader :io, :host
    attr_accessor :ncore

    def close
      @io.close
    end
  end # Connection

end
