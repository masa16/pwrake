module Pwrake

  class BranchConnection

    def initialize(host,workers)
      @host = host
      @worker_names = workers
      cmd = "ssh -x -T -q #{host} 'cd #{Dir.pwd}; exec ./pwrake_branch -t'"
      puts cmd
      @io = IO.popen(cmd, "r+")
    end

    attr_reader :io, :worker_set, :host

    def close
      @io.puts "exit_branch"
      @io.close
    end
  end

end
