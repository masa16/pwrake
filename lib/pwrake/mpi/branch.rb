begin
  require 'rake'
rescue LoadError
  require 'rubygems'
  require 'rake'
end

require "pwrake/version"
require "pwrake/master/master_application"
require "pwrake/task/task_manager"
require "pwrake/task/task_algorithm"
require "pwrake/branch/branch_application"

class Rake::Application
  include Pwrake::BranchApplication
  prepend Pwrake::MasterApplication
  prepend Pwrake::TaskManager
end

class Rake::Task
  include Pwrake::TaskAlgorithm
end

module Pwrake

  class CommunicatorSet
    def init_hosts
      @ipaddr_to_rank = {}
      (1..MPipe::Comm.size-1).each do |rank|
        io  = MPipe.new(rank)
        sz, = io.read(4).unpack("V")
        s   = io.read(sz)
        v   = s.split('|')
        v.each{|a| @ipaddr_to_rank[a] = rank}
      end
      Log.debug "@ipaddr_to_rank="+@ipaddr_to_rank.inspect
    end
    attr_reader :ipaddr_to_rank
  end

  class Communicator
    def setup_pipe(worker_code)
      ipa = IPSocket.getaddress(@host)
      if %w[127.0.0.1 ::1].include?(ipa)
        ipa = IPSocket.getaddress(Socket.gethostname)
      end
      @rank = @set.ipaddr_to_rank[ipa]
      if @rank.nil?
        raise RuntimeError,"no rank for #{@host}"
      end
      mp = MPipe.new(@rank)
      @ior = mp
      @ioe,w1 = IO.pipe
      @iow = mp
      @pid = nil
      w1.close
    end
  end

end

Pwrake::Branch.io_class = MPipe

# does NOT exit when writing to broken pipe
Signal.trap(:PIPE, "IGNORE")

Rake.application.run
