require "forwardable"

module Pwrake class CommunicatorSet
  extend Forwardable

  def initialize(master_rd,selector,option)
    @master_rd = master_rd
    @selector = selector
    @option = option
    @communicators = {}
    @initial_communicators = []
  end

  attr_reader :selector

  def_delegators :@communicators, :each, :each_value, :values, :size

  def create_communicators
    Fiber.new do
      s = @master_rd.get_line
      if s.chomp != "host_list_begin"
        raise "Branch#setup_worker: recv=#{s.chomp} expected=host_list_begin"
      end

      while s = @master_rd.get_line
        s.chomp!
        break if s == "host_list_end"
        if /^host:(\d+) (\S+) ([+-]?\d+)?$/ =~ s
          id, host, ncore = $1,$2,$3
          ncore &&= ncore.to_i
          @communicators[id] = Communicator.new(self,id,host,ncore,@selector,@option)
        else
          raise "Branch#setup_worker: recv=#{s.chomp} expected=host:id hostname ncore"
        end
      end
    end.resume
    @selector.run
    @initial_communicators = @communicators.dup
  end

  def add(comm)
    @communicators[comm.id] = comm
  end

  def delete(comm)
    @communicators.delete(comm.id)
    @error_host << comm.host
  end

  def run(message)
    @error_host = []
    n1 = @communicators.size
    @selector.run
    n2 = @communicators.size
    if n1 != n2
      Log.error "#{message.inspect} failed. hosts=[#{@error_host.join(',')}]"
      Log.error "# of communicators is varied from #{n1} to #{n2}."
    end
    if @communicators.empty?
      raise RuntimeError,"#{message.inspect} failed. hosts=[#{@error_host.join(',')}]"
    end
  end

end end
