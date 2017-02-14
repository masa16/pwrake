require "parallel/processor_count.rb"
require "pwrake/nbio"
require "pwrake/branch/fiber_queue"
require "pwrake/worker/writer"
require "pwrake/worker/log_executor"
require "pwrake/worker/executor"
require "pwrake/worker/invoker"
require "pwrake/worker/shared_directory"
require "pwrake/worker/gfarm_directory"

require "thread"
require "fileutils"
require "timeout"
require "socket"

module Pwrake
  class Invoker
    def get_io
      # get IP addresses
      v = Socket.getifaddrs
      v = v.select{|a| a.addr.ip? && (a.flags & Socket::IFF_MULTICAST != 0)}
      v = v.map{|a| a.addr.ip_address}
      s = v.join('|')
      # write IP addresses
      iow = MPipe.new(0)
      iow.write([s.size].pack("V"))
      iow.write(s)
      iow.flush
      # returns IO, $stdin, $stdout
      [MPipe, MPipe.new(0), MPipe.new(0)]
    end
  end
end

require "pwrake/worker/worker_main"
