require 'fiber'

module Pwrake
  class Channel
    @@id = "0"
    @@chan_by_id = {}
    @@chan_by_fiber = {}
    @@chan_by_io = Hash.new{|hash,key|hash[key]=[]}

    def initialize(io)
      @io = io
      @id = @@id
      @@id = @@id.succ
      @@chan_by_id[@id] = self
      @@chan_by_io[@io] << self
      @fiber = Fiber.current
      @@chan_by_fiber[@fiber] = self
      @queue = []
      @status
    end

    attr_reader :queue, :status

    def puts(arg)
      arg.split("\n").each do |x|
        @io.print "#{@id}:#{x}\n"
      end
    end

    def gets
      while @queue.empty?
        @wait_gets = true
        Fiber.yield
        @wait_gets = false
      end
      @queue.shift
    end

    def resume_gets
      if @wait_gets
        @fiber.resume
      end
    end

    def close
      #while @running
      #  pp self
      #  Fiber.yield
      #end
      # $stderr.print "--- close @id=#{@id}\n"
      # $stderr.flush
      @@chan_by_fiber.delete(@fiber)
      @@chan_by_io[@io].delete(self)
      if @@chan_by_io[@io].empty?
        @io.print "exit:\n"
        # @io.close
      end
      # @@chan_by_id.delete(@id)
    end

    def system(*cmd)
      @running = true
      self.puts(cmd.join(' '))
      while s = self.gets
        break if s == :end
        Util.puts s
      end
      res = 0
      @status = Rake::PseudoStatus.new(0)
      @running = false
      res
    end

    class << self
      def enq(id,item)
        chan = get_channel_by_id(id)
        chan.queue.push(item)
        chan.resume_gets
      end

      def check_line(line)
        case line
        when /^(\d+):(.*)$/
          id,item = $1,$2
          Pwrake::Channel.enq(id,item)
          return true
        when /^end:(\d+)$/
          id = $1
          Pwrake::Channel.enq(id,:end)
          return true
        else
          return false
        end
      end

      def get_channel_by_id(id)
        chan = @@chan_by_id[id]
        if chan.nil?
          pp @@chan_by_id
          raise "no channel w/ id=#{id.inspect} "
        end
        return chan
      end

      def current
        @@chan_by_fiber[Fiber.current]
      end
    end
  end
end
