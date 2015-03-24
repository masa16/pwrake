module Pwrake

  class MultiplexHandler

    RE_ID='\d+'

    def initialize
      @channel = {}
    end

    def add_channel(id,channel)
      @channel[id] = channel
    end

    def resume
      @channel.each{|k,ch| ch.resume}
    end

    def on_read(io)
      s = io.gets
      # $chk.print ">#{s}" if $dbg
      case s
      when /^(#{RE_ID}):(.*)$/
        id,item = $1,$2
        #@channel[id].enq(item)
        @channel[id].enq([:out,item])
        #
      when /^(#{RE_ID})e:(.*)$/
        id,item = $1,$2
        #@channel[id].enq_err(item)
        @channel[id].enq([:err,item])
        #
      when /^end:(#{RE_ID})(?::(\d+):([^,]*),(.*))?$/
        id,pid,stat_val,stat_cond = $1,$2,$3,$4
        @channel[id].enq([:end,pid,stat_val,stat_cond])
        #
      when /^start:(#{RE_ID}):(\d*)$/
        id,pid = $1,$2
        @channel[id].enq([:start,pid])
        #
      when /^ncore:(\d+)$/
        ncore = $1
        #@channel[id].enq([:ncore,ncore])
        #
      when /^exit$/
        $stderr.puts "exit"
        return true
      else
        puts "Invalild item: #{s}"
      end
      resume
      return false
    end

  end
end

