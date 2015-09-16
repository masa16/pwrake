module Pwrake

  class WorkerCommunicator < Communicator

    RE_ID='\d+'
    attr_reader :id, :host, :ncore
    attr_reader :channel

    @@worker_communicators = []

    def initialize(id,host,ncore,dispatcher,option)
      @id = id
      @ncore = @n_total_core = ncore
      @channel = {}
      #
      @dispatcher = dispatcher
      @worker_progs = option.worker_progs
      @option = option.worker_option
      @heartbeat_timeout = @option[:heartbeat_timeout]
      super(host)
      @close_command = "exit_worker"
      @@worker_communicators << self
    end

    def setup_connection(w0,w1,r2)
      d = File.dirname(__FILE__)+'/../worker/'
      worker_code = ""
      @worker_progs.each do |f|
        worker_code << IO.read(d+f+'.rb')
      end
      rb_cmd = "ruby -e 'eval ARGF.read(#{worker_code.size})'"
      if ['localhost','localhost.localdomain','127.0.0.1'].include? @host
        cmd = "cd; #{rb_cmd}"
      else
        cmd = "ssh -x -T -q #{@option[:ssh_option]} #{@host} \"#{rb_cmd}\""
      end
      Log.debug cmd
      @pid = spawn(cmd,:pgroup=>true,:out=>w0,:err=>w1,:in=>r2)
      w0.close
      w1.close
      r2.close
      @iow.write worker_code
      Marshal.dump(@ncore,@iow)
      Marshal.dump(@option,@iow)
      @heartbeat = Time.now
    end

    def close
      super
    end

    def set_ncore(ncore)
      @ncore = ncore if @ncore.nil?
    end

    def add_channel(id,channel)
      @channel[id] = channel
    end

    def delete_channel(id)
      @channel.delete(id)
    end

    def channel_empty?
      @channel.empty?
    end

    def on_read(io)   # return to Shell#io_read_loop
      s = io.gets
      # $chk.print ">#{s}" if $dbg
      # $stderr.puts ">"+s
      case s
      when /^(#{RE_ID}):(.*)$/
        id,item = $1,$2
        @channel[id].enq([:out,item])
        #
      when /^(#{RE_ID})e:(.*)$/
        id,item = $1,$2
        @channel[id].enq([:err,item])
        #
      when /^start:(#{RE_ID})$/
        id = $1
        @channel[id].enq([:start])
        #
      when /^end:(#{RE_ID})(?::(.*))?$/
        id,status = $1,$2
        @channel[id].enq([:end,status])
        #
      when /^err:(#{RE_ID}):(.*)$/
        id,stat_val,stat_cond = $1,$2,$3
        @channel[id].enq([:end,stat_val,stat_cond])
        #
      when /^open:(#{RE_ID})$/
        id = $1
        @channel[id].enq([:open])
        #
      when /^heartbeat$/
        @dispatcher.heartbeat(io)
        #
      when /^ncore:(\d+)$/
        @n_total_core = $1
        #
      when /^worker_end$/
        Log.debug "#{self.class}#on_read: #{s.chomp} id=#{@id} host=#{@host}"
        @@worker_communicators.delete(self)
        return @@worker_communicators.empty?
        #
      when /^exc:(#{RE_ID}):(.*)$/
        id,msg = $1,$2
        Log.error "worker(#{host},id=#{id}) err>#{msg}"
        return true
        #
      when /^log:(.*)$/
        Log.info "worker(#{host})>#{$1}"
        #
      else
        Log.warn "worker(#{host}) out>#{s.chomp}"
      end
      return false
    end

  end
end
