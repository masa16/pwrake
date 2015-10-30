module Pwrake

  class WorkerCommunicator

    RE_ID='\d+'
    attr_reader :id, :host, :ncore, :handler, :channel

    def self.read_worker_progs(option)
      d = File.dirname(__FILE__)+'/../worker/'
      code = ""
      option.worker_progs.each do |f|
        code << IO.read(d+f+'.rb')
      end
      code
    end

    def initialize(id,host,ncore,runner,option)
      @id = id
      @ncore = @n_total_core = ncore
      #
      @runner = runner
      @worker_progs = option.worker_progs
      @option = option.worker_option
      if hb = @option[:heartbeat]
        @heartbeat_timeout = hb + 15
      end
      @host = host
    end

    def setup_connection(worker_code)
      rb_cmd = "ruby -e 'eval ARGF.read(#{worker_code.size})'"
      if ['localhost','localhost.localdomain','127.0.0.1'].include? @host
        cmd = "cd; #{rb_cmd}"
      else
        cmd = "ssh -x -T -q #{@option[:ssh_option]} #{@host} \"#{rb_cmd}\""
      end
      Log.debug cmd
      @handler = Handler.new(@runner) do |w0,w1,r2|
        @pid = spawn(cmd,:pgroup=>true,:out=>w0,:err=>w1,:in=>r2)
        w0.close
        w1.close
        r2.close
      end
      @handler.host = @host
      iow = @handler.iow
      iow.write(worker_code)
      Marshal.dump(@ncore,iow)
      Marshal.dump(@option,iow)
      @channel = Channel.new(@handler)
    end

    def close
      if !@closed
        @closed = true
        @handler.close
      end
    end

    def set_ncore(ncore)
      @ncore = ncore if @ncore.nil?
    end

    def ncore_proc(s)
      if /^ncore:(\d+)$/ =~ s
        set_ncore($1.to_i)
        Log.debug "#{s.chomp} @#{@host}"
        return false
      else
        return common_line(s)
      end
    end

    def common_line(s)
      Log.debug "WorkerCommunicator#common_line: #{s.chomp} id=#{@id} host=#{@host}"
      case s
      when /^heartbeat$/
        Log.debug "Branch: heartbeat"
        @runner.heartbeat(@handler.ior)
      when /^exited$/
        Log.debug "Branch: receive exited"
        return false
      when /^log:(.*)$/
        Log.info "worker(#{host})>#{$1}"
      else
        Log.warn "worker(#{host}) out>#{s.chomp}"
      end
      true
    end

    def start_default_fiber
      Fiber.new do
        while common_line(@channel.get_line)
        end
        Log.debug "#{self.class}#start_default_fiber: end of fiber"
      end.resume
    end

  end
end
