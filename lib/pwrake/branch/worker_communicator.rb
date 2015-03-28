module Pwrake

  class WorkerCommunicator < Communicator

    @@worker_command = "ruby "+File.expand_path(File.dirname(__FILE__))+
      "/../../../bin/pwrake_worker"
    RE_ID='\d+'
    attr_reader :id, :host, :ncore

    def initialize(id,host,ncore,opt={})
      @id = id
      @ncore = @n_total_core = ncore
      @channel = {}
      #
      #$stderr.puts "opt=#{opt.inspect}"
      @option = opt
      @work_dir = @option[:work_dir] || Dir.pwd
      @pass_env = @option[:pass_env]
      @ssh_opt  = @option[:ssh_opt]
      @filesystem = @option[:filesystem]
      super(host)
    end

    def setup_connection(w0,w1,r2)
      @pid = spawn(system_cmd,:pgroup=>true,:out=>w0,:err=>w1,:in=>r2)
      w0.close
      w1.close
      r2.close
      if @path
        @iow.puts "export:PATH='#{path}'"
      end
      if @pass_env
        @pass_env.each do |k,v|
          @iow.puts "export:#{k}='#{v}'"
          #$stderr.puts "export:#{k}='#{v}'"
        end
      end
      if @filesystem
        #$stderr.puts "fs:#{@filesystem}"
        @iow.puts "fs:#{@filesystem}"
      end
      if @work_dir
        @iow.puts "wd:#{@work_dir}"
      end
      #$stderr.puts "setup_connection"
    end

    def system_cmd
      #if @work_dir
      #  cmd = "cd #{@work_dir}; #{@@worker_command}"
      #else
      #  cmd = @@worker_command
      #end
      if ['localhost','localhost.localdomain','127.0.0.1'].include? @host
        "cd;"+@@worker_command
      else
        "ssh -x -T -q #{@ssh_opt} #{@host} '#{@@worker_command}'"
      end
    end

    def set_ncore(ncore)
      @ncore = ncore if @ncore.nil?
    end

    def add_channel(id,channel)
      @channel[id] = channel
    end

    #def resume
    #  @channel.each{|k,ch| ch.resume}
    #end

    def on_read(io)
      s = io.gets
      # $chk.print ">#{s}" if $dbg
      # $stderr.puts s
      case s
      when /^(#{RE_ID}):(.*)$/
        id,item = $1,$2
        @channel[id].enq([:out,item])
        #
      when /^(#{RE_ID})e:(.*)$/
        id,item = $1,$2
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
        @n_total_core = $1
        #@channel[id].enq([:ncore,ncore])
        #
      when /^exit$/
        $stderr.puts "exit"
        return true
      else
        puts "Invalild item: #{s}"
      end
      #resume
      return false
    end

  end
end
