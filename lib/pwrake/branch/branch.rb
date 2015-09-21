module Pwrake

  class Branch

    def initialize(opts,r,w)
      @options = opts
      @task_q = {}  # worker_id => FiberQueue.new
      @timeout = @options['HEARTBEAT_TIMEOUT']
      @exit_cmd = "exit_connection"
      @shells = []
      @ior = r
      @iow = w
      @wk_comm = {}
      @shell_start_interval = @options['SHELL_START_INTERVAL']
    end

    # Rakefile is loaded after 'init' before 'run'

    def run
      @dispatcher = IODispatcher.new
      @comm_set = CommunicatorSet.new
      setup_shells
      setup_fibers
      bh = MasterHandler.new(@task_q,@iow,@comm_set)
      @dispatcher.attach(@ior,bh)
      @dispatcher.event_loop(@timeout)
    end

    attr_reader :logger

    def init_logger
      logfile = @options['LOGFILE']
      if logfile
        if dir = @options['LOG_DIR']
          ::FileUtils.mkdir_p(dir)
          logfile = File.join(dir,logfile)
        end
        @logger = Logger.new(logfile)
      else
        @logger = Logger.new($stderr)
      end

      if @options['DEBUG']
        @logger.level = Logger::DEBUG
      elsif @options['TRACE']
        @logger.level = Logger::INFO
      else
        @logger.level = Logger::WARN
      end
    end

    def setup_shells
      s = @ior.gets
      raise if s.chomp != "begin_worker_list"

      if fn = @options["PROFILE"]
        if dir = @options['LOG_DIR']
          ::FileUtils.mkdir_p(dir)
          fn = File.join(dir,fn)
        end
        Shell.profiler.open(fn,@options['GNU_TIME'],@options['PLOT_PARALLELISM'])
      end

      while s = @ior.gets
        s.chomp!
        break if s == "end_worker_list"
        if /^(\d+):(\S+) (\d+)?$/ =~ s
          id, host, ncore = $1,$2,$3
          ncore &&= ncore.to_i
          comm = WorkerCommunicator.new(id,host,ncore,@dispatcher,@options)
          @wk_comm[comm.ior] = comm
          @comm_set << comm
          @dispatcher.attach_hb(comm.ior,comm)
          @task_q[id] = FiberQueue.new
        end
      end

      # receive ncore from worker node
      io_list = @wk_comm.keys
      io_fail = []
      IODispatcher.event_once(io_list,@timeout) do |io|
        if io.eof?
          io_fail << io
        else
          while s = io.gets
            case s
            when /^ncore:(\d+)$/
              @wk_comm[io].set_ncore($1.to_i)
              Log.debug "#{s.chomp} @#{@wk_comm[io].host}"
              break
              #
            when /^log:(.*)$/
              Log.debug "worker(#{@wk_comm[io].host})>#{$1}"
              #
            else
              Log.debug "fail to receive #{s.chomp} @#{@wk_comm[io].host}"
            end
          end
        end
      end
      if !(io_list.empty? && io_fail.empty?)
        t = io_list.map{|io| @wk_comm[io].host}.join(',')
        f = io_fail.map{|io| @wk_comm[io].host}.join(',')
        raise RuntimeError, "error in connection to worker: timeout:[#{t}],fail:[#{f}]"
      end

      # ncore
      @wk_comm.each_value do |comm|
        # set WorkerChannel#ncore at Master
        @iow.puts "ncore:#{comm.id}:#{comm.ncore}"
        @iow.flush
      end
      @iow.puts "ncore:done"
      @iow.flush

      # shells
      @shells = []
      @wk_comm.each_value do |comm|
        comm.ncore.times do
          @shells << Shell.new(comm,@task_q[comm.id],@options.worker_option)
        end
      end
    end

    def setup_fibers
      @fiber_list = @shells.map{|shell| shell.create_fiber(@iow)}

      # start fiber
      @fiber_list.each do |fb|
        fb.resume
        sleep @shell_start_interval
      end
      Log.debug "all fiber started"

      waiters = {}
      errors = []
      @shells.each{|shl| waiters[shl.id]=true}

      # receive open notice from worker
      @dispatcher.event_loop_block do |io|
        if wk = @wk_comm[io]
          m = "id=#{wk.id} host=#{wk.host}"
        else
          m = ""
        end
        s = io.gets
        case s
        when /^open:(\d+)$/
          waiters.delete($1)
          #
        when /^worker_end$/
          m = "worker_end #{m}"
          Log.warn m
          @dispatcher.detach(io)
          @wk_comm.delete(io)
          #
        when /^heartbeat$/
          @dispatcher.heartbeat(io)
          #
        when /^log:(.*)$/
          Log.info "worker(#{@wk_comm[io].host})>#{$1}"
          #
        when /^exc:(\d+):(.*)$/
          id,msg = $1,$2
          m = "worker(#{wk.host},id=#{id}) err>#{msg}"
          Log.fatal m
          errors << m
          waiters.delete(id)
          #
        else
          m = "unexpected return from worker #{m}:`#{s.chomp}'"
          Log.fatal m
          errors << m
          waiters.clear
        end
        waiters.empty?
      end

      if !errors.empty?
        raise "Failed to start workers"
      end

      # setup end
      @wk_comm.values.each do |comm|
        comm.send_cmd "setup_end"
      end

      Log.debug "branch setup end"
      @iow.puts "branch_setup:done"
      @iow.flush
    end


    def handle_failed_target(name)
      case @options['FAILED_TARGET']
      when /rename/i, NilClass
        dst = name+"._fail_"
        ::FileUtils.mv(name,dst)
        msg = "Rename failed target file '#{name}' to '#{dst}'"
        $stderr.puts(msg)
        Log.warn(msg)
      when /delete/i
        ::FileUtils.rm(name)
        msg = "Delete failed target file '#{name}'"
        $stderr.puts(msg)
        Log.warn(msg)
      when /leave/i
      end
    end

    def kill(sig="INT")
      Log.info "#{self.class}#kill #{sig}"
      @comm_set.kill(sig)
    end

    def finish
      Log.debug "#{self.class}#finish"
      @comm_set.close_all
      @comm_set.each do |comm|
        while s=comm.gets
          Log.debug "comm.id=#{comm.id}> #{s}"
        end
      end
      @dispatcher.finish

      @iow.puts "branch_end"
      @iow.flush
      @ior.close
      @iow.close
    end

  end # Pwrake::Branch
end
