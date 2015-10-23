module Pwrake

  class Branch

    def initialize(opts,r,w)
      #Thread.abort_on_exception = true
      @options = opts
      @task_q = {}  # worker_id => FiberQueue.new
      @timeout = @options['HEARTBEAT_TIMEOUT']
      @shells = []
      @ior = r
      @iow = w
      @runner = Runner.new
      @master_hdl = Handler.new(@runner,@ior,@iow)
      @master_hdl.set_close_block do |hdl|
        hdl.iow.close
        hdl.ior.close
      end
      @master_chan = Channel.new(@master_hdl)
      @wk_comm = {}
      @wk_hdl_set = HandlerSet.new
      @shell_start_interval = @options['SHELL_START_INTERVAL']
    end

    # Rakefile is loaded after 'init' before 'run'

    def run
      setup_shells
      setup_fibers
      setup_master_channel
      @runner.run
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
      if s.chomp != "host_list_begin"
        raise "received=#{s.chomp}, expected=host_list_begin"
      end

      if fn = @options["PROFILE"]
        if dir = @options['LOG_DIR']
          ::FileUtils.mkdir_p(dir)
          fn = File.join(dir,fn)
        end
        Shell.profiler.open(fn,@options['GNU_TIME'],@options['PLOT_PARALLELISM'])
      end

      while s = @ior.gets
        s.chomp!
        break if s == "host_list_end"
        if /^host:(\d+) (\S+) (\d+)?$/ =~ s
          id, host, ncore = $1,$2,$3
          ncore &&= ncore.to_i
          comm = WorkerCommunicator.new(id,host,ncore,@runner,@options)
          @wk_comm[id] = comm
          @wk_hdl_set << comm.handler
          @task_q[id] = FiberQueue.new
        else
          raise "received=#{s.chomp}, expected=host:id hostname ncore"
        end
      end
      @wk_comm.each_value do |comm|
        Fiber.new do
          while comm.ncore_proc(comm.channel.get_line)
          end
          Log.debug "Branch#setup_shells: end of fiber for ncore"
        end.resume
      end
      @runner.run

      # ncore
      @wk_comm.each_value do |comm|
        # set WorkerChannel#ncore at Master
        @master_hdl.put_line "ncore:#{comm.id}:#{comm.ncore}"
      end
      @master_hdl.put_line "ncore:done"

      # shells
      @shells = []
      errors = []
      shell_id = 0
      @wk_comm.each_value do |comm|
        comm.ncore.times do
          chan = Channel.new(comm.handler,shell_id)
          shell_id += 1
          shell = Shell.new(chan,@task_q[comm.id],@options.worker_option)
          @shells << shell
          # wait for remote shell open
          Fiber.new do
            if !shell.open
              errors << [comm.host,s]
            end
            Log.debug "Branch#setup_shells: end of fiber to open shell"
          end.resume
          sleep @shell_start_interval
        end
      end

      @runner.run

      if !errors.empty?
        raise RuntimeError,"Failed to start workers: #{errors.inspect}"
      end
    end

    def setup_fibers
      # start fiber
      @shells.each do |shell|
        shell.create_fiber(@master_hdl).resume
      end
      Log.debug "all fiber started"

      @wk_comm.each_value do |comm|
        #comm.start_default_fiber
        Fiber.new do
          while comm.common_line(comm.channel.get_line)
          end
          Log.debug "Branch#setup_fiber: end of fiber for default channel"
        end.resume
      end

      # setup end
      @wk_comm.values.each do |comm|
        comm.handler.put_line "setup_end"
      end

      Log.debug "branch setup end"
      @master_hdl.put_line "branch_setup:done"
    end

    def setup_master_channel
      Fiber.new do
        while s = @master_chan.get_line
          # receive command from main pwrake
          Log.debug "Branch#setup_master_channel: #{s.inspect}"
          case s
            #
          when /^(\d+):(.+)$/o
            id, tname = $1,$2
            @task_q[id].enq(tname)
            #
          when /^exit_branch$/
            Log.debug "Branch: exit_branch from master"
            @task_q.each_value{|q| q.finish}
            @wk_hdl_set.close_all
            break
            #
          when /^kill:(.*)$/o
            sig = $1
            kill(sig)
          else
            Log.debug "Branch: invalid line from master: #{s}"
          end
        end
        Log.debug "Branch#setup_master_channel: end of fiber for master handling"
      end.resume
    end

    def kill(sig="INT")
      Log.warn "Branch#kill #{sig}"
      @wk_hdl_set.kill_all(sig)
    end

    def finish
      Log.debug "Branch#finish: begin"
      #@wk_hdl_set.close_all
      @wk_hdl_set.wait_close("Branch#finish","worker_end")
      Log.debug "Branch#finish: worker communicater finish"
      @master_hdl.put_line "branch_end"
      @master_hdl.close
    end

  end # Pwrake::Branch
end
