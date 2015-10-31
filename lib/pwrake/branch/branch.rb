module Pwrake

  class Branch

    def initialize(opts,r,w)
      #Thread.abort_on_exception = true
      @option = opts
      @task_q = {}  # worker_id => FiberQueue.new
      @shells = []
      @ior = r
      @iow = w
      @runner = Runner.new(@option['HEARTBEAT'])
      @master_hdl = Handler.new(@runner,@ior,@iow)
      @master_chan = Channel.new(@master_hdl)
      @wk_comm = {}
      @wk_hdl_set = HandlerSet.new
      @shell_start_interval = @option['SHELL_START_INTERVAL']
    end

    # Rakefile is loaded after 'init' before 'run'

    def run
      setup_worker
      setup_shell
      setup_fiber
      setup_master_channel
      @runner.run
      Log.debug "Brandh#run end"
    end

    attr_reader :logger

    def init_logger
      logfile = @option['LOGFILE']
      if logfile
        if dir = @option['LOG_DIR']
          ::FileUtils.mkdir_p(dir)
          logfile = File.join(dir,logfile)
        end
        @logger = Logger.new(logfile)
      else
        @logger = Logger.new($stderr)
      end

      if @option['DEBUG']
        @logger.level = Logger::DEBUG
      elsif @option['TRACE']
        @logger.level = Logger::INFO
      else
        @logger.level = Logger::WARN
      end
    end

    def setup_worker
      s = @ior.gets
      if s.chomp != "host_list_begin"
        raise "Branch#setup_worker: received=#{s.chomp} expected=host_list_begin"
      end

      if fn = @option["PROFILE"]
        if dir = @option['LOG_DIR']
          ::FileUtils.mkdir_p(dir)
          fn = File.join(dir,fn)
        end
        Shell.profiler.open(fn,@option['GNU_TIME'],@option['PLOT_PARALLELISM'])
      end

      worker_code = WorkerCommunicator.read_worker_progs(@option)

      while s = @ior.gets
        s.chomp!
        break if s == "host_list_end"
        if /^host:(\d+) (\S+) (\d+)?$/ =~ s
          id, host, ncore = $1,$2,$3
          ncore &&= ncore.to_i
          comm = WorkerCommunicator.new(id,host,ncore,@runner,@option)
          comm.setup_connection(worker_code)
          @wk_comm[id] = comm
          @wk_hdl_set << comm.handler
          @task_q[id] = FiberQueue.new
        else
          raise "Branch#setup_worker: received=#{s.chomp} expected=host:id hostname ncore"
        end
      end
      @wk_comm.each_value do |comm|
        Fiber.new do
          while comm.ncore_proc(comm.channel.get_line)
          end
          Log.debug "Branch#setup_worker: fiber end of ncore_proc"
        end.resume
      end
      @runner.run

      # ncore
      @wk_comm.each_value do |comm|
        # set WorkerChannel#ncore at Master
        @master_hdl.put_line "ncore:#{comm.id}:#{comm.ncore}"
      end
      @master_hdl.put_line "ncore:done"
    end

    def setup_shell
      @shells = []
      errors = []
      shell_id = 0
      @wk_comm.each_value do |comm|
        comm.ncore.times do
          chan = Channel.new(comm.handler,shell_id)
          shell_id += 1
          shell = Shell.new(chan,@task_q[comm.id],@option.worker_option)
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

    def setup_fiber
      # start fibers
      @shells.each do |shell|
        shell.create_fiber(@master_hdl).resume
      end
      Log.debug "all fiber started"

      @wk_comm.each_value do |comm|
        #comm.start_default_fiber
        Fiber.new do
          while s = comm.channel.get_line
            break unless comm.common_line(s)
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
          Log.debug "Branch:recv #{s.inspect} from master"
          case s
            #
          when /^(\d+):(.+)$/o
            id, tname = $1,$2
            @task_q[id].enq(tname)
            #
          when /^exit$/
            @task_q.each_value{|q| q.finish}
            @shells.each{|shell| shell.close}
            @runner.finish
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
      @wk_hdl_set.kill(sig)
    end

    def finish
      return if @finished
      @finished = true
      Log.debug "Branch#finish: begin"
      @wk_hdl_set.exit
      Log.debug "Branch#finish: worker communicater finish"
      @master_hdl.put_line "exited"
      Log.debug "Branch#finish: sent 'exited' to master"
    end

  end # Pwrake::Branch
end
