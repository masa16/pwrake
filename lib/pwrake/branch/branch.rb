module Pwrake

  class Branch

    def initialize(opts,r,w)
      #Thread.abort_on_exception = true
      @option = opts
      @task_q = {}  # worker_id => FiberQueue.new
      @shells = []
      @ior = r
      @iow = w
      @selector = AIO::Selector.new(@option['HEARTBEAT'])
      @master_rd = AIO::Reader.new(@selector,@ior)
      @master_wt = AIO::Writer.new(@selector,@iow)
      #@wk_hdl_set = HandlerSet.new
      @shell_start_interval = @option['SHELL_START_INTERVAL']
    end

    # Rakefile is loaded after 'init' before 'run'

    def run
      setup_worker
      setup_shell
      setup_fiber
      setup_master_channel
      @cs.run("task execution")
      Log.debug "Brandh#run end"
    end

    attr_reader :logger

    def init_logger
      if dir = @option['LOG_DIR']
        logfile = File.join(dir,@option['LOG_FILE'])
        @logger = Logger.new(logfile)
      else
        if @option['DEBUG']
          @logger = Logger.new($stderr)
        else
          @logger = Logger.new(File::NULL)
        end
      end

      if @option['DEBUG']
        @logger.level = Logger::DEBUG
      elsif @option['TRACE']
        @logger.level = Logger::INFO
      else
        @logger.level = Logger::WARN
      end

      if dir = @option['LOG_DIR']
        fn = File.join(dir,@option["COMMAND_CSV_FILE"])
        Shell.profiler.open(fn,@option['GNU_TIME'],@option['PLOT_PARALLELISM'])
      end
    end

    def setup_worker
      @cs = CommunicatorSet.new(@master_rd,@selector,@option.worker_option)
      @cs.create_communicators
      worker_code = read_worker_progs(@option.worker_progs)
      @cs.each_value do |comm|
        Fiber.new do
          comm.connect(worker_code)
        end.resume
      end
      @cs.run("connect to workers")
      #
      Fiber.new do
        @cs.each_value do |comm|
          # set WorkerChannel#ncore at Master
          @master_wt.put_line "ncore:#{comm.id}:#{comm.ncore}"
        end
        @master_wt.put_line "ncore:done"
      end.resume
      @selector.run
    end

    def read_worker_progs(worker_progs)
      d = File.dirname(__FILE__)+'/../worker/'
      code = ""
      worker_progs.each do |f|
        code << IO.read(d+f+'.rb')
      end
      code
    end

    def setup_shell
      @shells = []
      @cs.each_value do |comm|
        puts "comm.host=#{comm.host} comm.id=#{comm.id}"
        @task_q[comm.id] = task_q = FiberQueue.new
        #puts "task_q=#{task_q.inspect} @task_q=#{@task_q.inspect} comm.id=#{comm.id}"
        comm.ncore.times do
          #chan = Channel.new(comm.handler,shell_id)
          chan = comm.new_channel
          shell = Shell.new(chan,task_q,@option.worker_option)
          @shells << shell
          # wait for remote shell open
          Fiber.new do
            shell.open
            Log.debug "Branch#setup_shells: end of fiber to open shell"
          end.resume
          sleep @shell_start_interval
        end
      end

      @cs.run("setup shells")
    end

    def setup_fiber
      # start fibers
      @shells.each do |shell|
        shell.create_fiber(@master_wt).resume
      end
      Log.debug "all fiber started"

      @cs.each_value do |comm|
        #comm.start_default_fiber
        Fiber.new do
          while s = comm.reader.get_line
            break unless comm.common_line(s)
          end
          Log.debug "Branch#setup_fiber: end of fiber for default channel"
        end.resume
      end

      # setup end
      @cs.each_value do |comm|
        comm.writer.put_line "setup_end"
      end

      @master_wt.put_line "branch_setup:done"
      Log.debug "Branch#setup_fiber: setup end"
    end

    def setup_master_channel
      Fiber.new do
        while s = @master_rd.get_line
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
            @selector.finish
            break
            #
          when /^kill:(.*)$/o
            sig = $1
            kill(sig)
          else
            Log.debug "Branch: invalid line from master: #{s}"
          end
        end
        Log.debug "Branch#setup_master_channel: end of fiber for master channel"
      end.resume
    end

    def kill(sig="INT")
      Log.warn "Branch#kill #{sig}"
      #@wk_hdl_set.kill(sig)
    end

    def finish
      return if @finished
      @finished = true
      #Log.debug "Branch#finish: begin"
      #@wk_hdl_set.exit
      Log.debug "Branch#finish: worker exited"
      @master_wt.put_line "exited"
      Log.debug "Branch#finish: sent 'exited' to master"
    end

  end # Pwrake::Branch
end
