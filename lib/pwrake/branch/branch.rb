require "pwrake/nbio"
require "pwrake/branch/communicator_set"
require "pwrake/branch/fiber_queue"
require "pwrake/branch/shell"
require "pwrake/branch/file_utils"
require "pwrake/option/option"

module Pwrake

  class Branch

    @@io_class = IO

    def self.io_class=(io_class)
      @@io_class = io_class
    end

    def initialize(opts,r,w)
      Thread.abort_on_exception = true
      @option = opts
      @task_q = {}  # worker_id => FiberQueue.new
      @shells = []
      @ior = r
      @iow = w
      @selector = NBIO::Selector.new(@@io_class)
      @master_rd = NBIO::Reader.new(@selector,@ior)
      @master_wt = NBIO::Writer.new(@selector,@iow)
      @shell_start_interval = @option['SHELL_START_INTERVAL']
    end

    # Rakefile is loaded after 'init' before 'run'

    def run
      setup_worker
      setup_shell
      setup_fiber
      setup_master_channel
      @cs.run("task execution")
      Log.debug "Branch#run end"
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
      at_exit{@logger.close}

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
      code = ""
      modpath = {}
      worker_progs.each do |f|
        m = f.split(/\//).first
        if !modpath[m]
          $LOAD_PATH.each do |x|
            if File.directory?(File.join(x,m))
              modpath[m] = x
              break
            end
          end
          if !modpath[m]
            raise RuntimeError,"load path for module #{m} not found"
          end
        end
        path = File.join(modpath[m],f)
        path += '.rb' if /\.rb$/ !~ path
        if !File.exist?(path)
          raise RuntimeError,"program file #{path} not found"
        end
        code << IO.read(path) + "\n"
      end
      code
    end

    def setup_shell
      @shells = []
      @cs.each_value do |comm|
        @task_q[comm.id] = task_q = FiberQueue.new
        comm.ncore.times do
          chan = comm.new_channel
          shell = Shell.new(chan,comm,task_q,@option.worker_option)
          # wait for remote shell open
          Fiber.new do
            if shell.open
              @shells << shell
            else
              @master_wt.put_line "retire:#{comm.id}"
            end
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
            begin
              task_name = tname.sub(/^\d+:/,"")
              @task_q[id].enq(tname)
            rescue => e
              Log.error Log.bt(e)
              ret="taskfail:#{id}:#{task_name}"
              Log.debug "fail to enq task_q[#{id}], ret=#{ret}"
              @master_wt.put_line(ret)
            end
            #
          when /^exit$/
            #@task_q.each_value{|q| q.finish}
            #@cs.drop_all
            @cs.finish_shells

            #@shells.each{|shell| shell.exit} # just for comfirm
            #@selector.halt # should halt after exited
            break
            #
          when /^drop:(.*)$/o
            id = $1
            taskq = @task_q.delete(id)
            Log.debug "drop @task_q[#{id}]=#{taskq.inspect}"
            @cs.drop(id)
            #
          when /^kill:(.*)$/o
            sig = $1
            kill(sig)
            #
          else
            Log.debug "Branch: invalid line from master: #{s}"
          end
        end
        Log.debug "Branch#setup_master_channel: end of fiber for master channel"
      end.resume
    end

    def kill(sig="INT")
      Log.warn "Branch#kill #{sig}"
      @cs.kill(sig)
    end

    def finish
      return if @finished
      @finished = true
      #Log.debug "Branch#finish: begin"
      @cs.exit
      Log.debug "Branch#finish: worker exited"
      @master_wt.put_line "exited"
      Log.debug "Branch#finish: sent 'exited' to master"
    end

  end # Pwrake::Branch
end
