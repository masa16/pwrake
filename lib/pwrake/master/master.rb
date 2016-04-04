require "fileutils"
require "pwrake/logger"
require "pwrake/aio"
require "pwrake/option/option"
require "pwrake/task/task_wrapper"
require "pwrake/queue/task_queue"
require "pwrake/master/fiber_pool"

module Pwrake

  class Master

    def initialize
      @selector = AIO::Selector.new
      @hostid_by_taskname = {}
      @option = Option.new
      @hdl_set = []
      @channel_by_hostid = {}
      @channels = []
      @hosts = {}
      init_logger
    end

    attr_reader :task_queue
    attr_reader :option
    attr_reader :logger

    def init_logger
      if logdir = @option['LOG_DIR']
        ::FileUtils.mkdir_p(logdir)
        logfile = File.join(logdir,@option['LOG_FILE'])
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
      else
        @logger.level = Logger::INFO
      end
    end

    def init(hosts=nil)
      @option.init
      TaskWrapper.init_task_logger(@option)
    end

    def setup_branch_handler(sub_host)
      ior,w0 = IO.pipe
      r2,iow = IO.pipe
      if sub_host == "localhost" && /^(n|f)/i !~ ENV['T']
        @thread = Thread.new(r2,w0,@option) do |r,w,o|
          Rake.application.run_branch_in_thread(r,w,o)
        end
      else
        dir = File.absolute_path(File.dirname($PROGRAM_NAME))
        cmd = "ssh -x -T -q #{sub_host} '" +
          "cd \"#{Dir.pwd}\";"+
          "PATH=#{dir}:${PATH} exec pwrake_branch'"
        Log.debug("BranchCommunicator cmd=#{cmd}")
        spawn(cmd,:pgroup=>true,:out=>w0,:in=>r2)
        w0.close
        r2.close
        Marshal.dump(@option,iow)
        iow.flush
        s = ior.gets
        if !s or s.chomp != "pwrake_branch start"
          raise RuntimeError,"pwrake_branch start failed: receive #{s.inspect}"
        end
      end
      return AIO::Handler.new(@selector,ior,iow,sub_host)
    end

    def signal_trap(sig)
      case @killed
      when 0
        # log writing failed. can't be called from trap context
        if Rake.application.options.debug
          $stderr.puts "\nSignal trapped. (sig=#{sig} pid=#{Process.pid}"+
            " thread=#{Thread.current} ##{@killed})"
          $stderr.puts caller
        else
          $stderr.puts "\nSignal trapped. (sig=#{sig} pid=#{Process.pid}"+
            " ##{@killed})"
        end
        $stderr.puts "Exiting..."
        @no_more_run = true
        @failed = true
        AIO::Handler.kill(@hdl_set,sig)
        # @selector.run : not required here
      when 1
        $stderr.puts "\nOnce more Ctrl-C (SIGINT) for exit."
      else
        Kernel.exit(false) # must wait for nomral exit
      end
      @killed += 1
    end

    def setup_branches
      sum_ncore = 0
      @option.host_map.each do |sub_host, wk_hosts|
        @hdl_set << hdl = setup_branch_handler(sub_host)
        Fiber.new do
        hdl.put_line "host_list_begin"
        wk_hosts.each do |host_info|
          name = host_info.name
          ncore = host_info.ncore
          host_id = host_info.id
          Log.debug "connecting #{name} ncore=#{ncore} id=#{host_id}"
          hdl.put_line "host:#{host_id} #{name} #{ncore}"
          @channel_by_hostid[host_id] = hdl
          @hosts[host_id] = name
        end
        hdl.put_line "host_list_end"
        while s = hdl.get_line
          case s
          when /^ncore:done$/
            break
          when /^ncore:(\d+):(\d+)$/
            id, ncore = $1.to_i, $2.to_i
            Log.debug "worker_id=#{id} ncore=#{ncore}"
            @option.host_map.by_id[id].set_ncore(ncore)
            sum_ncore += ncore
          when /^exited$/
            raise RuntimeError,"Unexpected branch exit"
          else
            msg = "#{hdl.host}:#{s.inspect}"
            raise RuntimeError,"invalid return: #{msg}"
          end
        end
        end.resume
      end
      @selector.run

      Log.info "num_cores=#{sum_ncore}"
      @hosts.each do |id,host|
        Log.info "#{host} id=#{id} ncore=#{
          @option.host_map.by_id[id].idle_cores}"
      end
      queue_class = Pwrake.const_get(@option.queue_class)
      @task_queue = queue_class.new(@option.host_map)

      @branch_setup_thread = Thread.new do
        #@channels.each do |chan|
        create_fiber(@hdl_set) do |hdl|
          while s = hdl.get_line
            case s
            when /^retire:(\d+)$/
              @option.host_map.by_id[$1.to_i].decrease(1)
            when /^branch_setup:done$/
              break
            else
              raise RuntimeError,"branch_setup failed: s=#{s.inspect}"
            end
          end
        end
        @selector.run
        @killed = 0
        [:TERM,:INT].each do |sig|
          Signal.trap(sig) do
            signal_trap(sig)
          end
        end
      end

    end

    def create_fiber(channels,&blk)
      channels.each do |chan|
        fb = Fiber.new(&blk)
        fb.resume(chan)
      end
    end

    def invoke(t, args)
      @failed = false
      t.pw_search_tasks(args)

      if @option['GRAPH_PARTITION']
        setup_postprocess0
        @task_queue.deq_noaction_task do |tw,hid|
          tw.preprocess
          tw.status = "end"
          @post_pool.enq(tw)
        end
        @selector.run
        @post_pool.finish
        Log.debug "@post_pool.finish"

        require 'pwrake/misc/mcgp'
        MCGP.graph_partition(@option.host_map)
      end

      setup_postprocess1
      @branch_setup_thread.join
      send_task_to_idle_core
      #
      create_fiber(@hdl_set) do |hdl|
        while s = hdl.get_line
          Log.debug "Master:recv #{s.inspect} from branch[#{hdl.host}]"
          case s
          when /^task(\w+):(\d*):(.*)$/o
            status, shell_id, task_name = $1, $2.to_i, $3
            tw = Rake.application[task_name].wrapper
            tw.shell_id = shell_id
            tw.status = status
            hid = @hostid_by_taskname[task_name]
            if hid.nil?
              raise "unknown hostid: task_name=#{task_name} s=#{s.inspect} @hostid_by_taskname=#{@hostid_by_taskname.inspect}"
            end
            @task_queue.task_end(tw,hid) # @idle_cores.increase(..
            # check failure
            if tw.status == "fail"
              $stderr.puts %[task "#{tw.name}" failed.]
              if tw.no_more_retry?
                if !@failed
                  @failed = true
                  case @option['FAILURE_TERMINATION']
                  when 'kill'
                    AIO::Handler.kill(@hdl_set,"INT")
                    @selector.run
                    @no_more_run = true
                    $stderr.puts "... Kill running tasks."
                  when 'continue'
                    $stderr.puts "... Continue runable tasks."
                  else # 'wait'
                    @no_more_run = true
                    $stderr.puts "... Wait for running tasks."
                  end
                end
                if tw.has_output_file? && File.exist?(tw.name)
                  handle_failed_target(tw.name)
                end
              end
            end
            # postprocess
            @post_pool.enq(tw) # must be after @no_more_run = true
            break if @finished
          when /^exited$/o
            @exited = true
            Log.debug "receive #{s.chomp} from branch"
            break
          else
            Log.error "unknown result: #{s.inspect}"
            $stderr.puts(s)
          end
        end
        Log.debug "Master#invoke: fiber end"
      end
      if !ending?
        Log.debug "@selector.run begin"
        @selector.run
        Log.debug "@selector.run end"
      end
      @post_pool.finish
      Log.debug "Master#invoke: end of task=#{t.name}"
    end

    def send_task_to_idle_core
      #Log.debug "#{self.class}#send_task_to_idle_core start"
      count = 0
      # @idle_cores.decrease(..
      @task_queue.deq_task do |tw,hid|
        count += 1
        @hostid_by_taskname[tw.name] = hid
        tw.preprocess
        if tw.has_action?
          s = "#{hid}:#{tw.task_id}:#{tw.name}"
          @channel_by_hostid[hid].put_line(s)
          tw.exec_host = @hosts[hid]
        else
          tw.status = "end"
          @task_queue.task_end(tw,hid) # @idle_cores.increase(..
          @post_pool.enq(tw)
        end
      end
      if count == 0 && !@task_queue.empty? && @hostid_by_taskname.empty?
        m="No task was invoked while unexecuted tasks remain"
        Log.error m
        raise RuntimeError,m
      end
      #Log.debug "#{self.class}#send_task_to_idle_core end time=#{Time.now-tm}"
    end

    def setup_postprocess
      i = 0
      n = @option.max_postprocess_pool
      @post_pool = FiberPool.new(n) do |pool|
        postproc = @option.postprocess(@selector)
        i += 1
        Log.debug "New postprocess fiber ##{i}"
        Fiber.new do
          j = i
          while tw = pool.deq()
            Log.debug "postproc##{j} deq=#{tw.name}"
            loc = postproc.run(tw)
            tw.postprocess(loc)
            pool.count_down
            @hostid_by_taskname.delete(tw.name)
            tw.retry_or_subsequent
            break if yield(pool,j)
          end
          postproc.close
          Log.debug "postproc##{j} end"
        end
      end
    end

    def setup_postprocess0
      setup_postprocess{false}
    end

    def setup_postprocess1
      setup_postprocess do |pool,j|
        #Log.debug "@no_more_run=#{@no_more_run.inspect}"
        #Log.debug "@task_queue.empty?=#{@task_queue.empty?}"
        #Log.debug "@hostid_by_taskname=#{@hostid_by_taskname.inspect}"
        #Log.debug "pool.empty?=#{pool.empty?}"
        if ending?
          Log.debug "postproc##{j} closing @channels=#{@channels.inspect}"
          @finished = true
          @hdl_set.each{|hdl| hdl.break_fiber} # get out of fiber
          true
        elsif !@no_more_run
          send_task_to_idle_core
          false
        end
      end
    end

    def ending?
      (@no_more_run || @task_queue.empty?) && @hostid_by_taskname.empty?
    end

    def handle_failed_target(name)
      case @option['FAILED_TARGET']
        #
      when /rename/i, NilClass
        dst = name+"._fail_"
        ::FileUtils.mv(name,dst)
        msg = "Rename failed target file '#{name}' to '#{dst}'"
        $stderr.puts(msg)
        Log.warn(msg)
        #
      when /delete/i
        ::FileUtils.rm(name)
        msg = "Delete failed target file '#{name}'"
        $stderr.puts(msg)
        Log.warn(msg)
        #
      when /leave/i
      end
    end

    def finish
      Log.debug "Master#finish begin"
      @branch_setup_thread.join
      if !@exited
        AIO::Handler.exit(@hdl_set)
        @selector.run
      end
      TaskWrapper.close_task_logger
      Log.debug "Master#finish end"
      @failed
    end

  end
end
