module Pwrake

  class Master

    def initialize
      @runner = Runner.new
      @hostid_by_taskname = {}
      @idle_cores = IdleCores.new
      @option = Option.new
      @hdl_set = HandlerSet.new
      @channel_by_hostid = {}
      @channels = []
      @hosts = {}
      init_logger
    end

    attr_reader :task_queue
    attr_reader :option
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
      if sub_host == "localhost" && /^(n|f)/i !~ ENV['T']
        hdl = Handler.new(@runner) do |w0,w1,r2|
          @thread = Thread.new(r2,w0,@option) do |r,w,o|
            Rake.application.run_branch_in_thread(r,w,o)
          end
        end
      else
        hdl = Handler.new(@runner) do |w0,w1,r2|
          dir = File.absolute_path(File.dirname($PROGRAM_NAME))
          #args = Shellwords.shelljoin(@args)
          cmd = "ssh -x -T -q #{sub_host} '" +
            "cd \"#{Dir.pwd}\";"+
            "PATH=#{dir}:${PATH} exec pwrake_branch'"
          Log.debug("BranchCommunicator cmd=#{cmd}")
          #$stderr.puts "BranchCommunicator cmd=#{cmd}"
          spawn(cmd,:pgroup=>true,:out=>w0,:err=>w1,:in=>r2)
          w0.close
          w1.close
          r2.close
        end
        Marshal.dump(@option,hdl.iow)
        hdl.iow.flush
        s = hdl.ior.gets
        if !s or s.chomp != "pwrake_branch start"
          raise RuntimeError,"pwrake_branch start failed: receive #{s.inspect}"
        end
      end
      hdl.host = sub_host
      return hdl
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
        @hdl_set.kill(sig)
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
        @channels << chan = Channel.new(hdl)
        chan.puts "host_list_begin"
        wk_hosts.each do |host_info|
          name = host_info.name
          ncore = host_info.ncore
          host_id = host_info.id
          Log.debug "connecting #{name} ncore=#{ncore} id=#{host_id}"
          chan.puts "host:#{host_id} #{name} #{ncore}"
          @channel_by_hostid[host_id] = chan
          @hosts[host_id] = name
        end
        chan.puts "host_list_end"

        while s = chan.gets
          case s
          when /ncore:done/
            break
          when /ncore:(\d+):(\d+)/
            id, ncore = $1.to_i, $2.to_i
            Log.debug "worker_id=#{id} ncore=#{ncore}"
            #@workers[id].ncore = ncore
            @idle_cores[id] = ncore
            sum_ncore += ncore
          else
            msg = "#{hdl.host}:#{s.inspect}"
            raise "invalid return: #{msg}"
          end
        end
      end

      Log.info "num_cores=#{sum_ncore}"
      @hosts.each do |id,host|
        Log.info "#{host} id=#{id} ncore=#{@idle_cores[id]}"
      end
      @task_queue = Pwrake.const_get(@option.queue_class).new(@idle_cores)

      @branch_setup_thread = Thread.new do
        @channels.each do |chan|
          s = chan.gets
          if /^branch_setup:done$/ !~ s
            raise "branch_setup failed" # "#{x.handler.host}:#{s}"
          end
        end
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
        @runner.run
        @post_pool.finish
        Log.debug "@post_pool.finish"

        require 'pwrake/misc/mcgp'
        MCGP.graph_partition(@option.host_map)
      end

      setup_postprocess1
      @branch_setup_thread.join
      send_task_to_idle_core
      #
      create_fiber(@channels) do |chan|
        while s = chan.get_line
          Log.debug "Master:recv #{s.inspect} from branch[#{chan.handler.host}]"
          case s
          when /^task(\w+):(\d*):(.*)$/o
            status, shell_id, task_name = $1, $2.to_i, $3
            tw = Rake.application[task_name].wrapper
            tw.shell_id = shell_id
            tw.status = status
            hid = @hostid_by_taskname[task_name]
            @task_queue.task_end(tw,hid) # @idle_cores.increase(..
            # check failure
            if tw.status == "fail"
              $stderr.puts %[task "#{tw.name}" failed.]
              if !@failed
                @failed = true
                case @option['FAILURE_TERMINATION']
                when 'kill'
                  @hdl_set.kill("INT")
                  @no_more_run = true
                  $stderr.puts "... Kill running tasks."
                when 'continue'
                  $stderr.puts "... Continue runable tasks."
                else # 'wait'
                  @no_more_run = true
                  $stderr.puts "... Wait for running tasks."
                end
              end
              if tw.is_file_task? && File.exist?(tw.name)
                handle_failed_target(tw.name)
              end
            end
            # postprocess
            hid = @hostid_by_taskname.delete(task_name)
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
      @runner.run
      @post_pool.finish
      Log.debug "Master#invoke: end of task=#{t.name}"
    end

    def send_task_to_idle_core
      #Log.debug "#{self.class}#send_task_to_idle_core start"
      # @idle_cores.decrease(..
      @task_queue.deq_task do |tw,hid|
        @hostid_by_taskname[tw.name] = hid
        tw.preprocess
        if tw.has_action?
          s = "#{hid}:#{tw.task_id}:#{tw.name}"
          @channel_by_hostid[hid].put_line(s)
          tw.exec_host = @hosts[hid]
        else
          tw.status = "end"
          @task_queue.task_end(tw,hid) # @idle_cores.increase(..
          hid = @hostid_by_taskname.delete(tw.name)
          @post_pool.enq(tw)
        end
      end
      #Log.debug "#{self.class}#send_task_to_idle_core end time=#{Time.now-tm}"
    end

    def setup_postprocess
      i = 0
      n = @option.max_postprocess_pool
      @post_pool = FiberPool.new(n) do |pool|
        postproc = @option.postprocess(@runner)
        i += 1
        Log.debug "New postprocess fiber ##{i}"
        Fiber.new do
          j = i
          while tw = pool.deq()
            Log.debug "postproc##{j} deq=#{tw.name}"
            loc = postproc.run(tw.name)
            tw.postprocess(loc)
            pool.count_down
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
        if (@no_more_run || @task_queue.empty?) &&
            @hostid_by_taskname.empty? && pool.empty?
          Log.debug "postproc##{j} closing @channels=#{@channels.inspect}"
          @finished = true
          @channels.each{|ch| ch.finish} # exit
          true
        elsif !@no_more_run
          send_task_to_idle_core
          false
        end
      end
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
      @hdl_set.exit unless @exited
      TaskWrapper.close_task_logger
      Log.debug "Master#finish end"
      @failed
    end

  end
end
