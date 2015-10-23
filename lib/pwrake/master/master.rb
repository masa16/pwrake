module Pwrake

  class Master

    def initialize
      @runner = Runner.new
      @hostid_by_taskname = {}
      @idle_cores = IdleCores.new
      @option = Option.new
      @exit_task = []
      @hdl_set = HandlerSet.new
      @channels = {}
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
        @logger = Logger.new($stdout)
      end

      if @option['DEBUG']
        @logger.level = Logger::DEBUG
      elsif @option['TRACE']
        @logger.level = Logger::INFO
      else
        @logger.level = Logger::WARN
      end
    end

    def init(hosts=nil)
      @option.init
      TaskWrapper.init_task_logger(@option)
    end

    def setup_branches
      @killed = 0
      [:TERM,:INT].each do |sig|
        Signal.trap(sig) do
          @hdl_set.terminate(sig)
        end
      end

      sum_ncore = 0

      @option.host_map.each do |sub_host, wk_hosts|
        hdl = Handler.new(@runner) do |w0,w1,r2|
          Thread.new(r2,w0,@option) do |r,w,o|
            Rake.application.run_branch_in_thread(r,w,o)
          end
        end
        hdl.host = sub_host
        hdl.set_close_block do |h|
          h.put_line "exit_branch"
        end
        @hdl_set << hdl
        chan = Channel.new(hdl)
        chan.put_line "host_list_begin"

        wk_hosts.each do |host_info|
          name = host_info.name
          ncore = host_info.ncore
          host_id = host_info.id
          Log.debug "connecting #{name} ncore=#{ncore} id=#{host_id}"
          chan.put_line "host:#{host_id} #{name} #{ncore}"
          @channels[host_id] = chan
          @hosts[host_id] = name
        end
        chan.put_line "host_list_end"

        create_fiber([chan]) do |chan|
          while s = chan.get_line
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
        @runner.run
      end

      Log.info "num_cores=#{sum_ncore}"
      @hosts.each do |id,host|
        Log.info "#{host} id=#{id} ncore=#{@idle_cores[id]}"
      end
      q_class = Pwrake.const_get(@option.queue_class)
      @task_queue = q_class.new(@idle_cores)

      # wait for branch setup end
      @branch_setup_thread = Thread.new do
        create_fiber(@channels.values) do |chan|
          s = chan.get_line
          if /^branch_setup:done$/ !~ s
            raise "branch_setup failed" # "#{x.handler.host}:#{s}"
          end
        end
        @runner.run
      end
    end

    def create_fiber(channels,&blk)
      channels.each do |chan|
        fb = Fiber.new(&blk)
        fb.resume(chan)
      end
    end

    def invoke(t, args)
      @exit_task << t.name
      t.pw_search_tasks(args)
      @branch_setup_thread.join
      send_task_to_idle_core
      #
      create_fiber(@channels.values) do |chan|
        while s = chan.get_line
          case s
          when /^task(\w+):(\d*):(.*)$/o
            status, shell_id, task_name = $1, $2.to_i, $3
            tw = Rake.application[task_name].wrapper
            tw.shell_id = shell_id
            tw.status = status
            hid = @hostid_by_taskname.delete(task_name)
            @task_queue.task_end(tw,hid) # @idle_cores.increase(..
            # check failure
            if tw.status=="fail"
              Log.warn "taskfail: #{tw.name}"
              if tw.is_file_task? && File.exist?(tw.name)
                handle_failed_target(tw.name)
              end
              # failure termination
              case @option['FAILURE_TERMINATION']
              when 'kill'
                if !@no_more_run
                  $stderr.puts "... kills running tasks"
                  @no_more_run = true
                  @hdl_set.kill_all("INT")
                end
              when 'continue'
                $stderr.puts "... continues runable tasks"
              else # 'WAIT'
                if !@no_more_run
                  $stderr.puts "... waits for running tasks"
                  @no_more_run = true
                end
              end
            end
            # check exit
            @exit_task.delete(tw.name)
            if @exit_task.empty?
              @no_more_run = true
            end
            # postprocess
            @post_pool.enq(tw) # must be after @no_more_run = true
            #if @no_more_run && @hostid_by_taskname.empty?
            if @hostid_by_taskname.empty?
              break
            end
          when /^branch_end$/o
            Log.debug "receive #{s.chomp} from branch"
            break
          else
            Log.error "unknown result: #{s.inspect}"
            $stderr.puts(s)
          end
          #puts "exit_task=#{@exit_task}"
        end
      end
      @runner.run
      @post_pool.finish
    end

    def send_task_to_idle_core
      #Log.debug "#{self.class}#send_task_to_idle_core start"
      tm = Time.now
      # @idle_cores.decrease(..
      @task_queue.deq_task do |tw,hid|
        tw.preprocess
        @hostid_by_taskname[tw.name] = hid
        #if tw.has_action?
          s = "#{hid}:#{tw.task_id}:#{tw.name}"
          @channels[hid].put_line(s)
          tw.exec_host = @hosts[hid]
        #else
        #  taskend_proc("noaction",-1,tw.name)
        #end
      end
      #Log.debug "#{self.class}#send_task_to_idle_core end time=#{Time.now-tm}"
    end

    def setup_postprocess
      n = @option.max_postprocess_pool
      @post_pool = FiberPool.new(n) do |pool|
        postproc = @option.postprocess(@runner)
        Fiber.new do
          while tw = pool.deq()
            loc = postproc.run(tw.name)
            tw.postprocess(loc)
            if !@no_more_run
              send_task_to_idle_core
            end
          end
          postproc.close
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
      @branch_setup_thread.join
      @hdl_set.close_all
      @hdl_set.wait_close("Master#finish","branch_end")
      TaskWrapper.close_task_logger
      Log.debug "master:finish"
    end

  end
end
