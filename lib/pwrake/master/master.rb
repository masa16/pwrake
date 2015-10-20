module Pwrake

  class Master

    def initialize
      @runner = Runner.new
      @hostid_by_taskname = {}
      @idle_cores = IdleCores.new
      @option = Option.new
      @exit_task = []
      @hdl_list = []
      @channels = {}
      @hosts = {}
      init_logger
    end

    attr_reader :task_queue
    attr_reader :option
    attr_reader :logger
    attr_reader :task_logger

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
      init_tasklog
    end

    def init_tasklog
      if tasklog = @option['TASKLOG']
        if dir = @option['LOG_DIR']
          ::FileUtils.mkdir_p(dir)
          tasklog = File.join(dir,tasklog)
        end
        @task_logger = File.open(tasklog,'w')
        @task_logger.print %w[
          task_id task_name start_time end_time elap_time preq preq_host
          exec_host shell_id has_action executed file_size file_mtime file_host
        ].join(',')+"\n"
      end
    end

    def setup_branches
      [:TERM,:INT].each do |sig|
        Signal.trap(sig) do
          @hdl_list.each do |hdl|
            hdl.kill(sig)
          end
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
        @hdl_list << hdl
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
            @post_pool.enq(tw)
            @exit_task.delete(tw.name)
            # returns true (end of loop) if @exit_task.empty?
            if tw.status=="fail" || @exit_task.empty?
              break
            end
          when /^branch_end$/o
            Log.warn "receive #{s.chomp} from branch"
            break
          else
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
        puts "--- create new fiber"
        postproc = @option.postprocess(@runner)
        Fiber.new do
          while tw = pool.deq()
            loc = postproc.run(tw.name)
            tw.postprocess(loc)
            if tw.status=="fail" || @exit_task.empty?
              @post_pool.finish
            else
              send_task_to_idle_core
            end
          end
          postproc.close
          puts "--- end of fiber"
        end
      end
    end

    def finish
      @branch_setup_thread.join
      create_fiber(@channels.values) do |chan|
        s = chan.get_line
        if s.nil? || /^branch_end$/o =~ s
          Log.debug "#{self.class}#finish: host=#{comm.host} s=#{s}"
        end
      end
      @hdl_list.each do |hdl|
        hdl.close
      end
      @task_logger.close if @task_logger
      Log.debug "master:finish"
    end

  end
end
