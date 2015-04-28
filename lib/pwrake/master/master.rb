module Pwrake

  class Master

    def initialize
      @dispatcher = IODispatcher.new
      @hostid_by_taskname = {}
      @idle_cores = IdleCores.new
      @workers = {}
      @writer = {}
      @option = Option.new
      @exit_task = []
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
      @conn_list = []
      @comm_by_io = {}

      @option.host_map.each do |sub_host, wk_hosts|
        conn = BranchCommunicator.new(sub_host,@option,self)
        @conn_list << conn
        @dispatcher.attach_communicator(conn)
        @writer[conn.ior] = $stdout
        @writer[conn.ioe] = $stderr
        @comm_by_io[conn.ior] = conn
        conn.send_cmd "begin_worker_list"
        wk_hosts.each do |host_info|
          name = host_info.name
          ncore = host_info.ncore
          Log.debug "connecting #{name} ncore=#{ncore}"
          chan = WorkerChannel.new(conn,name,ncore)
          @workers[chan.id] = chan
          #conn.send_cmd "#{chan.id}:#{name} #{ncore}"
        end
        conn.send_cmd "end_worker_list"
      end

      # receive ncore from WorkerCommunicator at Branch
      #Log.debug "@comm_by_io.keys: #{@comm_by_io.keys.inspect}"
      sum_ncore = 0
      io_list = @comm_by_io.keys
      io_fail = []
      IODispatcher.event_once(io_list,10) do |io|
        if io.eof?
          io_fail << io
          break
        end
        while s = io.gets
          s.chomp!
          Log.debug "in Master#setup_branches, event_once: #{s}"
          case s
          when /ncore:done/
            break
          when /ncore:(\d+):(\d+)/
            id, ncore = $1.to_i, $2.to_i
            Log.debug "worker_id=#{id} ncore=#{ncore}"
            @workers[id].ncore = ncore
            @idle_cores[id] = ncore
            sum_ncore += ncore
          else
            raise "Invalid return: #{s}"
          end
        end
      end
      if !(io_list.empty? && io_fail.empty?)
        t = io_list.map{|io| @comm_by_io[io].host}.join(',')
        f = io_fail.map{|io| @comm_by_io[io].host}.join(',')
        raise RuntimeError, "error in connection to branch: fail:(#{f}),timeout:(#{t})"
      end

      Log.info "num_cores=#{sum_ncore}"
      @workers.each do |id,wk|
        Log.info "#{wk.host} id=#{wk.id} ncore=#{wk.ncore}"
      end
      @task_queue = Pwrake.const_get(@option.queue_class).new(@idle_cores)
    end

    def invoke(t, args)
      @exit_task << t
      t.pw_search_tasks(args)
      wake_idle_core
      @dispatcher.event_loop
    end

    def wake_idle_core
      # @idle_cores.decrease(..
      @task_queue.deq_task do |tw,hid|
        tw.preprocess
        @hostid_by_taskname[tw.name] = hid
        #if tw.has_action?
          @workers[hid].send_task(tw)
          tw.exec_host = @workers[hid].host
        #else
        #  taskend_proc("noaction",-1,tw.name)
        #end
      end
    end

    def respond_from_branch(io) # called from BranchCommunicator#on_read
      s = io.gets
      case s
      when /^taskend:(\d*):(.*)$/o
        taskend_proc("end",$1.to_i,$2)
        # returns true (end of loop) if @exit_task.empty?
      when /^taskfail:(\d*):(.*)$/o
        taskend_proc("fail",$1.to_i,$2)
        # returns true (end of loop)
      when /^branch_end$/o
        s.chomp!
        Log.warn "receive #{s} from branch"
        @dispatcher.detach_communicator(@comm_by_io[io])
        @comm_by_io.delete(io)
        #@pool.finish if @pool
        @comm_by_io.empty? # exit condition
      else
        @writer[io].print(s)
        nil
      end
    end

    def taskend_proc(status, shell_id, task_name)
      tw = Rake.application[task_name].wrapper
      tw.shell_id = shell_id
      tw.status = status
      id = @hostid_by_taskname.delete(task_name)
      @task_queue.task_end(tw, id) # @idle_cores.increase(..
      if @pool && tw.task.kind_of?(Rake::FileTask)
        @pool.enq(tw)
      else
        tw.postprocess([])
        taskend_end(tw)
      end
    end

    def setup_postprocess
      @pool = @option.pool_postprocess(@dispatcher)
      if @pool
        @pool.set_block do |tw,loc|
          tw.postprocess(loc)
          taskend_end(tw)
        end
      end
    end

    def taskend_end(tw)
      @exit_task.delete(tw.task)
      if tw.status=="fail" || @exit_task.empty?
        @pool.finish if @pool
        true
      else
        wake_idle_core
        nil
      end
    end

    def finish
      #@task_queue.finish if @task_queue
      @conn_list.each do |conn|
        conn.close
      end
      @dispatcher.finish
      @dispatcher.event_loop_block do |io|
        s = io.gets
        if s.nil? || /^branch_end$/o =~ s
          @dispatcher.detach_communicator(@comm_by_io[io])
          @comm_by_io.delete(io)
        end
        @comm_by_io.empty? # exit condition
      end
      @task_logger.close if @task_logger
      Log.debug "master:finish"
    end

  end
end
