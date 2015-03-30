module Pwrake

  class Master

    def initialize
      @dispatcher = IODispatcher.new
      @id_by_taskname = {}
      @idle_cores = {}
      @workers = {}
      @writer = {}
      @option = Option.new
      @exit_task = []
      init_logger(@option['LOGFILE'])
    end

    attr_reader :task_queue
    attr_reader :option
    attr_reader :logger
    attr_reader :task_logger

    def init_logger(logfile=nil)
      if logfile
        logdir = File.dirname(logfile)
        if !File.directory?(logdir)
          mkdir_p logdir
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
        conn = BranchCommunicator.new(sub_host,@option)
        @conn_list << conn
        @dispatcher.attach_communicator(conn)
        @writer[conn.ior] = $stdout
        @writer[conn.ioe] = $stderr
        @comm_by_io[conn.ior] = conn
        conn.send_cmd "begin_worker_list"
        wk_hosts.each do |host_info|
          name = host_info.name
          ncore = host_info.ncore
          $stderr.puts "connecting #{name} ncore=#{ncore}"
          chan = WorkerChannel.new(conn,name,ncore)
          @workers[chan.id] = chan
          conn.send_cmd "#{chan.id}:#{name} #{ncore}"
        end
        conn.send_cmd "end_worker_list"
      end

      # receive ncore from WorkerCommunicator at Branch
      #Log.debug "@comm_by_io.keys: #{@comm_by_io.keys.inspect}"
      IODispatcher.event_once(@comm_by_io.keys,10) do |io|
        while true
          case s = io.gets
          when /ncore:done/
            break
          when /ncore:(\d+):(\d+)/
            id, ncore = $1.to_i, $2.to_i
            Log.debug "worker-id:#{id} ncore:#{ncore}"
            @workers[id].ncore = ncore
            @idle_cores[id] = ncore
          else
            raise "Invalid return: #{s}"
          end
        end
      end

      @task_queue = @option.queue_class.new(@option.host_map)
    end

    def invoke(t, args)
      @exit_task << t
      t.pw_search_tasks(args)
      wake_idle_core
      @dispatcher.event_loop do |io|
        s = io.gets
        s.chomp!
        case s
        when /^taskend:(.*)$/o
          on_taskend($1) # returns true if @exit_task.empty?
        when /^exit_connection$/o
          $stderr.puts "receive exit_connection from worker"
          true
        else
          @writer[io].puts(s)
          nil
        end
      end
    end

    def on_taskend(task_name)
      #puts "taskend: "+task_name
      id = @id_by_taskname.delete(task_name)
      t = Rake.application[task_name].wrapper
      t.postprocess
      @idle_cores[id] += t.n_used_cores
      @exit_task.delete(t.task)
      if @exit_task.empty?
        return true
      end
      wake_idle_core
      nil
    end

    def wake_idle_core
      queued = 0
      while true
        count = 0
        @idle_cores.keys.each do |id|
          if @idle_cores[id] > 0
            if t = @task_queue.deq(@workers[id].host)
              Log.debug "deq: #{t.name}"
              #Log.debug "@task.queue: #{@task_queue.inspect}"
              if @idle_cores[id] < t.n_used_cores
                @task_queue.enq(t)
              else
                t.preprocess
                @idle_cores[id] -= t.n_used_cores
                @id_by_taskname[t.name] = id
                @workers[id].send_cmd("#{id}:#{t.name}")
                count += 1
                queued += 1
              end
            end
          end
        end
        break if count == 0
      end
      if queued>0
        Log.debug "queued:#{queued} @idle_cores:#{@idle_cores.inspect}"
      end
      #Log.debug "wake_idle_core: end"
    end

    def finish
      @task_queue.finish if @task_queue
      @conn_list.each do |conn|
        conn.close
      end
      @dispatcher.event_loop do |io|
        s = io.gets
        if /^branch_end$/o =~ s
          @dispatcher.detach_communicator(@comm_by_io[io])
          @comm_by_io.delete(io)
        end
        @comm_by_io.empty? # exit condition
      end
      @task_logger.close if @task_logger
      Util.dputs "branch:finish"
    end

  end
end
