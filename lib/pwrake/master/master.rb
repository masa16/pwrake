module Pwrake

  class Master

    def initialize
      @dispatcher = IODispatcher.new
      @id_by_taskname = {}
      @idle_cores = {}
      @workers = {}
      @writer = {}
      @option = Option.new
      init_logger(@option['LOGFILE'])
    end

    attr_reader :task_queue
    attr_reader :option

    def init(hosts=nil)
      @option.init
    end

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

    attr_reader :logger

    def init_tasklog
      if @tasklog
        @task_logger = File.open(@tasklog,'w')
        h = %w[
          task_id task_name start_time end_time elap_time preq preq_host
          exec_host shell_id has_action executed file_size file_mtime file_host
        ].join(',')+"\n"
        @task_logger.print h
      end
    end


    def setup_branches
      @conn_list = []
      host_list = []

      @option.host_map.each do |sub_host, wk_hosts|
        conn = BranchCommunicator.new(sub_host,@option)
        @conn_list << conn
        @dispatcher.attach_read(conn.ior)
        @writer[conn.ior] = $stdout
        @dispatcher.attach_read(conn.ioe)
        @writer[conn.ioe] = $stderr
        conn.send_cmd "begin_worker_list"
        wk_hosts.each do |host_info|
          $stderr.puts "connecting #{host_info.name} #{host_info.ncore}"
          chan = WorkerChannel.new(conn,host_info.name,host_info.ncore)
          @workers[chan.id] = chan
          @idle_cores[chan.id] = chan.ncore
          host_list << host_info.name
          conn.send_cmd "#{chan.id}:#{chan.host} #{chan.ncore}"
        end
        conn.send_cmd "end_worker_list"
      end

      @task_queue = @option.queue_class.new(host_list)
    end

    def finish
      @task_queue.finish if @task_queue
      Util.dputs "main:exit_branch"
      BranchCommunicator.close_all
      @conn_list.each do |conn|
        while s=conn.gets
          Util.print s
        end
      end
      Util.dputs "branch:finish"
    end

    def invoke(t, args)
      t.pw_search_tasks(args)
      wake_idle_core
      @dispatcher.event_loop do |io|
        s = io.gets
        s.chomp!
        case s
        when /^taskend:(.*)$/o
          on_taskend($1)
        when /^exit_connection$/o
          p s
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
      t = Rake.application[task_name]
      t.pw_enq_subsequents
      @idle_cores[id] += t.n_used_cores
      if @id_by_taskname.empty? && Rake.application.task_queue.empty?
        puts "End of all tasks"
        return true
      end
      wake_idle_core
      nil
    end

    def wake_idle_core
      @idle_cores.keys.each do |id|
        if t = @task_queue.deq(@workers[id].host)
          #puts "deq: #{t.name}"
          if @idle_cores[id] < t.n_used_cores
            @task_queue.enq(t)
          else
            @idle_cores[id] -= t.n_used_cores
            @id_by_taskname[t.name] = id
            @workers[id].send_cmd("#{id}:#{t.name}")
          end
        end
      end
    end

  end
end

module Rake
  class Task
    def n_used_cores
      1
    end
  end
  class Application
    def task_queue
      @master.task_queue
    end
  end
end

