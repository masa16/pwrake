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
      ios = []

      @option.host_map.each do |sub_host, wk_hosts|
        conn = BranchCommunicator.new(sub_host,@option)
        @conn_list << conn
        @dispatcher.attach_communicator(conn)
        @writer[conn.ior] = $stdout
        @writer[conn.ioe] = $stderr
        conn.send_cmd "begin_worker_list"
        wk_hosts.each do |host_info|
          name = host_info.name
          ncore = host_info.ncore
          $stderr.puts "connecting #{name} ncore=#{ncore}"
          chan = WorkerChannel.new(conn,name,ncore)
          @workers[chan.id] = chan
          host_list << name
          conn.send_cmd "#{chan.id}:#{name} #{ncore}"
          ios << conn.ior
        end
        conn.send_cmd "end_worker_list"
      end

      # receive ncore from WorkerCommunicator at Branch
      IODispatcher.event_once(ios,10) do |io|
        s = io.gets
        if /ncore:(\d+):(\d+)/ =~ s
          id, ncore = $1.to_i, $2.to_i
          @workers[id].ncore = ncore
          @idle_cores[id] = ncore
        end
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
      t = Rake.application[task_name].wrapper
      t.postprocess
      @idle_cores[id] += t.n_used_cores
      if @id_by_taskname.empty? && Rake.application.task_queue.empty?
        puts "End of all tasks"
        return true
      end
      wake_idle_core
      nil
    end

    def wake_idle_core
      while true
        count = 0
        @idle_cores.keys.each do |id|
          if @idle_cores[id] > 0
            if t = @task_queue.deq(@workers[id].host)
              #puts "deq: #{t.name}"
              if @idle_cores[id] < t.n_used_cores
                @task_queue.enq(t)
              else
                t.preprocess
                @idle_cores[id] -= t.n_used_cores
                @id_by_taskname[t.name] = id
                @workers[id].send_cmd("#{id}:#{t.name}")
                count += 1
              end
            end
          end
        end
        break if count == 0
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

