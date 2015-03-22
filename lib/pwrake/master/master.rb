module Pwrake

  class Master

    def initialize
      setup_options
      @dispatcher = IODispatcher.new
      @id_by_taskname = {}
      @idle_cores = {}
      @workers = {}
    end

    def init(hosts=nil)
      setup_pass_env
      setup_filesystem

      if hosts
        hosts = hosts.dup
      else
        hosts = YAML.load(open(@confopt['HOSTFILE']))
        #hosts = YAML.load(open("../../../test/hosts.yaml"))
      end
      if hosts.kind_of? Hash
        hosts = [hosts]
      end
      @host_map = parse_hosts(hosts)
      Util.dputs "@host_map=#{@host_map.inspect}"
    end

    def parse_hosts(hosts)
      host_map = {}
      hosts.each do |a|
        a.each do |sub_host,wk_hosts|
          list = host_map[sub_host] || []
          wk_hosts.each do |s|
            h, ncore = s.split
            ncore = ncore.to_i if ncore
            if /(.*)\[([^-]+)(?:-|\.\.)([^-]+)\](.*)/o =~ h
              range = ($1+$2+$4)..($1+$3+$4)
            else
              range = h..h
            end
            range.each do |host|
              list << [host,ncore]
            end
          end
          host_map[sub_host] = list
        end
      end
      host_map
    end

    def setup_branches
      @conn_list = []
      host_list = []

      @host_map.each do |sub_host, wk_hosts|
        conn = BranchCommunicator.new(sub_host,@confopt)
        @conn_list << conn
        @dispatcher.attach_read(conn.ior)
        wk_hosts.each do |host,ncore,|
          # puts "connecting #{host} #{ncore}"
          chan = WorkerChannel.new(conn,host,ncore)
          @workers[chan.id] = chan
          @idle_cores[chan.id] = chan.ncore
          host_list << host
        end
        conn.send_cmd "end_worker_list"
      end

      @task_queue = TaskQueue.new(host_list)
    end

    attr_reader :task_queue

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
          Util.puts s
          nil
        end
      end
    end

    def on_taskend(task_name)
      puts "taskend:"+task_name
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
          puts "deq: #{t.name}"
          if @idle_cores[id] < t.n_used_cores
            @task_queue.enq(t)
          else
            @idle_cores[id] -= t.n_used_cores
            @id_by_taskname[t.name] = id
            @workers[id].send_cmd("#{id}:#{t.name}")
            @workers[id].send_cmd("end_task_list")
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
      @role.task_queue
    end
  end
end

