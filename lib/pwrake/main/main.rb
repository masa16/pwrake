require "yaml"

module Pwrake

  class Main

    DEFAULT_CONFFILES = ["pwrake_conf.yaml"]
    DEFAULT_CONF = {
      'HOSTFILE'=>'hosts.yaml'
    }

    def option(key)
      ENV[key] || @confopt[key] || DEFAULT_CONF[key]
    end

    def initialize(hosts=nil)
      @main_host = `hostname -f`.chomp

      @conffile = DEFAULT_CONFFILES.find{|fn| File.exist?(fn)}
      if @conffile.nil?
        raise "pwrake_conf.yaml not found"
      end
      Util.dputs "@conffile=#{@conffile}"
      @confopt = YAML.load(open(@conffile))

      @hostfile = option('HOSTFILE')
      if hosts
        @hosts = hosts.dup
      else
        @hosts = YAML.load(open(@hostfile))
      end
      if @hosts.kind_of? Hash
        @hosts = [@hosts]
      end
      Util.dputs "@hosts=#{@hosts.inspect}"

      @logfile = option('LOGFILE')

      @branch_set = []
      @worker_set = []

      @scheduler = RoundRobinScheduler.new
      @tracer = Tracer.new

      @ioevent = IOEvent.new
      @task_set = {}
    end

    def setup_branches
      @hosts.each do |a|
        a.each do |sub_host,wk_hosts|
          cmd = "ssh -x -T -q #{sub_host} 'cd #{Dir.pwd};" +
            "exec ./pwrake_branch -t'"
          conn = Connection.new(sub_host,cmd)
          @ioevent.add_io(conn.ior,conn)
          conn.send_cmd "begin_worker_list"
          wk_hosts.map do |s|
            host, ncore = s.split
            ncore = ncore.to_i if ncore
            wk = WorkerChannel.new(conn.iow,host,ncore)
            @worker_set.push(wk)
            wk.send_worker
          end
          conn.send_cmd "end_worker_list"
        end
      end
    end

    def invoke(root, args)
      while tasks = @tracer.fetch_tasks(root)
        break if tasks.empty?

        task_hash = {}
        tasks.each{|t| task_hash[t.name]=t}

        # scheduling
        @scheduler.assign(tasks,@worker_set)

        # send tasks
        @worker_set.each do |wk|
          wk.send_tasks
        end

        @ioevent.each do |conn|
          conn.send_cmd "end_task_list"
        end

        #$stderr.puts "send task: #{Time.now-t} sec"
        #t= Time.now

        # event loop
        @ioevent.event_loop do |conn,s|
          s.chomp!
          if /^taskend:(.*)$/o =~ s
            task_name = $1
            if t = task_hash.delete(task_name)
              t.already_invoked = true
            end
            break if task_hash.empty?
          else
            Util.puts s
          end
        end
      end
    end


    def finish
      Util.dputs "main:exit_branch"
      @ioevent.each do |conn|
        conn.close if conn # finish if conn.respond_to?(:finish)
      end
      @ioevent.each_io do |io|
        while s=io.gets
          Util.print s
        end
      end
      Util.dputs "branch:finish"

      # @ioevent.finish "exit_branch"
    end

  end

end
