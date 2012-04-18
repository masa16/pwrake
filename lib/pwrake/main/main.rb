require "yaml"

module Pwrake

  class Main

    def initialize(hosts=nil)
      @main_host = `hostname -f`.chomp
      if hosts
        @hosts = hosts.dup
      else
        @hosts = YAML.load(open("rhosts.yaml"))
        Util.dputs "@hosts=#{@hosts.inspect}"
      end
      if @hosts.kind_of? Hash
        @hosts = [@hosts]
      end
      @branch_set = []
      @worker_set = []
      @conn_set = []

      @scheduler = RoundRobinScheduler.new
      @tracer = Tracer.new
    end

    def connect
      @ioevent = IOEvent.new

      @hosts.each do |a|
        a.each do |sub_host,wk_hosts|
          conn = BranchConnection.new(sub_host,wk_hosts)
          @conn_set.push(conn)
          @ioevent.add_io(conn.io,conn)
          conn.send "begin_worker_list"
          wk_hosts.map do |s|
            host, ncore = s.split
            ncore = ncore.to_i if ncore
            wk = WorkerChannel.new(conn.io,host,ncore)
            @worker_set << wk
            wk.send_worker
          end
          conn.send "end_worker_list"
        end
      end
      @task_set = {}
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
          conn.send "end_task_list"
        end

        #$stderr.puts "send task: #{Time.now-t} sec"
        #t= Time.now

        # event loop
        @ioevent.event_loop do |conn,s|
          s = s.chomp
          # print "|#{s}\n"
          if /^taskend:(.*)$/o =~ s
            task_name = $1
            if t = task_hash.delete(task_name)
              t.already_invoked = true
            end
            # print "task_hash=#{task_hash.inspect}\n"
            break if task_hash.empty?
          else
            Util.puts s
          end
        end

      end
    end

    def finish
      Util.dputs "main:exit_branch"
      @ioevent.finish "exit_branch"

      #@ioevent.each do |conn|
      #  conn.send "exit_branch"
      #end
      #@ioevent.event_loop do |conn,s|
      #  Util.print s
      #end
      # @conn_set.each{|conn| conn.close}
    end

  end

end
