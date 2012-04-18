require "pp"

module Pwrake

  class Branch

    def initialize
      @queue = FiberQueue.new
      @timeout = 10
      @exit_cmd = "exit:"
    end

    def run
      connect
      execute
    end

    def connect
      # from ~/2012/0410/test.rb
      # get host list from master
      # @hosts = ["local1 2","local2 2"]

      # get host list from master
      # @hosts = Marshal.load($stdin)
      # puts "Branch#connect @hosts=#{@hosts.inspect}"

      @ioevent = IOEvent.new
      @conn_list = []

      s = $stdin.gets
      raise if s.chomp != "begin_worker_list"
      while s = $stdin.gets
        s.chomp!
        break if s == "end_worker_list"
        if /^(\d+):(\S+) (\d+)?$/ =~ s
          id, host, ncore = $1,$2,$3
          ncore = ncore.to_i if ncore
          conn = WorkerConnection.new(id,host,ncore)
          @conn_list.push(conn)
          @ioevent.add_io(conn.ior,conn)
        else
          raise "invalid workers"
        end
      end

      @ioevent.event_each do |conn,s|
        if /ncore:(\d+)/ =~ s
          conn.ncore = $1.to_i
          #Util.puts "ncore:#{conn.host}:#{conn.ncore}"
        end
      end

      #Util.puts "end of ncore:"
      @list = []

      i=0
      @ioevent.each do |conn|
        conn.ncore.times do
          fb_idx = i

          f = Fiber.new do
            chan = Channel.new(conn.iow)
            while task = @queue.deq
              # Util.puts "deq:#{task.name} fiber:#{fb_idx}"
              task.execute
              @queue.release(task.resource)
              # Util.puts "task end:#{task.name} fiber:#{i}"
              Util.puts "taskend:#{task.name}"
            end
            chan.close
            #Util.dputs "fiber end"
          end

          # Util.puts "fiber.id = #{f.object_id}"
          i += 1
          @list.push f
        end
      end

      @list.each{|f| f.resume}
      #Util.puts "end connect"

      @ioevent.each{|conn| conn.send_cmd "start:"}

    end

    def execute
      tasks = []

      @ioevent.add_io($stdin)
      @ioevent.event_loop do |conn,s|
        s.chomp!
        s.strip!
        # Util.dputs "conn=#{conn.inspect} s=#{s.inspect}"

        if conn==$stdin

          # read from main pwrake
          case s
          when /^(\d+):(.+)$/o
            id, tname = $1,$2
            #Util.puts "id=#{id} task=#{tname}"
            task = Rake.application[tname]
            tasks.push task
          when /^end_task_list$/o
            #Util.puts "enq"
            @queue.enq(tasks)
            tasks.clear
          when /^exit_branch$/o
            Util.dputs "branch:exit_branch"
            break
          when /^kill:(.*)$/o
            self.kill($1)
          else
            Util.puts "unknown command:#{s.inspect}"
          end

        else

          # read from worker
          if !Channel.check_line(s)
            Util.puts '??:'+s
          end
        end
      end
      @ioevent.delete_io($stdin)
      @queue.finish
    end

    def kill(sig)
      @ioevent.delete_io($stdin)
      @exit_cmd = "kill:#{sig}"
      Kernel.exit
    end

    def finish
      @ioevent.each do |conn|
        conn.send_cmd @exit_cmd if conn.respond_to?(:send_cmd)
      end
      @ioevent.each_io do |io|
        while s=io.gets
          if !Channel.check_line(s)
            Util.print '?e:'+s
          end
        end
      end
      Util.dputs "branch:finish"
    end

  end # Branch
end # Pwrake
