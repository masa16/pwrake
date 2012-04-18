require "pp"

module Pwrake

  class Branch

    def initialize
      @queue = FiberQueue.new
      @timeout = 10
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

      s = $stdin.gets
      raise if s.chomp != "begin_worker_list"
      while s = $stdin.gets
        s.chomp!
        break if s == "end_worker_list"
        if /^(\d+):(\S+) (\d+)?$/ =~ s
          id, host, ncore = $1,$2,$3
          ncore = ncore.to_i if ncore
          conn = WorkerConnection.new(id,host,ncore)
          @ioevent.add_io(conn.io,conn)
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

      # Util.puts "end of ncore:"
      @list = []

      i=0
      @ioevent.each do |conn|
        conn.ncore.times do
          fb_idx = i
          f = Fiber.new do
            chan = Channel.new(conn.io)
            while task = @queue.deq
              #Util.puts "deq:#{task.name} fiber:#{fb_idx}"
              task.execute
              @queue.release(task.resource)
              # Util.puts "task end:#{task.name} fiber:#{i}"
              Util.puts "taskend:#{task.name}"
            end
            chan.close
            Util.puts "fiber end"
          end
          # Util.puts "fiber.id = #{f.object_id}"
          i += 1
          @list.push f
        end
      end

      @list.each{|f| f.resume}
      #Util.puts "end connect"

      @ioevent.each{|conn| conn.io.puts "start:"}

      @ioevent.add_io($stdin)
    end

    def execute
      tasks = []

      @ioevent.event_loop do |conn,s|
        s = s.chomp
        # Util.puts "conn=#{conn.inspect} s=#{s.inspect}"

        if conn==$stdin
          case s
          when /exit_branch/
            break
          when /end_task_list/
            #Util.puts "enq"
            @queue.enq(tasks)
            tasks.clear
          when /^(\d+):(.+)$/
            id, tname = $1,$2
            #Util.puts "id=#{id} task=#{tname}"
            task = Rake.application[tname]
            tasks.push task
          else
            #Util.puts "invalid command:#{s.inspect}"
          end
        else
          # Util.puts "#{[conn,s]}\n"
          if !Channel.check_line(s)
            Util.puts '--'+s
          end
        end
      end

      @ioevent.each do |conn|
        conn.io.puts "exit:"
      end

      @queue.finish
    end

  end # Branch
end # Pwrake
