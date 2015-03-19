require "pp"

module Pwrake

  class Branch

    def initialize(opts,r,w)
      @options = opts
      @queue = FiberQueue.new
      @timeout = 10
      @exit_cmd = "exit_connection"
      @ioevent = IOEvent.new
      @ior = r
      @iow = w
      init
    end

    def init
      # setup_options
      # pp @options
      set_env
      setup_filesystem
    end

    # Rakefile is loaded after 'init' before 'run'

    def run
      begin
        begin
          setup_workers
          setup_fibers
          execute
        rescue => e
          $stderr.puts e.message
          $stderr.puts e.backtrace
        end
      ensure
        finish
      end
    end

    def set_env
      case envs = @options['PASS_ENV']
      when Hash
        envs.each do |k,v|
          ENV[k] = v
        end
      end
    end

    def setup_filesystem
      @cwd = @options['DIRECTORY']
      fs = @options['FILESYSTEM']
      #puts "fs=#{fs}"
      case fs
      when /gfarm/io
        require 'pwrake/gfarm'
        @fs = GfarmPath.new
        @fs.chdir(@cwd)
        # Dir.chdir(@cwd)
      when /nfs/io
        Dir.chdir(@cwd)
      when /local/io
        Dir.chdir(@cwd)
      else
        raise "unknown filesystem: #{fs}"
      end
      # p Dir.pwd
    end


    def setup_workers
      s = @ior.gets
      raise if s.chomp != "begin_worker_list"

      while s = @ior.gets
        s.chomp!
        break if s == "end_worker_list"
        if /^(\d+):(\S+) (\d+)?$/ =~ s
          id, host, ncore = $1,$2,$3
          ncore = ncore.to_i if ncore
          dir = File.absolute_path(File.dirname($PROGRAM_NAME))
          cmd = "ssh -x -T -q #{host} '"+
            "PATH=#{dir}:${PATH} exec pwrake_worker #{id} #{ncore}'"
          cmd = "PATH=#{dir}:${PATH} pwrake_worker #{id} #{ncore}"
          conn = Communicator.new(host,cmd,ncore)

          #Marshal.dump(@wk_opt,conn.iow)

          @ioevent.add_io(conn.ior,conn)
        else
          raise "invalid workers"
        end
      end

      @ioevent.event_each do |conn,s|
        if /ncore:(\d+)/ =~ s
          conn.ncore = $1.to_i
        end
      end

      if !@ioevent.closed.empty?
        raise "Error in communicator setup from Branch to Worker"
      end

      if pass_env = @options['PASS_ENV']
        @ioevent.each do |conn|
          pass_env.each do |k,v|
            conn.send_cmd "export:#{k}=#{v}"
          end
        end
      end

      if @options['FILESYSTEM']=='gfarm'
        gfarm = true
      end

      @ioevent.each do |conn|
        if gfarm
          #puts "fs:gfarm"
          conn.send_cmd "fs:gfarm"
        end
        #puts "cd:#{@cwd}"
        conn.send_cmd "cd:#{@cwd}"
      end
    end

    def setup_fibers
      fiber_list = []

      @ioevent.each do |conn|
        conn.ncore.times do

          f = Fiber.new do
            chan = Channel.new(conn)
            while task = @queue.deq
              task.execute
              @queue.release(task.resource)
              @iow.puts "taskend:#{task.name}"
              @iow.flush
            end
            chan.close
          end

          fiber_list.push f
        end
      end

      fiber_list.each{|f| f.resume}
      @ioevent.each{|conn| conn.send_cmd "start:"}
    end

    def execute
      tasks = []

      @ioevent.add_io(@ior)
      @ioevent.event_loop do |conn,s|
        s.chomp!
        s.strip!

        if conn==@ior

          # receive command from main pwrake
          case s

          when /^(\d+):(.+)$/o
            id, tname = $1,$2
            task = Rake.application[tname]
            tasks.push(task)

          when /^end_task_list$/o
            @queue.enq(tasks)
            tasks.clear

          when /^exit_connection$/o
            Util.dputs "branch:exit_connection"
            break

          when /^kill:(.*)$/o
            sig = $1
            # Util.puts "branch:kill:#{sig}"
            Communicator.kill(sig)
            Kernel.exit

          else
            @iow.puts "unknown command:#{s.inspect}"
          end

        else

          # read from worker
          if !Channel.check_line(s)
            @iow.puts '??:'+s
          end
        end
      end
      @queue.finish
    end

    def finish
      @ioevent.delete_io(@ior)
      @ioevent.each do |conn|
        conn.close
      end
      @ioevent.each_io do |io|
        while s=io.gets
          if !Channel.check_line(s)
            Util.print '?e:'+s
          end
        end
      end
      Util.dputs "branch:finish"
      @iow.close
      @ior.close
    end

  end # Branch
end # Pwrake
