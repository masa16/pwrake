module Pwrake

  class Branch

    def initialize(opts,r,w)
      @options = opts
      @queue = FiberQueue.new
      @timeout = 10
      @exit_cmd = "exit_connection"
      @shells = []
      @ior = r
      @iow = w
      @wk_comm = {}
    end

    # Rakefile is loaded after 'init' before 'run'

    def run
      begin
        begin
          @dispatcher = IODispatcher.new
          setup_shells
          @dispatcher.event_loop
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


    def setup_shells
      s = @ior.gets
      raise if s.chomp != "begin_worker_list"

      if fn = @options["PROFILE"]
        Shell.profiler.open(fn,@options['GNU_TIME'],@options['PLOT_PARALLELISM'])
      end

      ios = []
      while s = @ior.gets
        s.chomp!
        #p s
        break if s == "end_worker_list"
        if /^(\d+):(\S+) (\d+)?$/ =~ s
          id, host, ncore = $1,$2,$3
          if ncore
            ncore = ncore.to_i
          else
            ncore = 1
          end
          comm = WorkerCommunicator.new(id,host,ncore)
          @wk_comm[comm.ior] = comm
          @dispatcher.attach_communicator(comm)
          ios << comm.ior
        end
      end

      timeout = 10
      while !ios.empty? and io_sel = select(ios,nil,nil,timeout)
        for io in io_sel[0]
          if io.eof?
            break
          else
            if /ncore:(\d+)/ =~ s
              @wk_comm[io].set_ncore($1.to_i)
            end
          end
          ios.delete(io)
        end
      end
      if !ios.empty?
        raise RuntimeError, "Connection error: from Branch to Worker"
      end

      @shells = []
      @wk_comm.each_value do |comm|
        comm.ncore.times do
          @shells << @options.shell_class.new(comm,@options.shell_option)
        end
      end

      @fiber_list = @shells.map do |shell|
        Fiber.new do
          shell.start
          while task = @queue.deq
            #p task
            task.pw_execute
            #@queue.release(task.resource)
            @iow.puts "taskend:#{task.name}"
            @iow.flush
          end
          shell.close
        end
      end

      @fiber_list.each{|f| f.resume}

      bh = BranchHandler.new(@queue)
      @dispatcher.attach_handler(@ior,bh)

      # if @options['FILESYSTEM']=='gfarm'
      #   gfarm = true
      # end
      #
      # @ioevent.each do |conn|
      #   if gfarm
      #     #puts "fs:gfarm"
      #     conn.send_cmd "fs:gfarm"
      #   end
      #   #puts "cd:#{@cwd}"
      #   conn.send_cmd "cd:#{@cwd}"
      # end
    end

    def finish
      #@ioevent.delete_io(@ior)
      #@ioevent.each do |conn|
      #  conn.close
      #end
      #@ioevent.each_io do |io|
      #  while s=io.gets
      #    if !Channel.check_line(s)
      #      Util.print '?e:'+s
      #    end
      #  end
      #end
      @shells.each do |shl|
        shl.close
      end
      Util.dputs "branch:finish"
      @iow.close
      @ior.close
    end

  end # Pwrake::Branch
end
