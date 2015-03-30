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
          setup_fibers
          @dispatcher.event_loop
        rescue => e
          $stderr.puts "Error!!!"
          $stderr.puts e.message
          $stderr.puts e.backtrace
        end
      ensure
        finish
      end
    end


    def setup_shells
      s = @ior.gets
      raise if s.chomp != "begin_worker_list"

      if fn = @options["PROFILE"]
        Shell.profiler.open(fn,@options['GNU_TIME'],@options['PLOT_PARALLELISM'])
      end

      while s = @ior.gets
        s.chomp!
        break if s == "end_worker_list"
        if /^(\d+):(\S+) (\d+)?$/ =~ s
          id, host, ncore = $1,$2,$3
          ncore &&= ncore.to_i
          comm = WorkerCommunicator.new(id,host,ncore,@options.worker_option)
          @wk_comm[comm.ior] = comm
          @dispatcher.attach_communicator(comm)
        end
      end

      # receive ncore from worker node
      IODispatcher.event_once(@wk_comm.keys,90) do |io|
        s = io.gets
        if /ncore:(\d+)/ =~ s
          @wk_comm[io].set_ncore($1.to_i)
        end
      end

      @shells = []
      @wk_comm.each_value do |comm|
        # set WorkerChannel#ncore at Master
        @iow.puts "ncore:#{comm.id}:#{comm.ncore}"
        @iow.flush
        comm.ncore.times do
          @shells << @options.shell_class.new(comm,@options.worker_option)
        end
      end
    end

    def setup_fibers
      @fiber_list = @shells.map do |shell|
        Fiber.new do
          shell.start
          while task = @queue.deq
            #$stderr.puts "task=#{task.name} @queue=#{@queue.inspect} fiber=#{Fiber.current.inspect}"
            begin
              task.execute
            rescue Exception=>e
              if task.kind_of?(Rake::FileTask) && File.exist?(task.name)
                failprocess(task.name)
              end
              raise e
            end
            @iow.puts "taskend:#{task.name}"
            @iow.flush
          end
          shell.close
          # if comm is no longer used, close comm
          comm = shell.communicator
          if comm.channel_empty?
            comm.close
          end
        end
      end

      bh = BranchHandler.new(@queue)
      @dispatcher.attach_handler(@ior,bh)

      @fiber_list.each{|fb| fb.resume}
    end

    def failprocess(name)
      case @options['FAILED_TARGET']
      when /rename/i, NilClass
        dst = name+"._fail_"
        ::FileUtils.mv(name,dst)
        msg = "Rename failed target file '#{name}' to '#{dst}'"
        $stderr.puts(msg)
      when /delete/i
        ::FileUtils.rm(name)
        msg = "Delete failed target file '#{name}'"
        $stderr.puts(msg)
      when /leave/i
      end
    end

    def finish
      @iow.puts "branch_end"
      @iow.flush
      @ior.close
      @iow.close
    end

  end # Pwrake::Branch
end
