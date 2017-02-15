require "fileutils"
require "pwrake/logger"
require "pwrake/nbio"
require "pwrake/option/option"
require "pwrake/task/task_wrapper"
require "pwrake/queue/task_queue"
require "pwrake/master/fiber_pool"

module Pwrake

  class Master

    def initialize
      @selector = NBIO::Selector.new
      @hostinfo_by_taskname = {}
      @hdl_set = []
      @channel_by_hostid = {}
      @channels = []
      @hostinfo_by_id = {}
      # init
      @option = Option.new
      Log.set_logger(@option)
      @option.init
      TaskWrapper.init_task_logger(@option)
    end

    attr_reader :task_queue
    attr_reader :option
    attr_reader :thread

    def setup_branch_handler(sub_host)
      ior,w0 = IO.pipe
      r2,iow = IO.pipe
      if sub_host == "localhost" && /^(n|f)/i !~ ENV['T']
        @thread = Thread.new(r2,w0,@option) do |r,w,o|
          Rake.application.run_branch_in_thread(r,w,o)
        end
      else
        dir = File.absolute_path(File.dirname($PROGRAM_NAME))
        cmd = "ssh -x -T -q #{sub_host} '" +
          "cd \"#{Dir.pwd}\";"+
          "PATH=#{dir}:${PATH} exec pwrake_branch'"
        Log.debug("BranchCommunicator cmd=#{cmd}")
        spawn(cmd,:pgroup=>true,:out=>w0,:in=>r2)
        w0.close
        r2.close
        Marshal.dump(@option,iow)
        iow.flush
        s = ior.gets
        if !s or s.chomp != "pwrake_branch start"
          raise RuntimeError,"pwrake_branch start failed: receive #{s.inspect}"
        end
      end
      rd = NBIO::Reader.new(@selector,ior)
      wt = NBIO::Writer.new(@selector,iow)
      return NBIO::Handler.new(rd,wt,sub_host)
    end

    def signal_trap(sig)
      case @killed
      when 0
        # log writing failed. can't be called from trap context
        $stderr.puts "\nSignal trapped. (sig=#{sig} pid=#{Process.pid} ##{@killed})"
        if Rake.application.options.debug
          $stderr.puts "in master thread #{Thread.current}:"
          $stderr.puts caller
          if @thread
            $stderr.puts "in branch thread #{@thread}:"
            $stderr.puts @thread.backtrace.join("\n")
          end
        end
        $stderr.puts "Exiting..."
        @no_more_run = true
        @failed = true
        NBIO::Handler.kill(@hdl_set,sig)
        # @selector.run : not required here
      when 1
        $stderr.puts "\nOnce more Ctrl-C (SIGINT) for exit."
      when 2
        @thread.kill if @thread
      else
        Kernel.exit(false) # must wait for nomral exit
      end
      @killed += 1
    end

    def setup_branches
      sum_ncore = 0
      @option.host_map.each do |sub_host, wk_hosts|
        @hdl_set << hdl = setup_branch_handler(sub_host)
        Fiber.new do
        hdl.put_line "host_list_begin"
        wk_hosts.each do |host_info|
          name = host_info.name
          ncore = host_info.ncore
          host_id = host_info.id
          Log.debug "connecting #{name} ncore=#{ncore} id=#{host_id}"
          hdl.put_line "host:#{host_id} #{name} #{ncore}"
          @channel_by_hostid[host_id] = hdl
          @hostinfo_by_id[host_id] = host_info
        end
        hdl.put_line "host_list_end"
        while s = hdl.get_line
          case s
          when /^ncore:done$/
            break
          when /^ncore:(\d+):(\d+)$/
            id, ncore = $1.to_i, $2.to_i
            Log.debug "worker_id=#{id} ncore=#{ncore}"
            @hostinfo_by_id[id].set_ncore(ncore)
            sum_ncore += ncore
          when /^exited$/
            raise RuntimeError,"Unexpected branch exit"
          else
            msg = "#{hdl.host}:#{s.inspect}"
            raise RuntimeError,"invalid return: #{msg}"
          end
        end
        end.resume
      end
      @selector.run

      Log.info "num_cores=#{sum_ncore}"
      @option.total_cores = sum_ncore
      @hostinfo_by_id.each do |id,host|
        if ncore = @hostinfo_by_id[id].idle_cores
          Log.info "#{host} id=#{id} ncore=#{ncore}"
        else
          @hostinfo_by_id.delete(id)
        end
      end
      if @hostinfo_by_id.empty?
        raise RuntimeError,"no worker host"
      end
      queue_class = Pwrake.const_get(@option.queue_class)
      @task_queue = queue_class.new(@hostinfo_by_id)

      @branch_setup_thread = Thread.new do
        #@channels.each do |chan|
        create_fiber(@hdl_set) do |hdl|
          while s = hdl.get_line
            case s
            when /^retire:(\d+)$/
              retire($1.to_i)
            when /^branch_setup:done$/
              break
            else
              raise RuntimeError,"branch_setup failed: s=#{s.inspect}"
            end
          end
        end
        @selector.run
        @killed = 0
        [:TERM,:INT].each do |sig|
          Signal.trap(sig) do
            signal_trap(sig)
          end
        end
      end

    end

    def retire(hid)
      host_info = @hostinfo_by_id[hid.to_i]
      if host_info && host_info.decrease(1)
        # all retired
        m = "retired: host #{host_info.name}"
        Log.warn(m)
        $stderr.puts(m)
        drop_host(host_info) # delete from hostinfo_by_id
        if @hostinfo_by_id.empty?
          raise RuntimeError,"no worker host"
        end
      end
    end

    def create_fiber(channels,&blk)
      channels.each do |chan|
        fb = Fiber.new(&blk)
        fb.resume(chan)
      end
    end

    def invoke(t, args)
      @failed = false
      t.pw_search_tasks(args)

      if @option['GRAPH_PARTITION']
        setup_postprocess0
        @task_queue.deq_noaction_task do |tw|
          tw.preprocess
          tw.status = "end"
          @post_pool.enq(tw)
        end
        @selector.run
        @post_pool.finish
        Log.debug "@post_pool.finish"

        require 'pwrake/misc/mcgp'
        MCGP.graph_partition(@option.host_map)
      end

      setup_postprocess1
      @branch_setup_thread.join
      send_task_to_idle_core
      #
      n_retry = @option["RETRY"]
      create_fiber(@hdl_set) do |hdl|
        while s = hdl.get_line
          Log.debug "Master:recv #{s.inspect} from branch[#{hdl.host}]"
          case s
          when /^task(\w+):(\d*):(.*)$/o
            status, shell_id, task_name = $1, $2.to_i, $3
            tw = Rake.application[task_name].wrapper
            tw.shell_id = shell_id
            tw.status = status
            host_info = @hostinfo_by_taskname[task_name]
            if host_info.nil?
              m = "unknown hostid: task_name=#{task_name} s=#{s.inspect}"+
                " @hostinfo_by_taskname.keys=#{@hostinfo_by_taskname.keys.inspect}"
              Log.error(m)
              $stderr.puts(m)
            end
            task_end(tw,host_info) # @idle_cores.increase(..
            # check failure
            if tw.status == "fail"
              $stderr.puts %[task "#{tw.name}" failed.]
              if host_info
                continuous_fail = host_info.task_result(tw.status)
                Log.debug "task=#{tw.name} continuous_fail=#{continuous_fail}"
                if continuous_fail > n_retry && @hostinfo_by_id.size > 1
                  # retire this host
                  drop_host(host_info)
                  Log.warn("retired host:#{host_info.name} due to continuous fail")
                end
              end
              if tw.no_more_retry && !@failed
                @failed = true
                case @option['FAILURE_TERMINATION']
                when 'kill'
                  NBIO::Handler.kill(@hdl_set,"INT")
                  @selector.run
                  @no_more_run = true
                  $stderr.puts "... Kill running tasks."
                when 'continue'
                  $stderr.puts "... Continue runable tasks."
                else # 'wait'
                  @no_more_run = true
                  $stderr.puts "... Wait for running tasks."
                end
              end
              if tw.has_output_file? && File.exist?(tw.name)
                handle_failed_target(tw.name)
              end
            end
            # postprocess
            @post_pool.enq(tw) # must be after @no_more_run = true
            break if @finished
          when /^retire:(\d+)$/
            retire($1.to_i)
          when /^exited$/o
            @exited = true
            Log.debug "receive #{s.chomp} from branch"
            break
          else
            Log.error "unknown result: #{s.inspect}"
            $stderr.puts(s)
          end
        end
        Log.debug "Master#invoke: fiber end"
      end
      if !ending?
        Log.debug "@selector.run begin"
        @selector.run
        Log.debug "@selector.run end"
      end
      @post_pool.finish
      Log.debug "Master#invoke: end of task=#{t.name}"
    end

    def send_task_to_idle_core
      #Log.debug "#{self.class}#send_task_to_idle_core start"
      count = 0
      # @idle_cores.decrease(..
      @task_queue.deq_task do |tw,host_info,ncore|
        host_info.busy(ncore)
        count += 1
        @hostinfo_by_taskname[tw.name] = host_info
        tw.preprocess
        if tw.has_action?
          hid = host_info.id
          s = "#{hid}:#{tw.task_id}:#{tw.name}"
          @channel_by_hostid[hid].put_line(s)
          tw.exec_host = host_info.name
        else
          tw.status = "end"
          task_end(tw,host_info) # @idle_cores.increase(..
          @post_pool.enq(tw)
        end
      end
      if count == 0 && !@task_queue.empty? && @hostinfo_by_taskname.empty?
        m="No task was invoked while unexecuted tasks remain"
        Log.error m
        Log.error "@task_queue="+@task_queue.inspect
        raise RuntimeError,m
      end
      #Log.debug "#{self.class}#send_task_to_idle_core end time=#{Time.now-tm}"
    end

    def setup_postprocess
      i = 0
      n = @option.max_postprocess_pool
      @post_pool = FiberPool.new(n) do |pool|
        postproc = @option.postprocess(@selector)
        i += 1
        Log.debug "New postprocess fiber ##{i}"
        Fiber.new do
          j = i
          while tw = pool.deq()
            Log.debug "postproc##{j} deq=#{tw.name}"
            loc = postproc.run(tw)
            tw.postprocess(loc)
            pool.count_down
            @hostinfo_by_taskname.delete(tw.name)
            tw.retry_or_subsequent unless @exited
            break if yield(pool,j)
          end
          postproc.close
          Log.debug "postproc##{j} end"
        end
      end
    end

    def task_end(tw,host_info)
      if host_info && host_info.idle(tw.n_used_cores(host_info))
        # all retired
        Log.warn("retired host:#{host_info.name} because all core retired")
        drop_host(host_info)
      end
    end

    def setup_postprocess0
      setup_postprocess{false}
    end

    def setup_postprocess1
      setup_postprocess do |pool,j|
        #Log.debug " pool.empty?=#{pool.empty?}"
        if ending?
          Log.debug "postproc##{j} closing"
          @finished = true
          @selector.halt
          true
        elsif !@no_more_run
          send_task_to_idle_core
          false
        end
      end
    end

    def ending?
      #Log.debug " @no_more_run=#{@no_more_run.inspect}"
      #Log.debug " @task_queue.empty?=#{@task_queue.empty?}"
      #Log.debug " @hostinfo_by_id.empty?=#{@hostinfo_by_id.empty?}"
      #Log.debug " @hostinfo_by_taskname.keys=#{@hostinfo_by_taskname.keys.inspect}"
      (@no_more_run || @task_queue.empty? || @hostinfo_by_id.empty?) &&
        @hostinfo_by_taskname.empty?
    end

    def handle_failed_target(name)
      case @option['FAILED_TARGET']
        #
      when /rename/i, NilClass
        dst = name+"._fail_"
        ::FileUtils.mv(name,dst)
        msg = "Rename output file '#{name}' to '#{dst}'"
        $stderr.puts(msg)
        Log.warn(msg)
        #
      when /delete/i
        ::FileUtils.rm(name)
        msg = "Delete output file '#{name}'"
        $stderr.puts(msg)
        Log.warn(msg)
        #
      when /leave/i
      end
    end

    def drop_host(host_info)
      Log.debug "drop_host: #{host_info.name}"
      hid = host_info.id
      if @hostinfo_by_id[hid]
        s = "drop:#{hid}"
        @channel_by_hostid[hid].put_line(s)
        @task_queue.drop_host(host_info)
        @hostinfo_by_id.delete(hid)
        if @hostinfo_by_id.empty?
          if @finished
            Log.debug "drop_host: @finished and @hostinfo_by_id.empty?"
          else
            Log.error "drop_host: All workers retired."
            $stderr.puts "All workers retired."
            @failed = true
          end
        end
      end
    end

    def finish
      Log.debug "Master#finish begin"
      @branch_setup_thread.join
      # continues running fibers
      Log.debug "Master#finish @selector.run begin"
      @selector.run(60)
      Log.debug "Master#finish @selector.run end"
      if !@exited
        @exited = true
        Log.debug "Master#finish Handler.exit begin"
        NBIO::Handler.exit(@hdl_set)
        @selector.run(60)
        Log.debug "Master#finish Handler.exit end"
      end
      TaskWrapper.close_task_logger
      Log.debug "Master#finish end"
      @failed
    end

  end
end
