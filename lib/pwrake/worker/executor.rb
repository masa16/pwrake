module Pwrake

  class Executor

    def initialize(selector,dir_class,id)
      @selector = selector
      @id = id
      @out = Writer.instance
      @log = LogExecutor.instance
      @queue = []
      @rd_list = []
      @dir = dir_class.new
      @dir.open
      @dir.open_messages.each{|m| @log.info(m)}
      @out.puts "#{@id}:open"
    end

    def stop
      @stopped = true
      @queue.clear
    end

    def close
      if @thread
        @thread.join(15)
        sleep 0.1
      end
      @thread = Thread.new do
        @dir.close_messages.each{|m| @log.info(m)}
        @dir.close
      end
    rescue => exc
      @log.error(([exc.to_s]+exc.backtrace).join("\n"))
    end

    def join
      if @thread
        @thread.join(15)
      end
    rescue => exc
      @log.error(([exc.to_s]+exc.backtrace).join("\n"))
    end

    def execute(cmd)
      return if @stopped
      @queue.push(cmd)
      start_process
    end

    def start_process
      return if @thread      # running
      command = @queue.shift
      return if !command     # empty queue
      @spawn_in, @sh_in = IO.pipe
      @sh_out, @spawn_out = IO.pipe
      @sh_err, @spawn_err = IO.pipe

      @pid = Kernel.spawn(command,
                          :in=>@spawn_in,
                          :out=>@spawn_out,
                          :err=>@spawn_err,
                          :chdir=>@dir.current,
                          :pgroup=>true
                         )
      @log.info "pid=#{@pid} started. command=#{command.inspect}"

      @thread = Thread.new do
        @pid2,@status = Process.waitpid2(@pid)
        @spawn_in.close
        @spawn_out.close
        @spawn_err.close
      end

      @rd_out = Reader.new(@sh_out,"o")
      @rd_err = Reader.new(@sh_err,"e")
      @rd_list = [@rd_out,@rd_err]

      @selector.add_reader(@sh_out){callback(@rd_out)}
      @selector.add_reader(@sh_err){callback(@rd_err)}
    end

    def callback(rd)
      while s = rd.gets
        @out.puts "#{@id}:#{rd.mode}:#{s.chomp}"
      end
      if rd.eof?
        @selector.delete_reader(rd.io)
        @rd_list.delete(rd)
        if @rd_list.empty?  # process_end
          @thread = @pid = nil
          @log.info inspect_status
          @out.puts "#{@id}:z:#{exit_status}"
          @sh_in.close
          @sh_out.close
          @sh_err.close
          start_process     # next process
        end
      end
    rescue => exc
      @log.error(([exc.to_s]+exc.backtrace).join("\n"))
      stop
    end

    def inspect_status
      s = @status
      case
      when s.signaled?
        if s.coredump?
          "pid=#{s.pid} dumped core."
        else
          "pid=#{s.pid} was killed by signal #{s.termsig}"
        end
      when s.stopped?
        "pid=#{s.pid} was stopped by signal #{s.stopsig}"
      when s.exited?
        "pid=#{s.pid} exited normally. status=#{s.exitstatus}"
      else
        "unknown status %#x" % s.to_i
      end
    end

    def exit_status
      s = @status
      case
      when s.signaled?
        if s.coredump?
          "core_dumped"
        else
          "killed:#{s.termsig}"
        end
      when s.stopped?
        "stopped:#{s.stopsig}"
      when s.exited?
        "#{s.exitstatus}"
      else
        "unknown:%#x" % s.to_i
      end
    end

    def kill(sig)
      stop
      if @pid
        Process.kill(sig,-@pid)
        @log.warn "Executor(id=#{@id})#kill pid=#{@pid} sig=#{sig}"
      end
    end

  end
end
