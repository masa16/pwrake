require 'pwrake/branch/shell_profiler'

module Pwrake

  class DummyMutex
    def synchronize
      yield
    end
  end

  class Shell

    OPEN_LIST={}
    BY_FIBER={}
    @@profiler = ShellProfiler.new

    def self.profiler
      @@profiler
    end

    def self.current
      BY_FIBER[Fiber.current]
    end

    def initialize(chan,comm,task_q,opt={})
      @chan = chan
      @id   = chan.id
      @host = chan.host
      @comm = comm
      @task_q = task_q
      @lock = DummyMutex.new
      @option = opt
      @work_dir = @option[:work_dir] || Dir.pwd
      @comm.shells[self] = true
    end

    attr_reader :id, :host, :status, :profile

    def open
      if @opened
        Log.warn "already opened: host=#{@host} id=#{@id}"
        return
      end
      @opened = true
      _puts("open")
      if (s = _gets) == "open"
        OPEN_LIST[__id__] = self
        true
      else
        Log.error("Shell#open failed: recieve #{s.inspect}")
        false
      end
    end

    def exit
      if !@opened
        Log.debug "already exited: host=#{@host} id=#{@id}"
        return
      end
      @opened = false
      _puts("exit")
      if (s = _gets) == "exit"
        OPEN_LIST.delete(__id__)
        Log.debug("Shell#exit: recieve #{s.inspect}")
        true
      else
        Log.debug("Shell#exit: recieve #{s.inspect}")
        false
      end
    rescue IOError,Errno::EPIPE => e
      Log.debug("Shell#exit: #{Log.bt(e)}")
      false
    end

    def backquote(*command)
      command = command.join(' ')
      @lock.synchronize do
        a = []
        _execute(command){|x| a << x}
        a.join("\n")
      end
    end

    def system(*command)
      command = command.join(' ')
      @lock.synchronize do
        _execute(command){|x| print x+"\n"}
      end
      @status == 0
    end

    def cd(dir="")
      _system("cd #{dir}") or die
    end

    def die
      raise "Failed at #{@host}, id=#{@id}, cmd='#{@cmd}'"
    end

    at_exit {
      Shell.profiler.close
    }

    private

    def _puts(s)
      #Log.debug "Shell#_puts(host=#{@host},id=#{@id}): #{s.inspect}"
      @chan.put_line(s)
    end

    def _gets
      s = @chan.get_line
      Log.debug "Shell#_gets(host=#{@host},id=#{@id}): #{s.inspect}"
      case s
      when Exception
        @chan.halt
        Log.error Log.bt(s)
      end
      s
    end

    def _system(cmd)
      @cmd = cmd
      @lock.synchronize do
        _puts(cmd)
        status = io_read_loop{}
        Integer(status||1) == 0
      end
    end

    def _backquote(cmd)
      @cmd = cmd
      a = []
      @lock.synchronize do
        _puts(cmd)
        @status = io_read_loop{|x| a << x}
      end
      a.join("\n")
    end

    def _execute(cmd,quote=nil,&block)
      @cmd = cmd
      if !@opened
        raise "non opened"
      end
      @status = nil
      start_time = Time.now
      begin
        _puts(cmd)
        @status = io_read_loop(&block)
      ensure
        end_time = Time.now
        @status = @@profiler.profile(@task_id, @task_name, cmd,
                                     start_time, end_time, host, @status)
      end
    end

    def io_read_loop
      while s = _gets
        case s
        when /^(\w+):(.*)$/
          x = [$1,$2]
          case x[0]
          when "o"
            yield x[1]
            next
          when "e"
            $stderr.print x[1]+"\n"
            next
          when "z"
            # see Executor#status_to_str
            status = x[1]
            case status
            when /^\d+$/
              status = status.to_i
            end
            return status
          when "err"
            # see Executor#status_to_str
            status = x[1]
            case status
            when /^\d+$/
              status = status.to_i
            end
            return status
          end
        when "exit"
          msg = "Shell#io_read_loop: exit"
          $stderr.puts(msg)
          Log.error(msg)
          @exited = true
          @chan.halt
          return "exit"
        when IOError
          @exited = true
          @chan.halt
          return "ioerror"
        when NBIO::TimeoutError
          @exited = true
          @chan.halt
          return "timeout"
        end
        msg = "Shell#io_read_loop: Invalid result: #{s.inspect}"
        $stderr.puts(msg)
        Log.error(msg)
      end
    end

    public

    def create_fiber(master_w)
      @master_w = master_w
      if !@opened
        Log.warn "not opened: host=#{@host} id=#{@id}"
      end
      Fiber.new do
        BY_FIBER[Fiber.current] = self
        Log.debug "shell start id=#{@id} host=#{@host}"
        begin
          while task_str = @task_q.deq
            #Log.debug "task_str=#{task_str}"
            if /^(\d+):(.*)$/ =~ task_str
              task_id, task_name = $1.to_i, $2
            else
              raise RuntimeError, "invalid task_str: #{task_str}"
            end
            @task_id = task_id
            @task_name = task_name
            task = Rake.application[task_name]
            begin
              task.execute(task.arguments) if task.needed?
              result = "taskend:#{@id}:#{task.name}"
            rescue Exception=>e
              Rake.application.display_error_message(e)
              Log.error e
              result = "taskfail:#{@id}:#{task.name}"
              break if @exited
            ensure
              master_w.put_line result
            end
          end
          Log.debug "shell id=#{@id} fiber end"
          master_w.put_line "retire:#{@comm.id}"
          @comm.shells.delete(self)
          exit
          if @comm.shells.empty?
            @comm.dropout
          end
          @chan.halt
        rescue => e
          m = Log.bt(e)
          #$stderr.puts m
          Log.error(m)
        end
      end
    end

    def finish_task_q
      @task_q.finish
      #Log.debug "finish_task_q: @task_q=#{@task_q.inspect}"
      while task_str = @task_q.deq_nonblock
        if /^(\d+):(.*)$/ =~ task_str
          task_id, task_name = $1.to_i, $2
        else
          raise RuntimeError, "invalid task_str: #{task_str}"
        end
        @master_w.put_line "taskfail:#{@id}:#{task_name}"
        Log.warn "unexecuted task: #{result}"
      end
      @chan.halt
    end

  end
end
