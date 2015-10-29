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

    def initialize(chan,task_q,opt={})
      @chan = chan
      @host = chan.handler.host
      @task_q = task_q
      @lock = DummyMutex.new
      @id = chan.id
      #
      @option = opt
      @work_dir = @option[:work_dir] || Dir.pwd
    end

    attr_reader :id, :host, :status, :profile

    def open
      if @opened
        Log.warn "already opened: host=#{@host} id=#{@id}"
      end
      _puts("open")
      if (s = _gets) == "open"
        OPEN_LIST[__id__] = self
        @opened = true
        true
      else
        Log.error("Shell#open failed: recieve #{s.inspect}")
        false
      end
    end

    def close
      if !@opened
        Log.warn "already closed: host=#{@host} id=#{@id}"
      end
      _puts("exit")
      if (s = _gets) == "exit"
        OPEN_LIST.delete(__id__)
        @opened = false
        true
      else
        Log.warn("Shell#close failed: recieve #{s.inspect}")
        false
      end
    end

    def set_current_task(task_id,task_name)
      @task_id = task_id
      @task_name = task_name
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
      Log.debug "Shell#_puts(host=#{@host},id=#{@id}): #{s.inspect}"
      @chan.put_line(s)
    end

    def _gets
      s = @chan.get_line
      Log.debug "Shell#_gets(host=#{@host},id=#{@id}): #{s.inspect}"
      if s.nil?
        begin
          raise
        rescue => e
          Log.debug e
        end
      end
      s
    end

    def _system(cmd)
      @cmd = cmd
      #raise "@chan is closed" if @chan.closed?
      @lock.synchronize do
        _puts(cmd)
        status = io_read_loop{}
        Integer(status||1) == 0
      end
    end

    def _backquote(cmd)
      @cmd = cmd
      #raise "@chan is closed" if @chan.closed?
      a = []
      @lock.synchronize do
        _puts(cmd)
        status = io_read_loop{|x| a << x}
      end
      a.join("\n")
    end

    def _execute(cmd,quote=nil,&block)
      @cmd = cmd
      #raise "@chan is closed" if @chan.closed?
      status = nil
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
        end
        msg = "Shell#io_read_loop: Invalid result: #{s.inspect}"
        $stderr.puts(msg)
        Log.error(msg)
      end
    end

    public

    def create_fiber(hdl)
      if !@opened
        Log.warn "not opened: host=#{@host} id=#{@id}"
      end
      Fiber.new do
        BY_FIBER[Fiber.current] = self
        Log.debug "shell start id=#{@id} host=#{@host}"
        begin
          while task_str = @task_q.deq
            Log.debug "task_str=#{task_str}"
            if /^(\d+):(.*)$/ =~ task_str
              task_id, task_name = $1.to_i, $2
            else
              raise RuntimeError, "invalid task_str: #{task_str}"
            end
            @task_id = task_id
            @task_name = task_name
            task = Rake.application[task_name]
            begin
              task.execute if task.needed?
              result = "taskend:#{@id}:#{task.name}"
            rescue Exception=>e
              result = "taskfail:#{@id}:#{task.name}"
              Log.error e
            end
            hdl.put_line result
          end
        ensure
          Log.debug "shell id=#{@id} fiber end"
        end
      end
    end

  end
end
