require 'pwrake/branch/shell_profiler'
require 'pwrake/io_dispatcher'

module Pwrake

  class DummyMutex
    def synchronize
      yield
    end
  end

  class Shell

    OPEN_LIST={}
    BY_FIBER={}
    @@current_id = "0"
    @@profiler = ShellProfiler.new

    def self.profiler
      @@profiler
    end

    def self.current
      BY_FIBER[Fiber.current]
    end

    def initialize(comm,task_q,opt={})
      @comm = comm
      @host = comm.host
      @task_q = task_q
      @lock = DummyMutex.new
      @@current_id = @@current_id.succ
      @id = @@current_id
      #
      @option = opt
      @work_dir = @option[:work_dir] || Dir.pwd
    end

    attr_reader :id, :host, :status, :profile

    def start
      BY_FIBER[Fiber.current] = self
      @event_q = FiberQueue.new
      @comm.add_queue(@id,@event_q)
      _open
      OPEN_LIST[__id__] = self
    end

    def finish
      close
    end

    def close
      @lock.synchronize do
        #if !@chan.closed?
        _system "exit"
        #end
        OPEN_LIST.delete(__id__)
        @comm.queue.delete(@id)
      end
    end

    def communicator
      @comm
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
      @comm.puts("#{@id}:#{s}")
      @comm.flush
    end

    def _open
      @comm.puts("open:#{@id}")
      @comm.flush
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
      # @chan.deq must be called in a Fiber
      while x = @event_q.deq  # receive from WorkerCommunicator#on_read
        #$stderr.puts "x=#{x.inspect}"
        case x[0]
        when :start
        when :out
          yield x[1]
        when :err
          $stderr.print x[1]+"\n"
        when :end
          # see Executor#status_to_str
          status = x[1]
          case status
          when /^\d+$/
            status = status.to_i
          end
          return status
        else
          msg = "Shell#io_read_loop: Invalid result: #{x.inspect}"
          $stderr.puts(msg)
          Log.error(msg)
        end
      end
    end

    public

    def create_fiber(iow)
      Fiber.new do
        start
        Log.debug "shell start id=#{@id} host=#{@host}"
        begin
          while task_str = @task_q.deq
            Log.debug "task_str=#{task_str}"
            if /^(\d+):(.*)$/ =~ task_str
              task_id, task_name = $1.to_i, $2
            else
              raise RuntimeError, "invalid task_str: #{task_str}"
            end
            #set_current_task(task_id,task_name)
            @task_id = task_id
            @task_name = task_name
            task = Rake.application[task_name]
            begin
              task.execute if task.needed?
            rescue Exception=>e
              if task.kind_of?(Rake::FileTask) && File.exist?(task.name)
                #handle_failed_target(task.name)
              end
              iow.puts "taskfail:#{@id}:#{task.name}"
              iow.flush
              raise e
            end
            iow.puts "taskend:#{@id}:#{task.name}"
            iow.flush
          end
        ensure
          Log.debug "closing shell id=#{@id}"
          close
        end
      end
    end

  end
end
