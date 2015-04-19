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

    def initialize(comm,opt={})
      @comm = comm
      @host = comm.host
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
      @chan = Channel.new(@comm,@id)
      @comm.add_channel(@id,@chan)
      OPEN_LIST[__id__] = self
    end

    def finish
      close
    end

    def close
      @lock.synchronize do
        if !@chan.closed?
          _system "exit"
        end
        OPEN_LIST.delete(__id__)
        @comm.delete_channel(@id)
      end
    end

    def force_close
      if !@chan.closed?
        @chan.close
      end
      OPEN_LIST.delete(__id__)
      @comm.delete_channel(@id)
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

    #require 'pp'
    at_exit {
      #$stderr.puts "Shell at_exit OPEN_LIST"
      #pp OPEN_LIST, $stderr
      OPEN_LIST.map do |id,sh|
        # kill running job ???
        # or wait for job ???
        sh.force_close
      end
      Shell.profiler.close
    }

    private

    def _system(cmd)
      @cmd = cmd
      raise "@chan is closed" if @chan.closed?
      @lock.synchronize do
        @chan.puts(cmd)
        status = io_read_loop{}
        Integer(status||1) == 0
      end
    end

    def _backquote(cmd)
      @cmd = cmd
      raise "@chan is closed" if @chan.closed?
      a = []
      @lock.synchronize do
        @chan.puts(cmd)
        status = io_read_loop{|x| a << x}
      end
      a.join("\n")
    end

    def _execute(cmd,quote=nil,&block)
      @cmd = cmd
      raise "@chan is closed" if @chan.closed?
      status = nil
      start_time = Time.now
      begin
        @chan.puts(cmd)
        @status = io_read_loop(&block)
      ensure
        end_time = Time.now
        @status = @@profiler.profile(@task_id, @task_name, cmd,
                                     start_time, end_time, host, @status)
      end
    end

    def io_read_loop
      # @chan.deq must be called in a Fiber
      while x = @chan.deq
        #$stderr.puts "x=#{x.inspect}"
        case x[0]
        when :start
          @pid = x[1].to_i
        when :out
          yield x[1]
        when :err
          $stderr.print x[1]+"\n"
        when :end
          return x[2].to_i
        else
          msg = "Invalid result: #{x.inspect}"
          $stderr.puts(msg)
          Log.error(msg)
        end
      end
    end

  end
end
