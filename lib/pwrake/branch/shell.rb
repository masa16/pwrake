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
    HOST_IO={}
    BY_FIBER={}
    #@@shell = "sh"
    MUX_HDL={}

    DISPATCHER=IODispatcher.new

    @@shell = "ruby "+File.expand_path(File.dirname(__FILE__))+"/../../../bin/pwrake_worker"
    @@nice = "nice"
    @@current_id = "0"
    @@profiler = ShellProfiler.new

    def self.profiler
      @@profiler
    end

    def self.current
      BY_FIBER[Fiber.current]
    end

    def initialize(host,opt={})
      $stderr.puts "host=#{host}"
      @host = host || 'localhost'
      @lock = DummyMutex.new
      @@current_id = @@current_id.succ
      @id = @@current_id
      #
      @option = opt
      @work_dir = @option[:work_dir] || Dir.pwd
      @pass_env = @option[:pass_env]
      @ssh_opt = @option[:ssh_opt]
    end

    attr_reader :id, :host
    attr_accessor :current_task

    def system_cmd(*arg)
      if ['localhost','localhost.localdomain','127.0.0.1'].include? @host
        [@@nice,@@shell].join(' ')
      else
        "ssh -x -T -q #{@ssh_opt} #{@host} #{@@nice} #{@@shell}"
      end
    end

    def start
      #Pwrake.current_shell = self
      BY_FIBER[Fiber.current] = self
      open()
      cd_work_dir
    end

    def cd_work_dir
      _execute("cd #{@work_dir}") or die
    end

    def open(path=nil)
      if io = HOST_IO[@host]
        @io = io
      else
        @io = HOST_IO[@host] = new_connection(path)
      end
      @chan = Channel.new(@io,@id)
      MUX_HDL[@io].add_channel(@id,@chan)

      OPEN_LIST[__id__] = self
      #_system "export PATH='#{path}'"
      #if @pass_env
      #  @pass_env.each do |k,v|
      #    _system "export #{k}='#{v}'"
      #  end
      #end
    end

    def new_connection(path=nil)
      $stderr.puts system_cmd
      io = IO.popen(system_cmd,"r+")
      mh = MultiplexHandler.new
      DISPATCHER.attach_read(io,mh)
      MUX_HDL[io] = mh

      if path
        io.puts "export:PATH='#{path}'"
      end
      if @pass_env
        @pass_env.each do |k,v|
          io.puts "export:#{k}='#{v}'"
        end
      end
      return io
    end

    attr_reader :host, :status, :profile

    def finish
      close
    end

    def close
      @lock.synchronize do
        if !@io.closed?
          @io.puts("exit")
          # @io.close
        end
        OPEN_LIST.delete(__id__)
      end
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
      #puts command
      @lock.synchronize do
        _execute(command){|x| print x+"\n"}
      end
      @status == 0
    end

    def cd_work_dir
      _system("cd #{@work_dir}") or die
    end

    def cd(dir="")
      _system("cd #{dir}") or die
    end

    def die
      raise "Failed at #{@host}, id=#{@id}, cmd='#{@cmd}'"
    end

    at_exit {
      OPEN_LIST.map do |k,v|
        v.close
      end
      Shell.profiler.close
    }

    private

    def _system(cmd)
      @cmd = cmd
      raise "@io is closed" if @io.closed?
      @lock.synchronize do
        #p cmd
        @chan.puts(cmd)
        status = io_read_loop{}
        Integer(status||1) == 0
      end
    end

    def _backquote(cmd)
      @cmd = cmd
      raise "@io is closed" if @io.closed?
      a = []
      @lock.synchronize do
        @chan.puts(cmd)
        status = io_read_loop{|x| a << x}
      end
      a.join("\n")
    end

    def _execute(cmd,quote=nil,&block)
      @cmd = cmd
      raise "@io is closed" if @io.closed?
      status = nil
      start_time = Time.now
      begin
        @chan.puts(cmd)
        @status = io_read_loop(&block)
      ensure
        end_time = Time.now
        @status = @@profiler.profile(@current_task, cmd,
                                     start_time, end_time, host, @status)
      end
    end

    def io_read_loop
      while x = @chan.deq
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
          $stderr.print "Invalid result: #{x.inspect}\n"
        end
      end
    end

  end


  class NoActionShell < Shell
    def initialize()
      @host = '(noaction)'
      @@current_id = @@current_id.succ
      @id = @@current_id
    end
    def start
    end
    def finish
    end
  end

end # module Pwrake
