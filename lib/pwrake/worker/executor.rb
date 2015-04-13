module Pwrake

  class Executor

    LIST = {}

    def initialize(dir_class,id)
      @id = id
      @env = {}
      @out = Writer.instance
      @log = LogExecutor.instance
      @queue = Queue.new
      LIST[@id] = self
      @out_thread  = start_out_thread
      @err_thread  = start_err_thread
      @exec_thread = start_exec_thread
      @dir = dir_class.new
      execute "open_directory"
    end

    def start_out_thread
      pipe_in, @pipe_out = IO.pipe
      Thread.new(pipe_in,"#{@id}:") do |pin,pre|
        while s = pin.gets
          @out.print pre+s
        end
       end
    end

    def start_err_thread
      pipe_in2, @pipe_err = IO.pipe
      Thread.new(pipe_in2,"#{@id}e:") do |pin,pre|
        while s = pin.gets
          @out.print pre+s
        end
      end
    end

    def execute(cmd)
      @queue.enq(cmd)
    end

    def start_exec_thread
      Thread.new do
        while cmd = @queue.deq
          begin
            run(cmd)
          rescue => exc
            @out.puts x="end:error:#{exc}"
            @log.error exc
            @log.error exc.backtrace.join("\n")
          end
        end
        @pipe_out.flush
        @pipe_err.flush
        @pipe_out.close
        @pipe_err.close
      end
    end

    def spawn(cmd,dir,env)
      @pid = Kernel.spawn(env,cmd,:out=>@pipe_out,:err=>@pipe_err,:chdir=>dir)
      @out.puts "start:#{@id}:#{@pid}"
      pid,status = Process.wait2(@pid)
      @pid = nil
      @pipe_out.flush
      @pipe_err.flush
      @out_thread.run
      @err_thread.run
      status_s = status_to_str(status)
      @out.puts "end:#{@id}:#{pid}:#{status_s}"
    end

    def status_to_str(s)
      if s.exited?
        x = "#{s.to_i},exited"
      elsif s.signaled?
        x = "#{s.to_i},signaled,signal=#{s.termsig}"
      elsif s.stopped?
        x = "#{s.to_i},stopped,signal=#{s.stopsig}"
      elsif s.coredump?
        x = "#{s.to_i},coredumped"
      end
      return x
    end

    def put_end
      @out.puts "end:#{@id}"
    end

    def close
      LIST.delete(@id)
      execute(nil)  # threads end
      @dir.close_messages.each{|m| @log.info(m)}
      @dir.close
    end

    #alias exit :close

    def join
      @out_thread.join  if @out_thread
      @err_thread.join  if @err_thread
      @exec_thread.join if @exec_thread
    end

    def kill(sig)
      sig = sig.to_i if /^\d+$/=~sig
      Process.kill(sig,@pid) if @pid
    end

    #
    def run(cmd)
      case cmd
      when Proc
        cmd.call
      when "open_directory"
        @dir.open
        @dir.open_messages.each{|m| @log.info(m)}
        #
      #when "close_directory"
      #  @dir.close
      #  #
      when "cd"
        @dir.cd
        put_end
        #
      when /^cd\s+(.*)$/
        @dir.cd($1)
        put_end
        #
      when "exit"
        close
        put_end
        #
      when /^export (\w+)=(.*)$/o
        k,v = $1,$2
        @env[k] = v
        #
      when String
        dir = @dir.current
        if !Dir.exist?(dir)
          raise "Directory '#{dir}' does not exsit"
        end
        spawn(cmd,dir,@env)
      else
        raise RuntimeError,"invalid cmd: #{cmd.inspect}"
      end
    end

  end
end
