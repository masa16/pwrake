module Pwrake

  class Executor

    LIST = {}

    def initialize(dir_class,id)
      @id = id
      @env = {}
      @out = Writer.instance
      @log = LogExecutor.instance
      @queue = Queue.new
      @dir = dir_class.new
      LIST[@id] = self
      @out_thread  = start_out_thread
      @err_thread  = start_err_thread
      @exec_thread = start_exec_thread
    end

    def start_out_thread
      pipe_in, @pipe_out = IO.pipe
      Thread.new(pipe_in,"#{@id}:") do |pin,pre|
        while s = pin.gets
          s.chomp!
          @out.puts pre+s
        end
      end
    end

    def start_err_thread
      pipe_in2, @pipe_err = IO.pipe
      Thread.new(pipe_in2,"#{@id}e:") do |pin,pre|
        while s = pin.gets
          s.chomp!
          @out.puts pre+s
        end
      end
    end

    def execute(cmd)
      @queue.enq(cmd)
    end

    def killed?
      @killed || !@out_thread.alive? || !@err_thread.alive?
    end

    def start_exec_thread
      Thread.new do
        begin
          @dir.open
          @dir.open_messages.each{|m| @log.info(m)}
          @out.puts "open:#{@id}"
          while cmd = @queue.deq
            break if killed?
            begin
              run(cmd)
            rescue => exc
              put_exc(exc)
              @log.error exc
              @log.error exc.backtrace.join("\n")
            end
            break if killed?
          end
          @pipe_out.flush
          @pipe_err.flush
          @pipe_out.close
          @pipe_err.close
        ensure
          @dir.close_messages.each{|m| @log.info(m)}
          @dir.close
        end
      end
    end

    def spawn_command(cmd,dir,env)
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
        x = "#{s.to_i}"
      elsif s.signaled?
        if s.coredump?
          x = "coredumped"
        else
          x = "termsig=#{s.termsig}"
        end
      elsif s.stopped?
        x = "stopsig=#{s.stopsig}"
      else
        x = "unknown_status"
      end
      return x
    end

    def put_end
      @out.puts "end:#{@id}"
    end

    def put_exc(exc)
      @out.puts "exc:#{@id}:#{exc}"
    end

    def close
      execute(nil)  # threads end
    end

    #alias exit :close

    def join
      LIST.delete(@id)
      @out_thread.join(3) if @out_thread
      @err_thread.join(3) if @err_thread
      @exec_thread.join(10) if @exec_thread
    end

    def kill(sig)
      @killed = true
      @queue.enq(nil)
      while @queue.deq; end
      @log.warn "Executor(id=#{@id})#kill pid=#{@pid} sig=#{sig}"
      Process.kill(sig,@pid) if @pid
      @queue.enq(nil)
    end

    #
    def run(cmd)
      case cmd
      when Proc
        cmd.call
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
        spawn_command(cmd,dir,@env)
      else
        raise RuntimeError,"invalid cmd: #{cmd.inspect}"
      end
    end

  end
end
