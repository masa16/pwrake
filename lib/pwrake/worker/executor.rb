module Pwrake

  class Executor

    LIST = {}
    CHARS='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
    TLEN=32

    def initialize(dir_class,id,shell_cmd,shell_rc)
      @id = id
      @shell_rc = shell_rc
      @shell_cmd = shell_cmd || ENV['SHELL'] || '/bin/sh'
      @terminator = ""
      TLEN.times{ @terminator << CHARS[rand(CHARS.length)] }
      @out = Writer.instance
      @log = LogExecutor.instance
      @queue = Queue.new
      @dir = dir_class.new
      @spawn_in, @sh_in = IO.pipe
      @sh_out, @spawn_out = IO.pipe
      @sh_err, @spawn_err = IO.pipe
      LIST[@id] = self
      @exec_thread = start_exec_thread
    end

    def execute(cmd)
      @queue.enq(cmd)
    end

    def killed?
      @killed
    end

    def start_exec_thread
      Thread.new do
        begin
          @dir.open
          @dir.open_messages.each{|m| @log.info(m)}
          begin
            @pid = Kernel.spawn(@shell_cmd,
                                :out=>@spawn_out,
                                :err=>@spawn_err,
                                :in=>@spawn_in,
                                :chdir=>@dir.current)
            @out.puts "#{@id}:open"
            @shell_rc.each do |cmd|
              run_rc(cmd)
            end
            while cmd = @queue.deq
              run(cmd)
            end
            @sh_in.puts("exit") if !@killed
          ensure
            pid,status = Process.waitpid2(@pid)
            @log.info("shell exit status: "+status.inspect)
          end
        rescue => exc
          @out.puts "#{@id}:exc:#{exc}"
          @log.error exc
        ensure
          @dir.close_messages.each{|m| @log.info(m)}
          @dir.close
        end
      end
    end

    def run(cmd)
      case cmd
      when Proc
        cmd.call
      when "cd"
        @dir.cd
        run_command("cd "+@dir.current)
        #
      when /^cd\s+(.*)$/
        @dir.cd($1)
        run_command("cd "+@dir.current)
        #
      when /^exit\b/
        close
        @out.puts "#{@id}:exit"
        #
      when String
        run_command(cmd)
        #
      else
        raise RuntimeError,"invalid cmd: #{cmd.inspect}"
      end
    end

    def run_rc(cmd)
      if /\\$/ =~ cmd  # command line continues
        @sh_in.puts(cmd)
        return
      end
      term = "\necho '#{@terminator}':$? \necho '#{@terminator}' 1>&2"
      @sh_in.puts(cmd+term)
      status = ""
      io_set = [@sh_out,@sh_err]
      loop do
        io_sel, = IO.select(io_set,nil,nil)
        for io in io_sel
          s = io.gets.chomp
          case io
          when @sh_out
            if s[0,TLEN] == @terminator
              status = s[TLEN+1..-1]
              io_set.delete(@sh_out)
            else
              @log.info "<#{@id}:o:"+s if @log
            end
          when @sh_err
            if s[0,TLEN] == @terminator
              io_set.delete(@sh_err)
            else
              @log.info "<#{@id}:e:"+s if @log
            end
          end
        end
        break if io_set.empty?
      end
      @log.info "<#{@id}:z:#{status}" if @log
    end

    def run_command(cmd)
      if /\\$/ =~ cmd  # command line continues
        @sh_in.puts(cmd)
        return
      end
      term = "\necho '#{@terminator}':$? \necho '#{@terminator}' 1>&2"
      @sh_in.puts(cmd+term)
      status = ""
      io_set = [@sh_out,@sh_err]
      loop do
        io_sel, = IO.select(io_set,nil,nil)
        for io in io_sel
          s = io.gets.chomp
          case io
          when @sh_out
            if s[0,TLEN] == @terminator
              status = s[TLEN+1..-1]
              io_set.delete(@sh_out)
            else
              @out.puts "#{@id}:o:"+s
            end
          when @sh_err
            if s[0,TLEN] == @terminator
              io_set.delete(@sh_err)
            else
              @out.puts "#{@id}:e:"+s
            end
          end
        end
        break if io_set.empty?
      end
      @out.puts "#{@id}:z:#{status}"
    end

    def close
      execute(nil)  # threads end
    end

    def join
      LIST.delete(@id)
      @exec_thread.join(10) if @exec_thread
    end

    def kill(sig)
      @killed = true
      @queue.clear
      if @pid
        s = `ps ho pid --ppid=#{@pid}`
        s.each_line{|x|
          pid=x.to_i
          Process.kill(sig,pid)
          @log.warn "Executor(id=#{@id})#kill pid=#{pid} sig=#{sig}"
        }
        Process.kill(sig,@pid)
        @log.warn "Executor(sh,id=#{@id})#kill pid=#{@pid} sig=#{sig}"
      end
      @spawn_out.puts @terminator+":signal=#{sig}"
      @spawn_err.puts @terminator
      #@queue.enq(nil)
    end

  end
end
