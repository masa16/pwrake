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
            @sh_in.puts("exit")
            @sh_in.flush
          ensure
            status = nil
            begin
              timeout(5){
                pid,status = Process.waitpid2(@pid)
              }
            rescue
              @log.info("#{@id}:kill INT sh @pid=#{@pid}")
              Process.kill("INT",@pid)
              pid,status = Process.waitpid2(@pid)
            end
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
      run_command_main(cmd){|s| @log.info "<"+s if @log}
    end

    def run_command(cmd)
      run_command_main(cmd){|s| @out.puts s}
    end

    def run_command_main(cmd)
      if /\\$/ =~ cmd  # command line continues
        @sh_in.puts(cmd)
        @sh_in.flush
        return
      end
      term = "\necho '#{@terminator}':$? \necho '#{@terminator}' 1>&2"
      @sh_in.puts(cmd+term)
      @sh_in.flush
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
              yield "#{@id}:o:"+s
            end
          when @sh_err
            if s[0,TLEN] == @terminator
              io_set.delete(@sh_err)
            else
              yield "#{@id}:e:"+s
            end
          end
        end
        break if io_set.empty?
      end
      yield "#{@id}:z:#{status}"
    end

    def close
      execute(nil)  # threads end
    end

    def join
      LIST.delete(@id)
      @exec_thread.join(15) if @exec_thread
    end

    def kill(sig)
      @queue.clear
      if @pid
        # kill process group
        s = `ps ho pid --ppid=#{@pid}`
        s.each_line do |x|
          pid = x.to_i
          Process.kill(sig,pid)
          @log.warn "Executor(id=#{@id})#kill pid=#{pid} sig=#{sig}"
        end
        if s.empty?
          @log.warn "Executor(id=#{@id})#kill nothing killed"
        end
      end
      @spawn_out.flush
      @spawn_err.flush
    end

  end
end
