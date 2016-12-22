require "etc"

module Pwrake

  class Invoker

    def initialize(dir_class, ncore, option)
      @dir_class = dir_class
      @option = option
      @selector = Selector.new
      @ex_list = {}
      @out = Writer.instance # firstly replace $stderr
      @log = LogExecutor.instance
      @log.init(@option)
      @log.open(@dir_class)
      @out.add_logger(@log)
      if ncore.kind_of?(Integer)
        if ncore > 0
          @ncore = ncore
        else
          @ncore = Etc.nprocessors + ncore
        end
        if @ncore <= 0
          m = "Out of range: ncore=#{ncore.inspect}"
          @out.puts "ncore:"+m
          raise ArgumentError,m
        end
      elsif ncore.nil?
        @ncore = Etc.nprocessors
      else
        m = "Invalid argument: ncore=#{ncore.inspect}"
        @out.puts "ncore:"+m
        raise ArgumentError,m
      end
      @out.puts "ncore:#{@ncore}"
      # does NOT exit when writing to broken pipe
      Signal.trap("PIPE", "SIG_IGN")
    end

    def get_line(io)
      line = io.gets
      if line
        line.chomp!
        line.strip!
        @log.info ">#{line}"
      end
      return line
    end

    def run
      setup_option
      setup_loop
      @rd = Reader.new($stdin)
      @selector.add_reader($stdin){command_callback(@rd)}
      @selector.loop
    rescue => exc
      @log.error(([exc.to_s]+exc.backtrace).join("\n"))
    ensure
      close_all
    end

    def setup_option
      @log.info @option.inspect
      @out.heartbeat = @option[:heartbeat]
      @shell_cmd = @option[:shell_command]
      @shell_rc = @option[:shell_rc] || []
      (@option[:pass_env]||{}).each do |k,v|
        ENV[k] = v
      end
    end

    def setup_loop
      while line = get_line($stdin)
        case line
        when /^(\d+):open$/o
          $1.split.each do |id|
            @ex_list[id] = Executor.new(@selector,@dir_class,id)
          end
        when "setup_end"
          return
        else
          if common_line(line)
            raise RuntimeError,"exit during setup_loop"
          end
        end
      end
      raise RuntimeError,"incomplete setup_loop"
    end

    def command_callback(rd)
      while line = get_line(rd) # rd returns nil if line is incomplete
        case line
        when /^(\d+):(.*)$/o
          id,cmd = $1,$2
          @ex_list[id].execute(cmd.chomp)
        else
          break if common_line(line)
        end
      end
      if rd.eof?
        # connection lost
        raise RuntimeError,"lost connection to master"
      end
    end

    def common_line(line)
      case line
      when /^exit$/o
        @selector.delete_reader($stdin)
        return true
        #
      when /^kill:(.*)$/o
        sig = $1
        sig = sig.to_i if /^\d+$/o =~ sig
        @log.warn "killing worker, signal=#{sig}"
        @ex_list.each{|id,ex| ex.kill(sig)}
        return false
        #
      when /^p$/o
        $stderr.puts "@ex_list = #{@ex_list.inspect}"
        return false
        #
      else
        msg = "invalid line: #{line}"
        @log.fatal msg
        raise RuntimeError,msg
      end
    end

    def close_all
      @log.info "close_all"
      @heartbeat_thread.kill if @heartbeat_thread
      Dir.chdir
      @ex_list.each_value{|ex| ex.close}
      @ex_list.each_value{|ex| ex.join}
      @log.info "worker:end:#{@ex_list.keys.inspect}"
      begin
        Timeout.timeout(20){@log.close}
      rescue => e
        $stdout.puts e
        $stdout.puts e.backtrace.join("\n")
      end
    ensure
      @out.puts "exited"
    end

  end
end
