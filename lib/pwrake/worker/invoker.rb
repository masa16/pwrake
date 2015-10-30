require "timeout"

module Pwrake

  class Invoker

    def initialize(dir_class, n_core, option)
      @dir_class = dir_class
      @option = option
      @out = Writer.instance # firstly replace $stderr
      @log = LogExecutor.instance
      @log.init(@option)
      @log.open(@dir_class)
      @out.add_logger(@log)
      @ncore = case n_core
               when /^\d+$/
                 n_core.to_i
               when Integer
                 n_core
               else
                 processor_count
               end
      @out.puts "ncore:#{@ncore}"
      at_exit{
        @log.info "at_exit"
        close_all
      }
      # does NOT exit when writing to broken pipe
      Signal.trap("PIPE", "SIG_IGN")
    end

    def get_line
      begin
        line = $stdin.gets
        exit if !line
        line.chomp!
        line.strip!
        @log.info ">#{line}"
        return line
      rescue
        exit
      end
    end

    def run
      setup_option
      if setup_loop
        start_heartbeat
        command_loop
      end
    end

    def setup_option
      @log.info @option.inspect
      @heartbeat_interval = @option[:heartbeat]
      @shell_cmd = @option[:shell_command]
      @shell_rc = @option[:shell_rc] || []
      (@option[:pass_env]||{}).each do |k,v|
        ENV[k] = v
      end
    end

    def setup_loop
      while line = get_line
        case line
        when /^(\d+):open$/o
          $1.split.each do |id|
            Executor.new(@dir_class,id,@shell_cmd,@shell_rc)
          end
        when "setup_end"
          return true
        else
          return false if common_line(line)
        end
      end
      false
    end

    def start_heartbeat
      if @heartbeat_interval
        @heartbeat_thread = Thread.new do
          while true
            @out.puts "heartbeat"
            sleep @heartbeat_interval
          end
        end
      end
    end

    def command_loop
      while line = get_line
        case line
        when /^(\d+):(.*)$/o
          id,cmd = $1,$2
          ex = Executor::LIST[id]
          if ex.nil?
            if cmd=="exit"
              @out.puts "#{id}:end"
              next
            else
              ex = Executor.new(@dir_class,id,@shell_cmd,@shell_rc)
            end
          end
          ex.execute(cmd)
        else
          break if common_line(line)
        end
      end
    end

    def common_line(line)
      case line
      when /^exit$/o
        return true
        #
      when /^kill:(.*)$/o
        kill_all($1)
        return false
        #
      when /^p$/o
        puts "Executor::LIST = #{Executor::LIST.inspect}"
        return false
        #
      else
        msg = "invalid line: #{line}"
        @log.fatal msg
        raise RuntimeError,msg
      end
    end

    def kill_all(sig)
      sig = sig.to_i if /^\d+$/o =~ sig
      @log.warn "worker_killed:signal=#{sig}"
      Executor::LIST.each{|id,exc| exc.kill(sig)}
    end

    def close_all
      @log.info "close_all"
      Dir.chdir
      id_list = Executor::LIST.keys
      ex_list = Executor::LIST.values
      ex_list.each{|ex| ex.close}
      begin
        ex_list.each{|ex| ex.join}
      rescue => e
        $stdout.puts e
        $stdout.puts e.backtrace.join("\n")
      end
      @heartbeat_thread.kill if @heartbeat_thread
      @log.info "worker:end:#{id_list.inspect}"
      begin
        timeout(20){@log.close}
      rescue => e
        $stdout.puts e
        $stdout.puts e.backtrace.join("\n")
      end
      @out.puts "exited"
    end

    # from Michael Grosser's parallel
    # https://github.com/grosser/parallel
    def processor_count
      host_os = RbConfig::CONFIG['host_os']
      case host_os
      when /linux|cygwin/
        ncpu = 0
        open("/proc/cpuinfo").each do |l|
          ncpu += 1 if /^processor\s+: \d+/=~l
        end
        ncpu
      when /darwin9/
        `hwprefs cpu_count`.to_i
      when /darwin/
        (hwprefs_available? ? `hwprefs thread_count` : `sysctl -n hw.ncpu`).to_i
      when /(open|free)bsd/
        `sysctl -n hw.ncpu`.to_i
      when /mswin|mingw/
        require 'win32ole'
        wmi = WIN32OLE.connect("winmgmts://")
        cpu = wmi.ExecQuery("select NumberOfLogicalProcessors from Win32_Processor")
        cpu.to_enum.first.NumberOfLogicalProcessors
      when /solaris2/
        `psrinfo -p`.to_i # physical cpus
      else
        raise "Unknown architecture: #{host_os}"
      end
    end

  end
end
