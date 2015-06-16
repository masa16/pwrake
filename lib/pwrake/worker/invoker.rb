require "timeout"

module Pwrake

  class Invoker

    def initialize(dir_class, n_core)
      @dir_class = dir_class
      @shellrc = []
      @out = Writer.instance # firstly replace $stderr
      @log = LogExecutor.instance
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
      setup_loop
      start_heartbeat
      command_loop
    end

    def setup_loop
      while line = get_line
        case line
          #
        when /^export:(\w+)=(.*)$/o
          k,v = $1,$2
          ENV[k] = v
          #
        when /^heartbeat:(.*)$/o
          @heartbeat_interval = $1.to_i
          #
        when /^shellrc:(.*)$/o
          @shellrc << $1
          #
        when /^open:(.*)$/o
          $1.split.each do |id|
            Executor.new(@dir_class,id,@shellrc)
          end
          #
        when /^kill:(.*)$/o
          kill_all($1)
          Kernel.exit
          #
        when "exit_worker"
          Kernel.exit
          #
        when "setup_end"
          return
        else
          raise RuntimeError,"invalid line: #{line}"
        end
      end
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
          #
        when /^(\d+):(.*)$/o
          id,cmd = $1,$2
          ex = Executor::LIST[id]
          if ex.nil?
            if cmd=="exit"
              @out.puts "end:#{id}"
              next
            else
              ex = Executor.new(@dir_class,id,@shellrc)
            end
          end
          ex.execute(cmd)
          #
        when "exit_worker"
          return
          #
        when /^kill:(\d+):(.*)$/o
          kill_one($1,$2)
          #
        when /^kill:(.*)$/o
          kill_all($1)
          return
          #
        when /^p$/o
          puts "Executor::LIST = #{Executor::LIST.inspect}"
          #
        else
          msg = "invalid line: #{line}"
          @log.fatal msg
          raise RuntimeError,msg
        end
      end
    end

    def kill_one(id,sig)
      sig = sig.to_i if /^\d+$/=~sig
      exc = Executor::LIST[id]
      exc.kill(sig)
    end

    def kill_all(sig)
      sig = sig.to_i if /^\d+$/=~sig
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
      @out.puts "worker_end"
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
