require 'timeout'

module Pwrake

  class Invoker

    def initialize(dir_class, n_core)
      @heartbeat_interval = 30
      @dir_class = dir_class
      @log = LogExecutor.instance
      @log.open(@dir_class)
      @ncore = case n_core
               when /^\d+$/
                 n_core.to_i
               when Integer
                 n_core
               else
                 processor_count
               end
      @out = Writer.instance
      @out.puts "ncore:#{@ncore}"

      at_exit{
        @log.info "at_exit"
        close_all
      }

      [:TERM,:INT].each do |sig|
        Signal.trap(sig) do
          #close_all # called at_exit
          Kernel.exit
        end
      end

      Signal.trap("PIPE", "EXIT")
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
        when /^export:(\w+)=(.*)$/o
          k,v = $1,$2
          ENV[k] = v
          #
        when /^open:(.*)$/o
          $1.split.each do |id|
            Executor.new(@dir_class,id)
          end
          #
        when "setup_end"
          return
        else
          raise RuntimeError,"invalid line: #{line}"
        end
      end
    end

    def command_loop
      @log.info "command_loop"
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
              ex = Executor.new(@dir_class,id)
            end
          end
          ex.execute(cmd)
          #
        when "exit_worker"
          return
          #
        when /^kill:(\d+):(.*)$/o
          id,sig = $1,$2
          sig = sig.to_i if /^\d+$/=~sig
          worker = Executor::LIST[id]
          worker.kill(sig) # if worker
          #
        when /^kill:(.*)$/o
          sig = $1
          sig = sig.to_i if /^\d+$/=~sig
          @log.warn "worker_killed:signal=#{sig}"
          Executor::LIST.each{|id,ex| ex.kill(sig)}
          return
          #
        when /^p$/o
          puts "Executor::LIST = #{Executor::LIST.inspect}"
          #
        else
          raise RuntimeError,"invalid line: #{line}"
        end
      end
    end

    def start_heartbeat
      @heartbeat_thread = Thread.new do
        while true
          @out.puts "heartbeat"
          sleep @heartbeat_interval
        end
      end
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
      @log.info "worker:end:#{id_list.inspect}"
      begin
        timeout(20){@log.close}
      rescue => e
        $stdout.puts e
        $stdout.puts e.backtrace.join("\n")
      end
      @heartbeat_thread.kill
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
