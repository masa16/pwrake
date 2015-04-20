module Pwrake

  class Invoker

    def initialize(dir_class, n_core)
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

    def run
      while line = $stdin.gets
        line.chomp!
        line.strip!
        @log.info ">#{line}"
        case line
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
        when /^p$/o
          puts "Executor::LIST = #{Executor::LIST.inspect}"
          #
        when /^new:(.*)$/o
          $1.split.each do |id|
            Executor.new(@dir_class,id)
          end
          #
        when /^export:(\w+)=(.*)$/o
          k,v = $1,$2
          ENV[k] = v
          #
        when /^exit$/o
          return
          #
        when "exit_worker"
          return
          #
        when /^kill:(\d+):(.*)$/o
          id,signal = $1,$2
          worker = Executor::LIST[id]
          worker.kill(signal) if worker
          #
        when /^kill:(.*)$/o
          sig = $1
          sig = sig.to_i if /^\d+$/=~sig
          @out.puts "worker_killed:signal=#{sig}"
          Process.kill(sig, 0)
          #
        else
          raise "invalid line: #{line}"
        end
      end
    end

    def close_all
      @log.info "close_all"
      Dir.chdir
      id_list = Executor::LIST.keys
      ex_list = Executor::LIST.values
      ex_list.each {|ex| ex.close}
      ex_list.each {|ex| ex.join}
      @out.puts "worker_end"
      @log.info "worker:end:#{id_list.inspect}"
      @log.close
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
