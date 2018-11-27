require "socket"
require "etc"

module Pwrake

  class Invoker
    def processor_count
      begin
        Etc.nprocessors
      rescue
        # only for Linux
        IO.read("/proc/cpuinfo").scan(/^processor/).size
      end
    end

    def get_io
      [IO, $stdin, $stdout]
    end

    def setup_connection
      ioc, ior, iow = get_io()
      # read @ncore and @option
      @ncore,len = ior.read(8).unpack("V2")
      @option = Marshal.load(ior.read(len))
      # set pipe to branch-master
      @selector = NBIO::Selector.new(ioc)
      @rd = NBIO::Reader.new(@selector,ior)
      @out = Writer.instance
      @out.out = iow
    end

    def initialize
      setup_connection
      @dir_class = Pwrake.const_get(@option[:shared_directory])
      @dir_class.init(@option)
      @ex_list = {}
      @log = LogExecutor.instance
      @log.init(@option)
      @log.open(@dir_class)
      @out.add_logger(@log)
      send_ipaddr
      send_ncore
      # does NOT exit when writing to broken pipe
      Signal.trap("PIPE", "SIG_IGN")
    end

    def send_ipaddr
      # get IP addresses
      v = Socket.getifaddrs.
        select{|a| a.addr.ip? && (a.flags & Socket::IFF_MULTICAST != 0)}
      # write IP addresses
      v.each do |a|
        @out.puts "ip:#{a.addr.ip_address}"
      end
    end

    def send_ncore
      if @ncore.kind_of?(Integer)
        if @ncore <= 0
          @ncore += processor_count()
        end
        if @ncore <= 0
          m = "Out of range: ncore=#{@ncore.inspect}"
          @out.puts "ncore:"+m
          raise ArgumentError,m
        end
      elsif @ncore.nil?
        @ncore = processor_count()
      else
        m = "Invalid argument: ncore=#{@ncore.inspect}"
        @out.puts "ncore:"+m
        raise ArgumentError,m
      end
      @out.puts "ncore:#{@ncore}"
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
      run_setup
      run_command
    ensure
      close_all
    end

    def run_setup
      setup_option
      Fiber.new{setup_loop}.resume
      @selector.run
    rescue => exc
      @log.error(([exc.to_s]+exc.backtrace).join("\n"))
    end

    def setup_option
      @log.info @option.inspect
      @out.heartbeat = @option[:heartbeat]
      (@option[:pass_env]||{}).each do |k,v|
        ENV[k] = v
      end
    end

    def setup_loop
      loop do
        line = get_line(@rd)
        case line
        when /^(\d+):open$/o
          $1.split.each do |id|
            @ex_list[id] = Executor.new(@selector,@dir_class,id,@option)
          end
        when "setup_end"
          return
        else
          if common_line(line)
            raise RuntimeError,"exit during setup_loop"
          end
        end
      end
    end

    def run_command
      Fiber.new{command_callback}.resume
      @selector.run
    rescue => exc
      @log.error(([exc.to_s]+exc.backtrace).join("\n"))
      kill_all("TERM")
    end

    def command_callback
      while line = get_line(@rd)
        case line
        when /^(\d+):exit$/o
          id = $1
          ex = @ex_list.delete(id)
          ex.close
          ex.join
        when /^(\d+):(.*)$/o
          id,cmd = $1,$2
          @ex_list[id].execute(cmd.chomp)
        else
          break if common_line(line)
        end
      end
      if @rd.eof?
        # connection lost
        raise RuntimeError,"lost connection to master"
      end
    end

    def common_line(line)
      case line
      when /^exit$/o
        return true
        #
      when /^kill:(.*)$/o
        sig = $1
        sig = sig.to_i if /^\d+$/o =~ sig
        kill_all(sig)
        return true
        #
      when /^p$/o
        $stderr.puts "@ex_list = #{@ex_list.inspect}"
        return false
        #
      else
        msg = "invalid line: #{line.inspect}"
        @log.fatal msg
        raise RuntimeError,msg
      end
    end

    def kill_all(sig)
      @log.warn "killing worker, signal=#{sig}"
      @ex_list.each{|id,ex| ex.kill(sig)}
    end

    def close_all
      @log.info "close_all"
      @heartbeat_thread.kill if @heartbeat_thread
      Dir.chdir
      @ex_list.each_value{|ex| ex.close}
      @ex_list.each_value{|ex| ex.join}
      @log.info "worker:end:#{@ex_list.keys.inspect}"
      Timeout.timeout(20){@log.close}
    ensure
      @out.puts "exited"
    end

  end
end
