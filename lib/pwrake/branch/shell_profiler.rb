require "csv"

module Pwrake

  class ShellProfiler

    HEADER_FOR_PROFILE =
      %w[exec_id task_id task_name command
         start_time end_time elap_time host status]

    HEADER_FOR_GNU_TIME =
      %w[realtime systime usrtime maxrss averss memsz
         datasz stcksz textsz pagesz majflt minflt nswap ncswinv
         ncswvol ninp nout msgrcv msgsnd signum]

    def initialize
      @lock = Mutex.new
      @gnu_time = false
      @id = 0
      @io = nil
    end

    def open(file,gnu_time=false,plot=false)
      @file = file
      @gnu_time = gnu_time
      @plot = plot
      @lock.synchronize do
        @io.close if @io != nil
        @io = CSV.open(file,"w")
      end
      _puts table_header
      t = Time.now
      profile(nil,nil,'pwrake_profile_start',t,t)
    end

    def close
      t = Time.now
      profile(nil,nil,'pwrake_profile_end',t,t)
      @lock.synchronize do
        @io.close if @io != nil
        @io = nil
      end
      if @plot
        require 'pwrake/report'
        Parallelism.plot_parallelism(@file)
      end
    end

    def _puts(s)
      @lock.synchronize do
        @io.puts(s) if @io
      end
    end

    def table_header
      a = HEADER_FOR_PROFILE
      if @gnu_time
        a += HEADER_FOR_GNU_TIME
      end
      a
    end

=begin
    def command(cmd,terminator)
      if @gnu_time
        if /\*|\?|\{|\}|\[|\]|<|>|\(|\)|\~|\&|\||\\|\$|;|`|\n/ =~ cmd
          cmd = cmd.gsub(/'/,"'\"'\"'")
          cmd = "sh -c '#{cmd}'"
        end
        f = %w[%x %e %S %U %M %t %K %D %p %X %Z %F %R %W %c %w %I %O %r
               %s %k].join(@separator)
        "/usr/bin/time -o /dev/stdout -f '#{terminator}:#{f}' #{cmd}"
      else
        "#{cmd}\necho '#{terminator}':$? "
      end
    end #`
=end

    def format_time(t)
      #t.utc.strftime("%F %T.%L")
      t.strftime("%F %T.%L")
    end

    def self.format_time(t)
      t.strftime("%F %T.%L")
    end

    def profile(task_id, task_name, cmd, start_time, end_time, host=nil, status=nil)
      id = @lock.synchronize do
        id = @id
        @id += 1
        id
      end
      if @io
        _puts [ id, task_id, task_name, cmd,
               format_time(start_time),
               format_time(end_time),
               "%.3f" % (end_time-start_time),
               host, status ]
      end
      case status
      when ""
        1
      when Integer
        status
      when String
        if @gnu_time
          if /^([^,]*),/ =~ status
            Integer($1)
          else
            status
          end
        else
          if /^\d+$/ =~ status
            Integer(status)
          else
            status
          end
        end
      end
    end

  end
end
