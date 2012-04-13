module Pwrake

  LOCK = Mutex.new

  class Logger
    include Pwrake::Log

    def initialize(arg=nil)
      @out=nil
      File.open(arg) if arg
    end

    def open(file)
      @out.close if @out && @closeable
      case file
      when IO
        @out=file
        @closeable=false
      else
        @out=File.open(file,"w")
        @closeable=true
      end
      @start_time = Time.now
      @trace = Rake.application.options.trace
      LOCK.synchronize do
        @out.puts "LogStart="+time_str(@start_time) if @trace
      end
    end

    def finish(str, start_time)
      if @out
        finish_time = Time.now
        t1 = time_str(start_time)
        t2 = time_str(finish_time)
        elap = finish_time - start_time
        LOCK.synchronize do
          @out.puts "#{str} : start=#{t1} end=#{t2} elap=#{elap}" if @trace
        end
      end
    end

    def puts(s)
      if @out
        LOCK.synchronize do
          @out.puts(s)
        end
      end
    end

    def <<(s)
      self.puts(s)
    end

    def close
      finish "LogEnd", @start_time
      @out.close if @closeable
      @out=nil
      @closeable=nil
    end
  end # class Logger

end # module Pwrake

